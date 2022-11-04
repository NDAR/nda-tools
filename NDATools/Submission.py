from __future__ import absolute_import, with_statement

import multiprocessing
import sys

from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.client import Config

if sys.version_info[0] < 3:
    import Queue as queue
    input = raw_input
else:
    import queue
from tqdm import tqdm
from NDATools.Configuration import *
from NDATools.MultiPartUploads import *

logger = logging.getLogger(__name__)
class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'In Progress'
    READY = 'Ready'


class Submission:
    def __init__(self, id, full_file_path, config, resume=False, allow_exit=False, username=None, password=None,
                 thread_num=None, batch_size=None, original_submission_id=None):
        self.config = config
        self.api = self.config.submission_api
        if username:
            self.config.username = username
        if password:
            self.config.password = password
        self.username = self.config.username
        self.password = self.config.password
        self.full_file_path = full_file_path
        self.total_upload_size = 0
        self.upload_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num
        self.batch_size = 10000
        if batch_size:
            self.batch_size = batch_size
        self.batch_status_update = []
        self.directory_list = self.config.directory_list
        self.credentials_list = []
        self.associated_files = []
        self.status = None
        self.total_files = None
        self.total_progress = None
        self.upload_tries = 0
        self.max_submit_time = 120
        self.resume = resume
        if self.resume:
            self.submission_id = id
            self.no_match = []
            self.no_read_access = set()
        else:
            self.package_id = id
        self.exit = allow_exit
        self.all_mpus = []
        self.original_submission_id = original_submission_id
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

    def replace_submission(self):
        response = put_request("/".join([self.api, self.original_submission_id]) + "?submissionPackageUuid={}".format(self.package_id), auth=self.auth)
        if response:
            self.status = response['submission_status']
            self.submission_id = response['submission_id']
        else:
            message = 'There was an error creating your submission'
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                        message=message)
            else:
                raise Exception("{}\n{}".format('SubmissionError', message))

    def submit(self):
        response = post_request("/".join([self.api, self.package_id]), auth=self.auth)
        if response:
            self.status = response['submission_status']
            self.submission_id = response['submission_id']
        else:
            message = 'There was an error creating your submission'
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                        message=message)
            else:
                raise Exception("{}\n{}".format('SubmissionError', message))

    def check_status(self):
        response = get_request("/".join([self.api, self.submission_id]), auth=self.auth)

        if response:
            self.status = response['submission_status']
        else:
            message='An error occurred while checking submission {} status.'.format(self.submission_id)
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                            message=message)
            else:
                raise Exception("{}\n{}".format('StatusError', message))

    def create_file_info_list(self, response):

        return [{'submissionFileId': file['id'], 'destination_uri': file['file_remote_path']} for file in response if
                file['status'] != Status.COMPLETE]

    def get_multipart_credentials(self, file_ids):
        all_credentials = []
        batched_ids = [file_ids[i:i + self.batch_size] for i in range(0, len(file_ids), self.batch_size)]

        for ids in batched_ids:
            query_params = ''
            if self.config.source_bucket is not None:
                query_params = '?s3SourceBucket={}'.format(self.config.source_bucket)
                query_params += '&s3Prefix={}'.format(self.config.source_prefix) if self.config.source_prefix is not None else ''

            credentials_list = post_request("/".join(
                [self.api, self.submission_id, 'files/batchMultipartUploadCredentials']) + query_params, payload=json.dumps(ids), auth=self.auth)
            all_credentials = all_credentials + credentials_list['credentials']
            time.sleep(2)

        return all_credentials

    def generate_data_for_request(self, status, id_list=[]):
        batch_status_update = []

        if self.credentials_list:
            id_list = self.credentials_list

        for cred in id_list:
            file = cred['destination_uri'].split('/')
            file = '/'.join(file[4:])
            size = self.full_file_path[file][1]
            update = {
                "id": str(cred['submissionFileId']),
                "md5sum": "None",
                "size": size,
                "status": status
            }
            batch_status_update.append(update)

        self.batch_status_update = batch_status_update
        return batch_status_update

    @property
    def incomplete_files(self):

        response = get_request("/".join([self.api, self.submission_id, 'files']), auth=self.auth)
        self.full_file_path = {}
        self.associated_files = []
        if response:
            for file in response:
                if file['status'] != Status.COMPLETE:
                    self.associated_files.append(file['file_user_path'])
            return len(self.associated_files) > 0
        else:
            message='There was an error requesting files for submission {}.'.format(self.submission_id)
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                            message=message)
            else:
                raise Exception("{}\n{}".format('SubmissionError', message))

    def check_submitted_files(self):
        response = get_request("/".join([self.api, self.submission_id, 'files']), auth=self.auth)

        file_info = self.create_file_info_list(response)

        data = self.generate_data_for_request(Status.COMPLETE, file_info)
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}

        batched_data = [data[i:i + self.batch_size] for i in
                        range(0, len(data),
                              self.batch_size)]
        unsubmitted_ids = []

        for batch in batched_data:
            session = requests.session()
            r = session.send(requests.Request('PUT', url, headers, auth=auth, data=json.dumps(batch)).prepare(),
                             timeout=300, stream=False)
            response = json.loads(r.text)
            errors = response['errors']
            for e in errors:
                unsubmitted_ids.append(e['submissionFileId'])

        file_ids = [int(ids['submissionFileId']) for ids in file_info]
        new_ids = set(file_ids) & set(unsubmitted_ids)

        self.credentials_list = self.get_multipart_credentials(list(new_ids))

        self.update_full_file_paths()

    def update_full_file_paths(self):
        full_file_path = {}

        for credentials in self.credentials_list:
            full_path = (credentials['destination_uri'].split(self.submission_id)[1][1:])
            for key,value in self.full_file_path.items():
                if full_path == key:
                    full_file_path[key] = value
                    break
        self.full_file_path = full_file_path

    def recollect_file_search_info(self):
        retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                      '<bucket name> to locate your associated files:')
        response = retry.split(' ')
        self.directory_list = response
        if response[0] == '-s3':
            self.source_bucket = response[1]
            self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
            if self.source_prefix == "":
                self.source_prefix = None


    def found_all_files(self, directories=None, source_bucket=None, source_prefix=None, retry_allowed=False):
        def raise_error(error, l = []):
            m = '\n'.join([error] + list(set(l)))
            raise Exception(m)

        self.directory_list = directories
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix

        if not self.directory_list and not self.source_bucket:
            if retry_allowed:
                self.recollect_file_search_info()
            else:
                error = 'Missing directory and/or an S3 bucket.'
                raise_error(error)

        if not self.no_match:
            for file in self.associated_files:
                self.no_match.append(file)

        # local files
        if self.directory_list:
            parse_local_files(self.directory_list, self.no_match, self.full_file_path, self.no_read_access,
                              self.config.skip_local_file_check)

        # files in s3
        no_access_buckets = []
        if self.source_bucket:
            if not self.config.aws_access_key:
                self.config.read_aws_credentials()

            s3_client = get_s3_client_with_config(self.config.aws_access_key, self.config.aws_secret_key, self.config.aws_session_token)
            for file in self.no_match[:]:
                key = file
                if self.source_prefix:
                    key = '/'.join([self.source_prefix, file])
                file_name = '/'.join(['s3:/', self.source_bucket, key])
                try:
                    response = s3_client.head_object(Bucket=self.source_bucket, Key=key)
                    self.full_file_path[file] = (file_name, int(response['ContentLength']))
                    self.no_match.remove(file)
                except botocore.exceptions.ClientError as e:
                    # If a client error is thrown, then check that it was a 404 error.
                    # If it was a 404 error, then the bucket does not exist.
                    error_code = int(e.response['Error']['Code'])
                    if error_code == 404:
                        pass
                    if error_code == 403:
                        no_access_buckets.append(self.source_bucket)
                        pass

        if self.no_match:
            if no_access_buckets:
                message = 'Your user does NOT have access to the following buckets. Please review the bucket ' \
                          'and/or your AWS credentials and try again.'
                if retry_allowed:
                    logger.info('\n', message)
                    for b in no_access_buckets:
                        logger.info(b)
                else:
                    error = "".join(['Bucket Access:', message])
                    raise_error(error, no_access_buckets)
            message = 'You must make sure all associated files listed in your validation file' \
                      ' are located in the specified directory or AWS bucket. Associated file not found in specified directory:\n'
            if retry_allowed:
                logger.info('\n', message)
                for file in self.no_match:
                    logger.info(file)
                self.recollect_file_search_info()
                self.found_all_files(self.directory_list, self.source_bucket, self.source_prefix, retry_allowed=True)
            else:
                error = "".join(['Missing Files:', message])
                raise_error(error, self.no_match)

        while self.no_read_access:
            message = 'You must make sure you have read-access to all the of the associated files listed in your validation file. ' \
                      'Please update your permissions for the following associated files:\n'
            if retry_allowed:
                logger.info(message)
                for file in self.no_read_access:
                    logger.info(file)
                self.recollect_file_search_info()
                [self.no_read_access.remove(i) for i in
                    [file for file in self.no_read_access if check_read_permissions(file)]]
            else:
                error = "".join(['Read Permission Error:', message])
                raise_error(error, self.no_match)

        self.config.directory_list = self.directory_list
        self.config.source_bucket = self.source_bucket
        self.config.source_prefix = self.source_prefix

        try:
            return len(self.directory_list) > 0
        except TypeError:
            return len(self.source_bucket) > 0

    def batch_update_status(self, status=Status.COMPLETE):
        list_data = self.generate_data_for_request(status)
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        data_to_dump = [list_data[i:i + self.batch_size] for i in range(0, len(list_data), self.batch_size)]
        for d in data_to_dump:
            data = json.dumps(d)
            put_request(url, payload=data, auth=self.auth)

    def complete_partial_uploads(self):

        bucket = (self.credentials_list[0]['destination_uri']).split('/')[2]
        prefix = 'submission_{}'.format(self.submission_id)

        # use self.credentials list to pass in a set of temporary tokens, which should allow listMultiPartUpload
        # for the entire submission and not just the specific file object.
        access_key = self.credentials_list[0]['access_key']
        secret_key = self.credentials_list[0]['secret_key']
        session_token = self.credentials_list[0]['session_token']

        multipart_uploads = MultiPartsUpload(bucket, prefix, self.config, access_key, secret_key, session_token)
        multipart_uploads.get_multipart_uploads()

        for upload in multipart_uploads.incomplete_mpu:
            self.all_mpus.append(upload)

    def submission_upload(self, hide_progress):
        with self.upload_queue.mutex:
            self.upload_queue.queue.clear()
        with self.progress_queue.mutex:
            self.progress_queue.queue.clear()
        self.total_files = len(self.full_file_path)
        for associated_file in self.full_file_path:
            path, file_size = self.full_file_path[associated_file]
            self.total_upload_size += file_size

        response = get_request("/".join([self.api, self.submission_id, 'files']), auth=self.auth)
        if response:
            if not self.resume:
                file_id_info = self.create_file_info_list(response)
                file_ids = [int(ids['submissionFileId']) for ids in file_id_info]
                self.credentials_list = self.get_multipart_credentials(file_ids)
                self.batch_update_status(status=Status.PROCESSING)

            if not hide_progress:
                self.total_progress = tqdm(total=self.total_upload_size,
                                           position=0,
                                           unit_scale=True,
                                           unit="bytes",
                                           desc="Total Upload Progress",
                                           ascii=os.name == 'nt',
                                           dynamic_ncols=True)

            workers = []
            for x in range(self.thread_num):
                worker = Submission.S3Upload(x, self.config, self.upload_queue, self.full_file_path, self.submission_id,
                                             self.progress_queue, self.credentials_list, self.all_mpus)

                workers.append(worker)
                worker.daemon = True
                worker.start()

            for file in response:
                if file['status'] != Status.COMPLETE:
                    self.upload_queue.put([file, False, True])
            self.upload_queue.put(["STOP", False, True])
            self.upload_tries += 1
            while any(map(lambda w: w.is_alive(), workers)):
                if not hide_progress:
                    for progress in iter(self.progress_queue.get, None):
                        if (self.total_progress.n < self.total_progress.total
                                and progress <= (self.total_progress.total - self.total_progress.n)):
                            self.total_progress.update(progress)
                time.sleep(2)
            if not hide_progress:
                if self.total_progress.n < self.total_progress.total:
                    self.total_progress.update(self.total_progress.total - self.total_progress.n)
                self.total_progress.close()
            session = None

        else:
            message='There was an error requesting submission {}.'.format(self.submission_id)
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                            message=message)
            else:
                raise Exception("{}\n{}".format('SubmissionError', message))

        self.batch_update_status()

        if not hide_progress:
            logger.info('\nUploads complete.')
            logger.info('Checking Submission Status.')
        self.check_status()
        if self.status == Status.UPLOADING:
            if not self.incomplete_files:
                t1 = time.time()
                while self.status == Status.UPLOADING and (time.time() - t1) < self.max_submit_time:
                    self.check_status()
                    timeout_message = 'Timed out while waiting for submission status to change. You may try again by resuming the submission.'
                    if not hide_progress:
                        sys.stdout.write('.')
                if self.status == Status.UPLOADING:
                    if self.exit:
                        exit_client(signal=signal.SIGTERM,
                                    message=timeout_message)
                    else:
                        raise Exception("{}\n{}".format('TimeOutError', timeout_message))

            else:
                logger.info('There was an error transferring some files, trying again')
                if self.found_all_files and self.upload_tries < 5:
                    self.submission_upload(hide_progress)
        if self.status != Status.UPLOADING and hide_progress:
            return

    class S3Upload(threading.Thread):
        def __init__(self, index, config, upload_queue, full_file_path, submission_id, progress_queue, credentials_list,
                     all_mpus):
            threading.Thread.__init__(self)
            logger.debug('Starting S3Upload thread-%d', index)
            self.config = config
            self.upload_queue = upload_queue
            self.upload = None
            self.upload_tries = 0
            self.api = self.config.submission_api
            self.username = self.config.username
            self.password = self.config.password
            self.source_bucket = self.config.source_bucket
            self.source_prefix = self.config.source_prefix
            self.full_file_path = full_file_path
            self.credentials_list = credentials_list
            self.submission_id = submission_id
            self.all_mpus = all_mpus
            self.index = index + 1
            self.progress_queue = progress_queue
            self.shutdown_flag = threading.Event()
            self.expired = False

        def upload_config(self):
            local_path = sanitize_file_path(self.upload['file_user_path'])
            remote_path = self.upload['file_remote_path']
            file_id = self.upload['id']
            for cred in self.credentials_list:
                if str(cred['submissionFileId']) == file_id:
                    credentials = {'access_key': cred['access_key'], 'secret_key': cred['secret_key'],
                                   'session_token': cred['session_token'], 'source_uri': cred['source_uri'] }
                    break
            paths = remote_path.split('/')
            bucket = paths[2]
            key = "/".join(paths[3:])
            full_path, file_size = self.full_file_path[local_path]
            return file_id, credentials, bucket, key, full_path, file_size

        def update_tokens(self):
            link = self.upload['_links']['multipartUploadCredentials']['href']
            credentials = get_request(link)

            for creds in self.credentials_list:
                if creds['fileId'] == credentials['fileId']:
                    self.credentials_list.remove(creds)
                    break
            self.credentials_list.append(credentials)

            return credentials

        def add_back_to_queue(self, bucket, prefix, try_nda_creds=True):
            # If expired token:
            #  1. Add to upload_queue so it is picked up by another thread again
            self.upload_queue.put([self.upload, True, try_nda_creds])  # need to figure out how to test this and if it is the best way to retry a single file upload
            self.upload_queue.put(["STOP", False, try_nda_creds])  # need to add the sentinel value again

            # 2. Add a function to get new credentials and update self.credentials_list AND
            # 3. In upload.config, edit it so it knows we need new credentials
            new_credentials = self.update_tokens()

            # 4. Check if file has mpu, add to self.mpu

            multipart_uploads = MultiPartsUpload(bucket, prefix, self.config, new_credentials['access_key'],
                                                 new_credentials['secret_key'], new_credentials['session_token'])
            multipart_uploads.get_multipart_uploads()

            self.all_mpus = multipart_uploads.incomplete_mpu

        class UpdateProgress(object):

            def __init__(self, progress_queue):
                self.progress_queue = progress_queue

            def __call__(self, bytes_amount):
                self.progress_queue.put(bytes_amount)

        def run(self):
            while True and not self.shutdown_flag.is_set():
                if self.upload and self.upload_tries < 5:
                    self.upload_tries += 1
                else:
                    upload = self.upload_queue.get()

                self.upload = upload[0]
                self.expired = upload[1]
                self.try_nda_credentials = upload[2]
                if self.upload == "STOP":
                    if self.upload_queue.qsize() == 0:
                        self.upload_queue.put(["STOP", False, True])
                        self.shutdown_flag.set()
                        break
                    else:
                        self.progress_queue.put(None)
                        self.upload_tries = 0
                        self.upload = None
                        self.upload_queue.task_done()
                        upload = self.upload_queue.get()
                        self.upload = upload[0]
                        self.expired = upload[1]
                        self.try_nda_credentials = upload[2]

                file_id, credentials, bucket, key, full_path, file_size = self.upload_config()
                prefix = 'submission_{}'.format(self.submission_id)


                """
                Methods for  file transfer: 

                If the source file is local, use the credentials supplied by the submission API to upload from local 
                file to remote S3 location.

                If the source file is from S3:
                a) check settings.cfg for permanent user credentials (aws_access_key, aws_secret_key)
                b) if permanent credentials are provided, use them to retrieve the source file,
                c) if not provided use a FederationUser token from DataManager API to retreive the source file,
                d) use credentials supplied by the submission API to upload to remote S3 location.
                
                If the file was uploaded using multi-part, it will first complete the multi part uploads.
                """

                expired_error = False
                mpu_exist = False
                for upload in self.all_mpus:
                    if upload['Key'] == key:
                        mpu_exist = True
                        mpu_to_complete = upload
                        break

                if full_path.startswith('s3'):

                    """
                    Assumes you are uploading from external s3 bucket. SOURCE_BUCKET and SOURCE_PREFIX are hard-coded 
                    values, which specify where the object should be copied from (i.e., 100206 subject directory can be
                    located in s3://hcp-openaccess-temp, with a prefix of HCP_1200).

                    Creates source and destination clients for S3 tranfer. If supplied in settings.cfg uses permanent 
                    credentials for accessing source buckets. If permanent credentials are not supplied in
                    settings.cfg, uses a tempoary FederationUser Token from DataManager API to access source bucket. 

                    The transfer uses a file streaming method by streaming the body of the file into memory and uploads 
                    the stream in chunks using AWS S3Transfer. This Transfer client will automatically use multi part 
                    uploads when necessary. To maximize efficiency, only files greater than 1e8 bytes are uploaded using 
                    the multi part upload. Smaller files are uploaded in one part.  

                    After each successful transfer, the script will change the status of the file to complete in NDA's 
                    submission webservice. 

                    NOTE: For best results and to be cost effective, it is best to perform this file transfer in an AWS 
                    EC2 instance.
                    """

                    tqdm.monitor_interval = 0

                    s3_config = Config(connect_timeout=240, read_timeout=240)
                    self.source_s3 = get_s3_resource(self.config.aws_access_key,
                                                     self.config.aws_secret_key,
                                                     self.config.aws_session_token,
                                                     s3_config)

                    source_key = key.split('/')[1:]
                    source_key = '/'.join(source_key)
                    self.source_key = '/'.join([self.source_prefix, source_key])

                    # try the original file credentials to perform a cp from source to dest
                    # Note - this is faster than downloading and uploading but may not work if NDA doenst have access
                    # to source bucket
                    if self.try_nda_credentials :
                        s3_client = self.credentials.get_s3_client_with_config(credentials['access_key'],
                                                                                    credentials['secret_key'],
                                                                                    credentials['session_token'])
                        try:
                            s3_client.copy({'Bucket': self.source_bucket, 'Key': self.source_key},
                                                            bucket, prefix + '/' + source_key)
                        except botocore.exceptions.ClientError as error:
                            self.add_back_to_queue(bucket, prefix, False)
                    elif mpu_exist:
                        self.fileobj = self.source_s3.Object(self.source_bucket, self.source_key).get()['Body']

                        u = UploadMultiParts(mpu_to_complete, self.full_file_path, bucket, prefix, self.config, credentials, file_size)
                        u.get_parts_information()
                        if not self.expired:
                            self.progress_queue.put(u.completed_bytes)
                        seq = 1

                        for buffer in self.fileobj.iter_chunks(chunk_size=u.chunk_size):
                            if seq in u.parts_completed:
                                part = u.parts[seq - 1]
                                u.check_md5(part, buffer)
                            else:
                                try:
                                    u.upload_part(buffer, seq)
                                    self.progress_queue.put(len(buffer))
                                    # upload missing part
                                except Exception as error:
                                    e = str(error)
                                    if "ExpiredToken" in e:
                                        self.add_back_to_queue(bucket, prefix)
                                        expired_error = True
                                    else:
                                        raise error
                            seq += 1
                        if not expired_error:
                            u.complete()
                        self.progress_queue.put(None)

                    else:
                        self.fileobj = self.source_s3.Object(self.source_bucket, self.source_key).get()['Body']

                        # set chunk size dynamically to based on file size
                        if file_size > 9999:
                            multipart_chunk_size = (file_size // 9999)
                        else:
                            multipart_chunk_size = file_size
                        transfer_config = TransferConfig(multipart_threshold=100 * 1024 * 1024,
                                                         multipart_chunksize=multipart_chunk_size)

                        self.dest = get_s3_client_with_config(credentials['access_key'],
                                                                               credentials['secret_key'],
                                                                               credentials['session_token'])
                        self.dest_bucket = bucket
                        self.dest_key = key
                        self.temp_key = self.dest_key + '.temp'

                        try:
                            self.dest.upload_fileobj(
                                self.fileobj,
                                self.dest_bucket,
                                self.dest_key,
                                Callback=self.UpdateProgress(self.progress_queue),
                                Config=transfer_config
                            )
                        except boto3.exceptions.S3UploadFailedError as error:
                            e = str(error)
                            if "ExpiredToken" in e:
                                self.add_back_to_queue(bucket, prefix)
                            else:
                                raise error
                    self.progress_queue.put(None)

                else:
                    """
                    Assumes the file is being uploaded from local file system
                    """

                    if mpu_exist:
                        u = UploadMultiParts(mpu_to_complete, self.full_file_path, bucket, prefix, self.config,
                                             credentials, file_size)
                        u.get_parts_information()
                        if not self.expired:
                            self.progress_queue.put(u.completed_bytes)
                        seq = 1

                        with open(full_path, 'rb') as f:
                            while True:
                                buffer_start = u.chunk_size * (seq - 1)
                                f.seek(buffer_start)
                                buffer = f.read(u.chunk_size)
                                if len(buffer) == 0:  # EOF
                                    break
                                if seq in u.parts_completed:
                                    part = u.parts[seq - 1]
                                    u.check_md5(part, buffer)
                                else:
                                    try:
                                        u.upload_part(buffer, seq)
                                        self.progress_queue.put(len(buffer))
                                    except Exception as error:
                                        e = str(error)
                                        if "ExpiredToken" in e:
                                            self.add_back_to_queue(bucket, prefix)
                                            expired_error = True
                                            break
                                        else:
                                            raise error

                                seq += 1
                        if not expired_error:
                            u.complete()
                        self.progress_queue.put(None)
                    else:
                        if credentials:
                            s3 = get_s3_client_with_config(credentials['access_key'],
                                                                            credentials['secret_key'],
                                                                            credentials['session_token'])
                            config = TransferConfig(
                                multipart_threshold=100 * 1024 * 1024,
                                max_concurrency=2,
                                num_download_attempts=10)

                            s3_transfer = S3Transfer(s3, config)
                            tqdm.monitor_interval = 0
                            try:
                                s3_transfer.upload_file(full_path, bucket, key,
                                                        callback=self.UpdateProgress(self.progress_queue))
                            except boto3.exceptions.S3UploadFailedError as error:
                                e = str(error)
                                if "ExpiredToken" in e:
                                    self.add_back_to_queue(bucket, prefix)
                                else:
                                    raise error

                            self.progress_queue.put(None)

                        else:
                            logger.info('There was an error uploading {} after {} retry attempts'.format(full_path,
                                                                                                   self.upload_tries))
                            continue

                self.upload_tries = 0
                self.upload = None
                self.upload_queue.task_done()
