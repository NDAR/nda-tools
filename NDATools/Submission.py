from __future__ import with_statement
from __future__ import absolute_import
import sys
import signal
import multiprocessing
import boto3
import botocore
from boto3.s3.transfer import S3Transfer, TransferConfig
from tqdm import tqdm
from botocore.client import Config, ClientError
import requests
if sys.version_info[0] < 3:
    import Queue as queue
    input = raw_input
else:
    import queue
from NDATools.Configuration import *
from NDATools.Utils import *
from NDATools.MultiPartUploads import *
from NDATools.TokenGenerator import *


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'processing'
    READY = 'Ready'


class Submission:
    def __init__(self, id, full_file_path, config, resume=False, allow_exit=False, username=None, password=None, thread_num=None):
        self.config = config
        self.api = self.config.submission_api
        if username:
            self.config.username = username
        if password:
            self.config.password = password
        self.username = self.config.username
        self.password = self.config.password
        self.aws_access_key = self.config.aws_access_key
        self.aws_secret_key = self.config.aws_secret_key
        self.full_file_path = full_file_path
        self.total_upload_size = 0
        self.upload_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num
        self.batch_size = 50000
        self.batch_status_update = []
        self.directory_list = self.config.directory_list
        self.credentials_list = []
        self.associated_files = []
        self.status = None
        self.total_files = None
        self.total_progress = None
        self.upload_tries = 0
        self.max_submit_time = 120
        self.url = self.config.datamanager_api
        if resume:
            self.submission_id = id
            self.no_match = []
            self.no_read_access = set()
        else:
            self.package_id = id
        self.exit = allow_exit
        self.all_mpus = []


    def submit(self):
        response, session = api_request(self, "POST", "/".join([self.api, self.package_id]))
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
        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id]))

        if response:
            self.status = response['submission_status']
        else:
            message='An error occurred while checking submission {} status.'.format(self.submission_id)
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                            message=message)
            else:
                raise Exception("{}\n{}".format('StatusError', message))

    def create_file_id_list(self, response):
        file_ids = []
        file_ids.extend(file['id'] for file in response if file['status'] != 'Complete')

        return file_ids


    def get_multipart_credentials(self, file_ids):
        all_credentials = []
        batched_ids = [file_ids[i:i + self.batch_size] for i in range(0, len(file_ids), self.batch_size)]
        for ids in batched_ids:
            credentials_list, session = api_request(self, "POST", "/".join(
                [self.api, self.submission_id, 'files/batchMultipartUploadCredentials']), data=json.dumps(ids))
            all_credentials = all_credentials + credentials_list['credentials']

        return all_credentials

    def generate_data_for_request(self, status):
        batch_status_update = []
        batched_file_info_lists = [self.credentials_list[i:i + self.batch_size] for i in
                                   range(0, len(self.credentials_list),
                                         self.batch_size)]

        for files_list in batched_file_info_lists:
            for cred in files_list:
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

        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id, 'files']))
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
        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id, 'files']))
        file_ids = self.create_file_id_list(response)
        self.credentials_list = self.get_multipart_credentials(file_ids)

        data = self.generate_data_for_request(Status.COMPLETE)
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}

        session = requests.session()
        r = session.send(requests.Request('PUT', url, headers, auth=auth, data=json.dumps(data)).prepare(),
                         timeout=300, stream=False)

        response = json.loads(r.text)

        errors = response['errors']

        unsubmitted_ids = []
        for e in errors:
           unsubmitted_ids.append(e['submissionFileId'])

        #update status of files already submitted
        for file in self.batch_status_update:
            if file['id'] in unsubmitted_ids:
                file_ids.remove(file['id'])

        if file_ids:
            self.credentials_list = self.get_multipart_credentials(file_ids)
            self.batch_update_status()

        #update full_file_path list
        self.credentials_list = self.get_multipart_credentials(unsubmitted_ids)
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
            if self.aws_access_key == "":
                self.aws_access_key = input('Enter the access_key for your AWS account: ')
            if self.aws_secret_key == "":
                self.aws_secret_key = input('Enter the secret_key for your AWS account: ')


    def check_read_permissions(self, file):
        try:
            open(file)
            return True
        except IOError:
            return False
        except PermissionError:
            return False

    def found_all_files(self, directories=None, source_bucket=None, source_prefix=None, access_key=None, secret_key=None, retry_allowed=False):
        def raise_error(error, l = []):
            m = '\n'.join([error] + list(set(l)))
            raise Exception(m)

        self.directory_list = directories
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.aws_access_key = access_key
        self.aws_secret_key = secret_key

        if not self.directory_list and not self.source_bucket:
            if retry_allowed:
                self.recollect_file_search_info()
            else:
                error ='Missing directory and/or an S3 bucket.'
                raise_error(error)

        if not self.no_match:
            for file in self.associated_files:
                self.no_match.append(file)

        # local files
        if self.directory_list:
            for file in self.no_match[:]:
                if file.startswith('/'):
                    f = file[1:]
                else:
                    f = file
                for d in self.directory_list:
                    file_name = os.path.join(d, f)
                    if os.path.isfile(file_name):
                        if not self.check_read_permissions(file_name):
                            self.no_read_access.add(file_name)
                        self.full_file_path[file] = (file_name, os.path.getsize(file_name))
                        self.no_match.remove(file)
                        break

        # files in s3
        no_access_buckets = []
        if self.source_bucket:
            if self.aws_access_key is "":
                self.aws_access_key = input('Enter the access_key for your AWS account: ')
            if self.aws_secret_key is "":
                self.aws_secret_key = input('Enter the secret_key for your AWS account: ')
            s3 = boto3.session.Session(aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
            s3_client = s3.client('s3')
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
                    print('\n', message)
                    for b in no_access_buckets:
                        print(b)
                else:
                    error = "".join(['Bucket Access:', message])
                    raise_error(error, no_access_buckets)
            message = 'You must make sure all associated files listed in your validation file' \
                      ' are located in the specified directory or AWS bucket. Associated file not found in specified directory:\n'
            if retry_allowed:
                print('\n', message)
                for file in self.no_match:
                    print(file)
                self.recollect_file_search_info()
                self.found_all_files(self.directory_list, self.source_bucket, self.source_prefix, self.aws_access_key,
                                     self.aws_secret_key, retry_allowed=True)
            else:
                error = "".join(['Missing Files:', message])
                raise_error(error, self.no_match)

        while self.no_read_access:
            message = 'You must make sure you have read-access to all the of the associated files listed in your validation file. ' \
                      'Please update your permissions for the following associated files:\n'
            if retry_allowed:
                print(message)
                for file in self.no_read_access:
                    print(file)
                self.recollect_file_search_info()
                [self.no_read_access.remove(i) for i in [file for file in self.no_read_access if self.check_read_permissions(file)]]
            else:
                error = "".join(['Read Permission Error:', message])
                raise_error(error, self.no_match)

        self.config.directory_list = self.directory_list
        self.config.source_bucket = self.source_bucket
        self.config.source_prefix = self.source_prefix
        self.config.aws_access_key = self.aws_access_key
        self.config.aws_secret_key = self.aws_secret_key

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
            api_request(self, "PUT", url, data=data)



    def complete_partial_uploads(self):

        bucket = (self.credentials_list[0]['destination_uri']).split('/')[2] # 'NDAR_Central_{}'.format((int(self.submission_id) % 4) + 1)
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


    def submission_upload(self, hide_progress=True):
        with self.upload_queue.mutex:
            self.upload_queue.queue.clear()
        with self.progress_queue.mutex:
            self.progress_queue.queue.clear()
        self.total_files = len(self.full_file_path)
        for associated_file in self.full_file_path:
            path, file_size = self.full_file_path[associated_file]
            self.total_upload_size += file_size

        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id, 'files']))
        if response:
            file_ids = self.create_file_id_list(response)
            self.credentials_list = self.get_multipart_credentials(file_ids)
            self.batch_update_status(status=Status.READY) #update the file size before submission, so service can compare.

            if hide_progress is False:
                self.total_progress = tqdm(total=self.total_upload_size,
                                           position=0,
                                           unit_scale=True,
                                           unit="bytes",
                                           desc="Total Upload Progress",
                                           ascii=os.name == 'nt')

            workers = []
            for x in range(self.thread_num):
                worker = Submission.S3Upload(x, self.config, self.upload_queue, self.full_file_path, self.submission_id,
                                             self.progress_queue, self.credentials_list, self.all_mpus)

                workers.append(worker)
                worker.daemon = True
                worker.start()

            for file in response:
                if file['status'] != Status.COMPLETE:
                    self.upload_queue.put(file)
            self.upload_queue.put("STOP")
            self.upload_tries += 1
            while any(map(lambda w: w.is_alive(), workers)):
                if hide_progress == False:
                    for progress in iter(self.progress_queue.get, None):
                        self.total_progress.update(progress)
                time.sleep(0.1)
            if hide_progress == False:
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
            print('\nUploads complete.')
            print('Checking Submission Status.')
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
                print('There was an error transferring some files, trying again')
                if self.found_all_files and self.upload_tries < 5:
                    self.submission_upload()
        if self.status != Status.UPLOADING and hide_progress:
            return

    class S3Upload(threading.Thread):
        def __init__(self, index, config, upload_queue, full_file_path, submission_id, progress_queue, credentials_list,
                     all_mpus):
            threading.Thread.__init__(self)
            self.config = config
            self.upload_queue = upload_queue
            self.upload = None
            self.upload_tries = 0
            self.api = self.config.submission_api
            self.username = self.config.username
            self.password = self.config.password
            self.source_bucket = self.config.source_bucket
            self.source_prefix = self.config.source_prefix
            self.aws_access_key = self.config.aws_access_key
            self.aws_secret_key = self.config.aws_secret_key
            self.full_file_path = full_file_path
            self.credentials_list = credentials_list
            self.submission_id = submission_id
            self.all_mpus = all_mpus
            self.index = index + 1
            self.progress_queue = progress_queue
            self.shutdown_flag = threading.Event()

        def upload_config(self):

            local_path = self.upload['file_user_path']
            remote_path = self.upload['file_remote_path']
            file_id = self.upload['id']
            for cred in self.credentials_list:
                if str(cred['submissionFileId']) == file_id:
                    credentials = {'access_key': cred['access_key'], 'secret_key': cred['secret_key'],
                                   'session_token': cred['session_token']}
                    break
            paths = remote_path.split('/')
            bucket = paths[2]
            key = "/".join(paths[3:])
            full_path, file_size = self.full_file_path[local_path]
            return file_id, credentials, bucket, key, full_path, file_size
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
                    self.upload = self.upload_queue.get()
                if self.upload == "STOP":
                    self.upload_queue.put("STOP")
                    self.shutdown_flag.set()
                    break


                file_id, credentials, bucket, key, full_path, file_size = self.upload_config()
                prefix = 'submission_{}'.format(self.submission_id)


                """
                Methods for  file transfer: 

                If the source file is local, use the credentials supplied by the submission API to upload from local 
                file to remote S3 location.

                If the source file is from S3, use a specific AWS Profile to retrieve the source file, and uses
                credentials supplied by the submission API to upload to remote S3 location.
                
                If the file was uploaded using multi-part, it will first complete the multi part uploads.
                """

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

                    Creates source and destination clients for S3 tranfer. Use permanent credentials for accessing both 
                    buckets and accounts. This will require permission from NDA to write to NDA buckets. 

                    The transfer uses a file streaming method by streaming the body of the file into memory and uploads 
                    the stream in chunks using AWS S3Transfer. This Transfer client will automatically use multi part 
                    uploads when necessary. To maximize efficiency, only files greater than 8e6 bytes are uploaded using 
                    the multi part upload. Smaller files are uploaded in one part.  

                    After each successful transfer, the script will change the status of the file to complete in NDA's 
                    submission webservice. 

                    NOTE: For best results and to be cost effective, it is best to perform this file transfer in an AWS 
                    EC2 instance.
                    """

                    tqdm.monitor_interval = 0


                    source_session = boto3.Session(aws_access_key_id=self.aws_access_key,
                                               aws_secret_access_key=self.aws_secret_key)

                    config = Config(connect_timeout=240, read_timeout=240)
                    self.source_s3 = source_session.resource('s3', config=config)

                    source_key = key.split('/')[1:]
                    source_key = '/'.join(source_key)
                    self.source_key = '/'.join([self.source_prefix, source_key])
                    self.fileobj = self.source_s3.Object(self.source_bucket, self.source_key).get()['Body'] # file stream
                    # self.bytes = self.source_s3.Object(self.source_bucket, self.source_key).get()['ContentLength']

                    if mpu_exist:
                        u = UploadMultiParts(mpu_to_complete, self.full_file_path, bucket, prefix, self.config, credentials)
                        u.get_parts_information()
                        self.progress_queue.put(u.completed_bytes)
                        seq = 1

                        for buffer in self.fileobj.iter_chunks(chunk_size=u.chunk_size):
                            if seq in u.parts_completed:
                                part = u.parts[seq - 1]
                                u.check_md5(part, buffer)
                            else:
                                u.upload_part(buffer, seq)
                                self.progress_queue.put(len(buffer))
                                # upload missing part
                            seq += 1
                        u.complete()
                        self.progress_queue.put(None)

                    else:

                        dest_session = boto3.Session(aws_access_key_id=credentials['access_key'],
                                                     aws_secret_access_key=credentials['secret_key'],
                                                     aws_session_token=credentials['session_token'],
                                                     region_name='us-east-1')

                        #GB = 1024 ** 3
                        config = TransferConfig(multipart_threshold=8 * 1024 * 1024)
                        self.dest = dest_session.client('s3')
                        self.dest_bucket = bucket
                        self.dest_key = key
                        self.temp_key = self.dest_key + '.temp'

                        self.dest.upload_fileobj(
                            self.fileobj,
                            self.dest_bucket,
                            self.dest_key,
                            Callback=self.UpdateProgress(self.progress_queue),
                            Config=config # ,
                            # ExtraArgs={"Metadata": {"ContentLength": self.bytes}}
                        )

                    self.progress_queue.put(None)

                else:
                    """
                    Assumes the file is being uploaded from local file system
                    """
                    if mpu_exist:
                        u = UploadMultiParts(mpu_to_complete, self.full_file_path, bucket, prefix, self.config, credentials)
                        u.get_parts_information()
                        self.progress_queue.put(u.completed_bytes)
                        seq = 1

                        with  open(full_path, 'rb+') as f:
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
                                    u.upload_part(buffer, seq)
                                    self.progress_queue.put(len(buffer))
                                seq += 1
                        u.complete()
                        self.progress_queue.put(None)

                    else:
                        if credentials:
                            session = boto3.session.Session(
                                aws_access_key_id=credentials['access_key'],
                                aws_secret_access_key=credentials['secret_key'],
                                aws_session_token=credentials['session_token'],
                                region_name='us-east-1'
                            )
                            s3 = session.client('s3')
                            config = TransferConfig(
                                multipart_threshold=8 * 1024 * 1024,
                                max_concurrency=2,
                                num_download_attempts=10)

                            s3_transfer = S3Transfer(s3, config)
                            tqdm.monitor_interval = 0
                            s3_transfer.upload_file(full_path, bucket, key, callback=self.UpdateProgress(self.progress_queue))

                            self.progress_queue.put(None)
                        else:
                            print('There was an error uploading {} after {} retry attempts'.format(full_path,
                                                                                                   self.upload_tries))
                            continue
                self.upload_tries = 0
                self.upload = None
                self.upload_queue.task_done()
