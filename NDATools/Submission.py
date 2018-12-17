from __future__ import with_statement
from __future__ import absolute_import
import sys
import signal
import multiprocessing
import boto3
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
        if resume:
            self.submission_id = id
            self.no_match = []
        else:
            self.package_id = id
        self.exit = allow_exit


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

    def generate_data_for_request(self):
        batch_status_update = []
        batched_file_info_lists = [self.credentials_list[i:i + self.batch_size] for i in
                                   range(0, len(self.credentials_list),
                                         self.batch_size)]

        for files_list in batched_file_info_lists:
            for cred in files_list:
                file = cred['configuration']['destinationURI'].split('/')
                file = '/'.join(file[1:])
                size = self.full_file_path[file][1]
                update = {
                    "id": str(cred['submissionFileId']),
                    "md5sum": "None",
                    "size": size,
                    "status": "Complete"
                }
                batch_status_update.append(update)

        self.batch_status_update = batch_status_update
        data = json.dumps(self.batch_status_update)

        return data

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

        data = self.generate_data_for_request()
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}

        session = requests.session()
        r = session.send(requests.Request('PUT', url, headers, auth=auth, data=data).prepare(),
                         timeout=300, stream=False)

        response = r.text
        list = response.split(': ')

        unsubmitted_ids = list[1].split(',')


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
            full_path = (credentials['destinationURI'].split(self.submission_id)[1][1:])
            for key,value in self.full_file_path.items():
                if full_path == key:
                    full_file_path[key] = value
                    break
        self.full_file_path = full_file_path

    def found_all_files(self, retry_allowed=False):
        print('\nSearching for associated files...')

        s3 = False
        self.directory_list = self.config.directory_list
        self.source_bucket = self.config.source_bucket
        if self.source_bucket:
            s3 = True
        self.source_prefix = self.config.source_prefix
        self.aws_access_key = self.config.aws_access_key
        self.aws_secret_key = self.config.aws_secret_key

        if not self.directory_list and not self.source_bucket:
            retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                          '<bucket name> to locate your associated files:')  # prefix and profile can be blank (None)
            response = retry.split(' ')
            self.directory_list = response
            if response[0] == '-s3':
                s3 = True
                self.source_bucket = response[1]
                self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
                if self.source_prefix == "":
                    self.source_prefix = None
                if self.aws_access_key == "":
                    self.aws_access_key = input('Enter the access_key for your AWS account: ')
                if self.aws_secret_key == "":
                    self.aws_secret_key = input('Enter the secret_key for your AWS account: ')

                self.config.source_bucket = self.source_bucket
                self.config.source_prefix = self.source_prefix
                self.config.aws_access_key = self.aws_access_key
                self.config.aws_secret_key = self.aws_secret_key
                self.directory_list = None

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
                        self.full_file_path[file] = (file_name, os.path.getsize(file_name))
                        self.no_match.remove(file)
                        break

        # files in s3
        if s3:
            if self.aws_access_key is None:
                self.aws_access_key = input('Enter the access_key for your AWS account: ')
            if self.aws_secret_key is None:
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
                except ClientError as e:
                    pass

        for file in self.no_match:
            print('Associated file not found in specified directory:', file)
        if self.no_match:
            print('\nYou must make sure all associated files listed in your validation file'
                  ' are located in the specified directory or AWS bucket. Please try again.')
            if retry_allowed:
                retry = input(
                    'Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                    '<bucket name> to locate your associated files:')  # prefix and profile can be blank (None)
                response = retry.split(' ')
                self.directory_list = response
                if response[0] == '-s3':
                    self.source_bucket = response[1]
                    self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
                    if self.source_prefix == "":
                        self.source_prefix = None
                    self.aws_access_key = input('Enter the access_key for your AWS account: ')
                    self.aws_secret_key = input('Enter the secret_key for your AWS account: ')
                    self.directory_list = None

                self.config.source_bucket = self.source_bucket
                self.config.source_prefix = self.source_prefix
                self.config.aws_access_key = self.aws_access_key
                self.config.aws_secret_key = self.aws_secret_key

                self.found_all_files(retry_allowed=True)
            else:
                print("\nYou did not enter all the correct directories or AWS bucket. Try again.")
                sys.exit(1)
        try:
            return len(self.directory_list) > 0
        except TypeError:
            return len(self.source_bucket) > 0


    def batch_update_status(self):
        data = self.generate_data_for_request()
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])

        api_request(self, "PUT", url, data=data)

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
                                             self.progress_queue, self.credentials_list)

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
            sys.exit(0)

    class S3Upload(threading.Thread):
        def __init__(self, index, config, upload_queue, full_file_path, submission_id, progress_queue, credentials_list):
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
            self.index = index + 1
            self.progress_queue = progress_queue
            self.shutdown_flag = threading.Event()

        def upload_config(self):

            local_path = self.upload['file_user_path']
            remote_path = self.upload['file_remote_path']
            file_id = self.upload['id']
            for cred in self.credentials_list:
                if str(cred['submissionFileId']) == file_id:
                    credentials = {'accessKey': cred['accessKey'], 'secretKey': cred['secretKey'],
                                   'sessionToken': cred['sessionToken']}
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



                """
                Methods for  file transfer: 

                If the source file is local, use the credentials supplied by the submission API to upload from local 
                file to remote S3 location.

                If the source file is from S3, use a specific AWS Profile to retrieve the source file, and uses
                credentials supplied by the submission API to upload to remote S3 location.
                """

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
                    self.fileobj = self.source_s3.Object(self.source_bucket, self.source_key).get()['Body']
                    self.bytes = self.source_s3.Object(self.source_bucket, self.source_key).get()['ContentLength']
                    dest_session = boto3.Session(aws_access_key_id=credentials['accessKey'],
                                                 aws_secret_access_key=credentials['secretKey'],
                                                 aws_session_token=credentials['sessionToken'],
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

                    if credentials:
                        session = boto3.session.Session(
                            aws_access_key_id=credentials['accessKey'],
                            aws_secret_access_key=credentials['secretKey'],
                            aws_session_token=credentials['sessionToken'],
                            region_name='us-east-1'
                        )
                        s3 = session.client('s3')
                        s3_transfer = S3Transfer(s3)
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

class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'processing'