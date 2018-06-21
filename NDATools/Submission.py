from __future__ import with_statement
from __future__ import absolute_import
import signal
import sys
import os
import time
import threading
import multiprocessing
import boto3
from boto3.s3.transfer import S3Transfer
from tqdm import tqdm

if sys.version_info[0] < 3:
    import Queue as queue

    input = raw_input
else:
    import queue

from NDATools.Configuration import *



class Submission:
    def __init__(self, id, full_file_path, config=None, resume=False, username=None, password=None):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration()
            self.config.username = username
            self.config.password = password

        self.api = self.config.submission_api
        self.username = self.config.username
        self.password = self.config.password
        self.full_file_path = full_file_path

        if resume:
            self.submission_id = id
        else:
            self.package_id = id
        self.total_upload_size = 0
        self.upload_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.cpu_num = max([1, multiprocessing.cpu_count() - 1])
        self.directory_list = self.config.directory_list
        self.associated_files = []
        self.status = None
        self.total_files = None
        self.total_progress = None
        self.upload_tries = 0

    def submit(self):
        response, session = api_request(self, "POST", "/".join([self.api, self.package_id]))
        if response:
            self.status = response['submission_status']
            self.submission_id = response['submission_id']
        else:
            self.config.exit_client(signal=signal.SIGINT,
                        message='There was an error creating your submission.')

    def check_status(self):
        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id]))
        if response:
            self.status = response['submission_status']
        else:
            self.config.exit_client(signal=signal.SIGINT,
                        message='An error occurred while checking submission {} status.'.format(self.submission_id))

    @property
    def incomplete_files(self):
        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id, 'files']))
        self.full_file_path = {}
        self.associated_files = []
        if response:
            for file in response:
                if file['status'] != 'Complete':
                    self.associated_files.append(file['file_user_path'])
            return len(self.associated_files) > 0
        else:
            exit_client(signal=signal.SIGINT,
                        message='There was an error requesting files for submission {}.'.format(self.submission_id))

    @property
    def found_all_local_files(self):
        if not self.directory_list:
            directory_input = input(
                '\nYour data has associated files. '
                'Please enter a list of directories where the associated files are stored, separated by a space:')
            self.directory_list = directory_input.split(' ')
        no_match = []
        for file in self.associated_files:
            no_match.append(file)
            if file.startswith('/'):
                f = file[1:]
            else:
                f = file
            for d in self.directory_list:
                file_name = os.path.join(d, f)
                if os.path.isfile(file_name):
                    self.full_file_path[file] = (file_name, os.path.getsize(file_name))
                    no_match.remove(file)
                    break
        for file in no_match:
            print('Associated file not found in specified directory:', file)
        if no_match:
            exit_client(signal.SIGINT, message=
            '\nYou must make sure all associated files listed in your validation file'
            ' are located in the specified directory. Please try again.')
        return len(self.directory_list) > 0

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
            if hide_progress == False:
                self.total_progress = tqdm(total=self.total_upload_size,
                                       position=0,
                                       unit_scale=True,
                                       unit="bytes",
                                       desc="Total Upload Progress",
                                       ascii=os.name == 'nt')
            workers = []
            for x in range(self.cpu_num):
                worker = Submission.S3Upload(x, self.config, self.upload_queue, self.full_file_path, self.submission_id, self.progress_queue)
                workers.append(worker)
                worker.daemon = True
                worker.start()
            # check status for files that are not yet complete -- associated files
            for file in response:
                if file['status'] != 'Complete':
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
            exit_client(signal=signal.SIGINT,
                        message='There was an error requesting submission {}.'.format(self.submission_id))
        if not hide_progress:
            print('\nUploads complete.')
            print('Checking Submission Status.')
        self.check_status()
        if self.status == 'Uploading':
            if not self.incomplete_files:
                t1 = time.time()
                while self.status == 'Uploading' and (time.time() - t1) < 120:
                    self.check_status()
                    if hide_progress:
                        timeout_message = 'Timed out while waiting for submission status to change.\n'
                    else:
                        sys.stdout.write('.')
                        timeout_message = 'Timed out while waiting for submission status to change.\nYou may try again by resuming the submission: python nda-validationtool-client.py -r {}\n'.format(self.submission_id)
                if self.status == 'Uploading':
                    exit_client(signal=signal.SIGINT,
                                message= timeout_message)
            else:
                print('There was an error transferring some files, trying again')
                if self.found_all_local_files and self.upload_tries < 5:
                    self.submission_upload()
        if self.status != 'Uploading' and hide_progress:
            sys.exit(0)

    class S3Upload(threading.Thread):

        def __init__(self, index, config, upload_queue, full_file_path, submission_id, progress_queue):
            threading.Thread.__init__(self)
            self.config = config
            self.upload_queue = upload_queue
            self.upload = None
            self.upload_tries = 0
            self.api = self.config.submission_api
            self.username = self.config.username
            self.password = self.config.password
            self.full_file_path = full_file_path
            self.submission_id = submission_id
            self.index = index + 1
            self.progress_queue = progress_queue
            self.shutdown_flag = threading.Event()

        def upload_config(self):
            link = self.upload['_links']['multipartUploadCredentials']['href']
            local_path = self.upload['file_user_path']
            remote_path = self.upload['file_remote_path']
            file_id = self.upload['id']
            credentials, session = api_request(self, "GET", link)
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
                if credentials:
                    session = boto3.session.Session(
                        aws_access_key_id=credentials['access_key'],
                        aws_secret_access_key=credentials['secret_key'],
                        aws_session_token=credentials['session_token'],
                        region_name='us-east-1'
                    )
                    s3 = session.client('s3')
                    s3_transfer = S3Transfer(s3)
                    tqdm.monitor_interval = 0
                    s3_transfer.upload_file(full_path, bucket, key,
                                            callback=self.UpdateProgress(self.progress_queue)
                                            )
                    api_request(self, "PUT", "/".
                                join([self.api, self.submission_id, "files", file_id])
                                + "?submissionFileStatus=Complete")
                    self.progress_queue.put(None)
                else:
                    print('There was an error uploading {} after {} retry attempts'.format(full_path,
                                                                                           self.upload_tries))
                    continue
                self.upload_tries = 0
                self.upload = None
                self.upload_queue.task_done()