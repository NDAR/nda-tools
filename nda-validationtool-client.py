from __future__ import with_statement
from __future__ import absolute_import
import signal
import sys
import argparse
import os
import getpass
import time
import csv
import threading
import multiprocessing
import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util
import json
import boto3
from boto3.s3.transfer import S3Transfer
from tqdm import tqdm

if sys.version_info[0] < 3:
    import ConfigParser as configparser
    import Queue as queue

    input = raw_input
    import thread
else:
    import configparser
    import queue
    import _thread as thread


class ClientConfiguration:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("settings.cfg")
        self.validation_api = self.config.get("Endpoints", "validation")
        self.submission_package_api = self.config.get("Endpoints", "submission_package")
        self.submission_api = self.config.get("Endpoints", "submission")
        self.validationtool_api = self.config.get("Endpoints", "validationtool")
        self.validation_results = self.config.get("Files", "validation_results")
        self.submission_packages = self.config.get("Files", "submission_packages")
        self.username = self.config.get("User", "username")
        self.password = self.config.get("User", "password")
        self.collection_id = None
        self.endpoint_title = None
        self.directory_list = None
        self.title = None
        self.description = None
        self.JSON = False

    def nda_login(self):
        if not config.username:
            self.username = input('Enter your NIMH Data Archives username:')
        if not config.password:
            self.password = getpass.getpass('Enter your NIMH Data Archives password:')


class Validation:
    def __init__(self, file_list, config=None):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration()
        self.api = self.config.validation_api.strip('/')
        self.file_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.file_list = file_list
        self.full_file_path = None
        self.directory_list = self.config.directory_list
        self.associated_files = []
        self.uuid = []
        self.uuid_dict = {}
        self.responses = []
        self.date = time.strftime("%Y%m%dT%H%M%S")
        self.e = False
        self.w = False
        if not self.config.validation_results:
            self.config.validation_results = 'NDAValidationResults'
        self.validation_result_dir = os.path.join(os.path.expanduser('~'), self.config.validation_results)
        if not os.path.exists(self.validation_result_dir):
            os.mkdir(self.validation_result_dir)
        if self.config.JSON:
            self.log_file = os.path.join(self.validation_result_dir, 'validation_results_{}.json'.format(self.date))
        else:
            self.log_file = os.path.join(self.validation_result_dir, 'validation_results_{}.csv'.format(self.date))

        self.field_names = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'MESSAGE', 'RECORD']
        self.validation_progress = None

    """
    Validates a list of csv files and saves a new csv file with the results in the Home directory.
    If data includes associated files and user has not entered any directories where they are saved, user is prompted to 
    enter a list of directories only if they have also indicated to build a package for submission.
    """

    def validate(self):
        print('\nValidating files...')
        # find out how many cpu in your computer to get max threads
        self.validation_progress = tqdm(total=len(self.file_list), position=0, ascii=os.name == 'nt')
        cpu_num = multiprocessing.cpu_count()
        if cpu_num > 1:
            cpu_num -= 1
        workers = []
        for x in range(cpu_num):
            worker = Validation.ValidationTask()
            workers.append(worker)
            worker.daemon = True
            worker.start()
        for file in self.file_list:
            self.file_queue.put(file)
        self.file_queue.put("STOP")
        while any(map(lambda x: x.is_alive(), workers)):
            time.sleep(0.1)
        self.validation_progress.close()
        while True:
            try:
                (response, file) = self.result_queue.get()
                self.result_queue.task_done()
                self.responses.append((response, file))
                if self.result_queue.empty():
                    break
            except queue.Empty:
                break
        for (response, file) in self.responses:
            if response['status'] == "SystemError":
                print('\nSystemError while validating: {}'.format(file))
                print('Please contact NDAHelp@mail.nih.gov')
                self.e = True
                response['errors'].update({'SystemError': [
                    {'message': 'SystemError while validating {}'.format(file)}
                ]})
            elif response['errors'] != {}:
                print('\nError! Check file: {}'.format(file))
                self.e = True
            if response['associated_file_paths'] and response['errors'] == {}:
                self.associated_files.append(response['associated_file_paths'])
            if response['warnings'] != {}:
                self.w = True
            self.uuid.append(response['id'])
            self.uuid_dict[response['id']] = {'file': file, 'errors': response['errors'] != {}}

    def output(self):
        if self.config.JSON:
            json_data = dict(Results=[])
            for (response, file) in self.responses:
                file_name = self.uuid_dict[response['id']]['file']
                json_data['Results'].append({
                    'File': file_name,
                    'ID': response['id'],
                    'Status': response['status'],
                    'Expiration Date': response['expiration_date'],
                    'Errors': response['errors']
                })
            with open(self.log_file, 'w') as outfile:
                json.dump(json_data, outfile)
        else:
            if sys.version_info[0] < 3:
                csvfile = open(self.log_file, 'wb')
            else:
                csvfile = open(self.log_file, 'w', newline='')
            writer = csv.DictWriter(csvfile, fieldnames=self.field_names)
            writer.writeheader()
            for (response, file) in self.responses:
                file_name = self.uuid_dict[response['id']]['file']
                if response['errors'] == {}:
                    writer.writerow(
                        {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                         'EXPIRATION_DATE': response['expiration_date'], 'ERRORS': 'None', 'MESSAGE': 'None',
                         'RECORD': 'None'})
                else:
                    for error, value in response['errors'].items():
                        for v in value:
                            message = v['message']
                            record = v['recordNumber']
                            writer.writerow(
                                {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'], 'ERRORS': error, 'MESSAGE': message,
                                 'RECORD': record})
            csvfile.close()
        print('Validation report output to: {}'.format(self.log_file))

    def warnings(self):
        if self.config.JSON:
            json_data = dict(Results=[])
            for (response, file) in self.responses:
                file_name = self.uuid_dict[response['id']]['file']
                json_data['Results'].append({
                    'File': file_name,
                    'ID': response['id'],
                    'Status': response['status'],
                    'Expiration Date': response['expiration_date'],
                    'Warnings': response['warnings']
                })
                new_path = ''.join([self.validation_result_dir, '/validation_warnings_', self.date, '.json'])
                with open(new_path, 'w') as outfile:
                    json.dump(json_data, outfile)
        else:
            new_path = ''.join([self.validation_result_dir, '/validation_warnings_', self.date, '.csv'])
            if sys.version_info[0] < 3:
                csvfile = open(new_path, 'wb')
            else:
                csvfile = open(new_path, 'w', newline='')
            fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'WARNINGS', 'MESSAGE']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for (response, file) in self.responses:
                file_name = self.uuid_dict[response['id']]['file']
                if response['warnings'] == {}:
                    writer.writerow(
                        {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                         'EXPIRATION_DATE': response['expiration_date'], 'WARNINGS': 'None', 'MESSAGE': 'None'})
                else:
                    for warning, value in response['warnings'].items():
                        for v in value:
                            message = v['message']
                            writer.writerow(
                                {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'], 'WARNINGS': warning, 'MESSAGE': message
                                 })
            csvfile.close()
        print('Warnings output to: {}'.format(self.log_file))

    def file_search(self):
        self.full_file_path = {}
        if not self.associated_files:
            return

        if not self.directory_list:
            directory_input = input(
                '\nYour data has associated files. '
                'Please enter a list of directories where the associated files are stored, separated by a space:')
            self.directory_list = directory_input.split(' ')

        no_match = []
        for a in self.associated_files:
            for file in a:
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
            print(
                '\nYou must make sure all associated files listed in your validation file'
                ' are located in the specified directory. Please try again.')
            retry = input('Press the "Enter" key to specify location(s) for files and try again:')
            self.directory_list = retry.split(' ')
            self.file_search()

    class ValidationTask(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result_queue = validation.result_queue
            self.file_queue = validation.file_queue
            self.api = validation.api
            self.progress_bar = validation.validation_progress
            self.responses = validation.responses
            self.shutdown_flag = threading.Event()

        def run(self):
            while True and not self.shutdown_flag.is_set():
                polling = 0
                file_name = self.file_queue.get()
                if file_name == "STOP":
                    self.file_queue.put("STOP")
                    self.shutdown_flag.set()
                    break
                try:
                    file = open(file_name, 'rb')
                except FileNotFoundError:
                    print('This file does not exist in current directory:', file_name)
                    sys.exit()
                data = file.read()
                response, session = api_request(self, "POST", self.api, data)
                while response and not response['done']:
                    response, session = api_request(self, "GET", "/".join([self.api, response['id']]), session=session)
                    time.sleep(0.1)
                    polling += 1
                    if polling == 50:
                        polling = 0
                if response:
                    response, session = api_request(self, "GET", "/".join([self.api, response['id']]), session=session)
                    self.result_queue.put((response, file_name))
                    self.progress_bar.update(n=1)
                # Stop thread after adding validation response
                self.file_queue.task_done()


class SubmissionPackage:
    def __init__(self, uuid, full_file_path, config=None):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration()
        self.api = self.config.submission_package_api
        self.validationtool_api = self.config.validationtool_api
        self.uuid = uuid
        self.full_file_path = full_file_path
        self.config.nda_login()
        self.username = self.config.username
        self.password = self.config.password
        if self.config.title:
            self.dataset_name = self.config.title
        else:
            self.dataset_name = input('Enter title for dataset name:')
        if self.config.description:
            self.dataset_description = self.config.description
        else:
            self.dataset_description = input('Enter description for the dataset submission:')
        self.package_info = dict
        self.download_links = []
        self.package_id = None
        self.package_folder = None
        self.collection_id = None
        self.endpoint_title = None
        self.collections = {}
        self.endpoints = []
        self.get_collections()
        self.get_custom_endpoints()
        if not self.config.submission_packages:
            self.config.submission_packages = 'NDASubmissionPackages'
        self.submission_packages_dir = os.path.join(os.path.expanduser('~'), config.submission_packages)
        if not os.path.exists(self.submission_packages_dir):
            os.mkdir(self.submission_packages_dir)

    def get_collections(self):
        collections, session = api_request(self,
                                           "GET",
                                           "/".join([self.validationtool_api, "user/collection"]))
        if collections:
            for c in collections:
                self.collections.update({c['id']: c['title']})

    def get_custom_endpoints(self):
        endpoints, session = api_request(self,
                                         "GET",
                                         "/".join([self.validationtool_api, "user/customEndpoints"]))
        if endpoints:
            for endpoint in endpoints:
                self.endpoints.append(endpoint['title'])

    def set_upload_destination(self):
        if len(self.collections) > 0 or len(self.endpoints) > 0:
            user_input = None
            if self.config.collection_id and self.config.endpoint_title:
                print('You selected both a collection and an alternate endpoint.\n'
                      'These options are mutually exclusive, please specify only one.')
            elif self.config.endpoint_title:
                self.endpoint_title = self.config.endpoint_title
            elif self.config.collection_id:
                self.collection_id = self.config.collection_id
            else:
                user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
            while True:
                if user_input:
                    try:
                        if user_input.startswith('-c'):
                            collection_id = user_input.split(" ")
                            self.collection_id = collection_id[1]
                            self.endpoint_title = ""
                        elif user_input.startswith('-a'):
                            endpoint_title = user_input.split(" ")
                            self.endpoint_title = endpoint_title[1]
                            self.collection_id = ""
                    except IndexError:
                        print('Do not leave this section blank.')
                try:
                    if self.collection_id and int(self.collection_id) in self.collections:
                        break
                    elif self.endpoint_title in self.endpoints:
                        break
                    else:
                        print('Collection IDs:')
                        for k, v in self.collections.items():
                            print('{}: {}'.format(k, v.encode('ascii', 'ignore')))
                        print('\nAlternate Endpoints:')
                        for endpoint in self.endpoints:
                            print(endpoint.encode('ascii', 'ignore'))
                        print('\nYou do not have access to submit to the collection or alternate upload location: {} '.
                              format(self.collection_id or self.endpoint_title))
                        user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint> from the list above:')
                except (AttributeError, ValueError, TypeError):
                    print(
                        'Error: Input must start with either a -c or -a and be an integer or string value, respectively.')
                    user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
        else:
            exit_client(signal=signal.SIGINT,
                        message='The user {} does not have permission to submit to any collections'
                                ' or alternate upload locations.'.format(self.config.username))

    def build_package(self):
        self.set_upload_destination()
        self.package_info = {
            "package_info": {
                "dataset_description": self.dataset_name,
                "dataset_name": self.dataset_description,
                "collection_id": self.collection_id,
                "endpoint_title": self.endpoint_title
            },
            "validation_results":
                self.uuid
        }
        json_data = json.dumps(self.package_info)
        response, session = api_request(self, "POST", self.api, json_data)
        if response:
            try:
                self.package_id = response['submission_package_uuid']
                print('\n\nPackage Information:')
                print('validation results: [{}]'.format(",".join(response['validation_results'])))
                print('submission_package_uuid: {}'.format(str(response['submission_package_uuid'])))
                print('created date: {}'.format(str(response['created_date'])))
                print('expiration date: {}'.format(str(response['expiration_date'])))
            except KeyError:
                exit_client(signal=signal.SIGINT,
                            message='There was an error creating your package.')
            polling = 0
            sys.stdout.write('Building Package')
            while response['package_info']['status'] == 'processing':
                response, session = api_request(self, "GET", "/".join([self.api, self.package_id]), session=session)
                polling += 1
                sys.stdout.write('.')
                self.package_id = response['submission_package_uuid']
            if response['package_info']['status'] == 'complete':
                for f in [f for f in response['files']
                          if f['type'] in ('Submission Memento', 'Submission Data Package')]:
                    for key, value in f['_links'].items():
                        for k, v in value.items():
                            self.download_links.append((v, "/".join(f['path'].split('/')[4:])))
                print('\nPackage finished building.\n')
            else:
                exit_client(signal=signal.SIGINT,
                            message='There was an error in building your package.')
        else:
            exit_client(signal=signal.SIGINT,
                        message='There was an error with your package request.')

    def download_package(self):
        print('Downloading submission package.')
        folder = self.download_links[0][1]
        folder = folder.split('/')
        self.package_folder = folder[0]
        session = requests.session()
        total_package_size = 0
        for i, (url, file_name) in enumerate(self.download_links):
            r = session.get(url, auth=(self.username, self.password), stream=True)
            size = r.headers['content-length']
            total_package_size += int(size)
            self.download_links[i] = (url, file_name, int(size))
            r.close()
        package_download = tqdm(total=total_package_size,
                                unit="bytes",
                                unit_scale=True,
                                desc="Submission Package Download",
                                ascii=os.name == 'nt')
        for url, file_name, size in self.download_links:
            new_path = os.path.join(os.path.expanduser('~'), self.config.submission_packages)
            path = os.path.join(new_path, self.package_folder)
            if not os.path.exists(path):
                os.mkdir(path)
            file_name = file_name.split('/')
            file_name = file_name[1]
            file = os.path.join(path, file_name)
            r = session.get(url, auth=(self.username, self.password), stream=True)
            with open(file, 'wb') as out_file:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out_file.write(chunk)
                        package_download.update(sys.getsizeof(chunk))
            session.close()
        if package_download.total > package_download.n:
            package_download.update(package_download.total - package_download.n)
        package_download.close()


class Submission:
    def __init__(self, id, full_file_path, config=None, resume=False):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration()
        self.api = self.config.submission_api
        self.config.nda_login()
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
        print('Requesting submission for package: {}'.format(self.package_id))
        response, session = api_request(self, "POST", "/".join([self.api, self.package_id]))
        if response:
            self.status = response['submission_status']
            self.submission_id = response['submission_id']
            print('Submission ID: {}'.format(str(self.submission_id)))
        else:
            exit_client(signal=signal.SIGINT,
                        message='There was an error creating your submission.')

    def check_status(self):
        response, session = api_request(self, "GET", "/".join([self.api, self.submission_id]))
        if response:
            self.status = response['submission_status']
        else:
            exit_client(signal=signal.SIGINT,
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
                        message='There was an error requesting files for submission {}.'.format(submission_id))

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

    def submission_upload(self):
        print('Preparing to upload associated files.')
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
            self.total_progress = tqdm(total=self.total_upload_size,
                                       position=0,
                                       unit_scale=True,
                                       unit="bytes",
                                       desc="Total Upload Progress",
                                       ascii=os.name == 'nt')
            workers = []
            for x in range(self.cpu_num):
                worker = Submission.S3Upload(x)
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
                for progress in iter(self.progress_queue.get, None):
                    self.total_progress.update(progress)
                time.sleep(0.1)
            if self.total_progress.n < self.total_progress.total:
                self.total_progress.update(self.total_progress.total - self.total_progress.n)
            self.total_progress.close()
            session = None

        else:
            exit_client(signal=signal.SIGINT,
                        message='There was an error requesting submission {}.'.format(submission_id))
        print('\nUploads complete.')
        print('Checking Submission Status.')
        self.check_status()
        if self.status == 'Uploading':
            if not self.incomplete_files:
                t1 = time.time()
                while self.status == 'Uploading' and (time.time() - t1) < 120:
                    self.check_status()
                    sys.stdout.write('.')
                if self.status == 'Uploading':
                    exit_client(signal=signal.SIGINT,
                                message='Timed out while waiting for submission status to change.\n'
                                        'You may try again by resuming the submission: '
                                        'python nda-validationtool-client.py -r {}\n'.format(self.submission_id))
            else:
                print('There was an error transferring some files, trying again')
                if self.found_all_local_files and self.upload_tries < 5:
                    submission.submission_upload()
        if self.status != 'Uploading':
            print('\nYou have successfully completed uploading files for submission {}!'.format(
                self.submission_id))
            sys.exit(0)

    class S3Upload(threading.Thread):

        def __init__(self, index):
            threading.Thread.__init__(self)
            self.config = submission.config
            self.upload_queue = submission.upload_queue
            self.upload = None
            self.upload_tries = 0
            self.api = submission.api
            self.username = submission.username
            self.password = submission.password
            self.full_file_path = submission.full_file_path
            self.submission_id = submission.submission_id
            self.index = index + 1
            self.progress_queue = submission.progress_queue
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


def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to validate files before submitting into NDAR. '
                    'You must enter a list of at least one file to be validated. '
                    'If your data also includes associated files, you must enter a list of at least one directory '
                    'where the associated files are saved. Any files that are created while running the client '
                    '(ie. results files) will be downloaded in your home directory under NDARValidationResults '
                    'If your submission was interupted in the middle, '
                    'you may resume your upload by entering a valid submission ID. ',
        usage='%(prog)s <file_list>')

    parser.add_argument('files', metavar='<file_list>', type=str, nargs='+', action='store',
                        help='Returns validation results for list of files')

    parser.add_argument('-l', '--listDir', metavar='<directory_list>', type=str, nargs='+', action='store',
                        help='Specifies the directories in which the associated files are located.')

    parser.add_argument('-w', '--warning', action='store_true',
                        help='Returns validation warnings for list of files')

    parser.add_argument('-a', '--alternateEndpoint', metavar='<arg>', type=str, nargs=1, action='store',
                        help='An alternate upload location for the submission package')

    parser.add_argument('-b', '--buildPackage', action='store_true',
                        help='Flag whether to construct the submission package')

    parser.add_argument('-c', '--collectionID', metavar='<arg>', type=int, nargs=1, action='store',
                        help='The NDA collection ID')

    parser.add_argument('-d', '--description', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The description of the submission')

    parser.add_argument('-t', '--title', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The title of the submission')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, nargs=1, action='store',
                        help='NDA username')

    parser.add_argument('-p', '--password', metavar='<arg>', type=str, nargs=1, action='store',
                        help='NDA password')

    parser.add_argument('-r', '--resume', action='store_true',
                        help='Restart an in-progress submission, resuming from the last successful part in a multi-part'
                             'upload. Must enter a valid submission ID.')

    parser.add_argument('-v', '--validationAPI', metavar='<arg>', type=str, nargs=1, action='store',
                        help='URL of the validation tool API')

    parser.add_argument('-j', '--JSON', action='store_true',
                        help='Flag whether to additionally download validation results in JSON format.')
    args = parser.parse_args()

    return args


def exit_client(signal, frame=None, message=None):
    for thread in threading.enumerate():
        try:
            thread.shutdown_flag.set()
        except AttributeError:
            continue
    if message:
        print('\n\n{}'.format(message))
    else:
        print('\n\nExit signal received, shutting down...')
    print('Please contact NDAHelp@mail.nih.gov.')
    sys.exit(1)


def api_request(api, verb, endpoint, data=None, session=None):
    t1 = time.time()
    retry = requests.packages.urllib3.util.retry.Retry(
        total=20,
        read=20,
        connect=20,
        backoff_factor=3,
        status_forcelist=(400, 403, 404, 500, 502, 504)
    )

    headers = {'accept': 'application/json'}
    if api.__class__.__name__ == 'ValidationTask':
        auth = None
        headers.update({'content-type': 'text/csv'})
    else:
        auth = requests.auth.HTTPBasicAuth(api.config.username, api.config.password)
        headers.update({'content-type': 'application/json'})
    if not session:
        session = requests.session()
        session.mount(endpoint, HTTPAdapter(max_retries=retry))
    r = None
    response = None
    try:
        r = session.send(requests.Request(verb, endpoint, headers, auth=auth, data=data).prepare(),
                         timeout=300, stream=False)
    except requests.exceptions.RequestException as e:
        print('\nAn error occurred while making {} request, check your endpoint configuration:\n'.
              format(e.request.method))
        print(e)
        if api.__class__.__name__.endswith('Task'):
            api.shutdown_flag.set()
            thread.interrupt_main()
        exit_client(signal.SIGINT)

    if r and r.ok:
        try:
            response = json.loads(r.text)
        except ValueError:
            print('Your request returned an unexpected response, please check your endpoints.\n'
                  'Action: {}\n'
                  'Endpoint:{}\n'
                  'Status:{}\n'
                  'Reason:{}'.format(verb, endpoint, r.status_code, r.reason))
            if api.__class__.__name__.endswith('Task'):
                api.shutdown_flag.set()
                thread.interrupt_main()
            else:
                exit_client(signal.SIGINT)
    elif r.status_code == 401:
        tries = 0
        while r.status_code == 401 and tries < 5:
            print('The username or password is not recognized.')
            username = input('Please enter your username:')
            password = getpass.getpass('Please enter your password:')
            auth = requests.auth.HTTPBasicAuth(username, password)
            r = session.send(requests.Request(verb, endpoint, headers, auth=auth, data=data).prepare(),
                             timeout=300, stream=False)
            tries += 1
        if r.ok:
            response = json.loads(r.text)
            print('Authentication successful, updating username/password.')
            api.username = username
            api.config.username = username
            api.password = password
            api.config.password = password
        else:
            exit_client(signal.SIGINT, message='Too many unsuccessful authentication attempts.')
    return response, session


if __name__ == "__main__":
    errors = None
    signal.signal(signal.SIGINT, exit_client)
    config = ClientConfiguration()
    args = parse_args()
    if args.username:
        config.username = args.username[0]
    if args.password:
        config.password = args.password[0]
    if args.collectionID:
        config.collection_id = args.collectionID[0]
    if args.alternateEndpoint:
        config.endpoint_title = args.alternateEndpoint[0]
    if args.listDir:
        config.directory_list = args.listDir
    if args.title:
        config.title = ' '.join(args.title)
    if args.description:
        config.description = ' '.join(args.description)
    if args.validationAPI:
        config.validation_api = args.validationAPI[0]
    if args.JSON:
        config.JSON = True

    if args.resume:
        submission_id = args.files[0]
        submission = Submission(id=submission_id, full_file_path=None, config=config, resume=True)
        submission.check_status()
        if submission.status == 'Uploading':
            if submission.incomplete_files and submission.found_all_local_files:
                submission.submission_upload()
        else:
            print('Submission Completed with status {}'.format(submission.status))
            sys.exit(0)
    else:
        validation = Validation(args.files, config=config)
        validation.validate()
        validation.output()
        if args.warning:
            validation.warnings()
        else:
            if validation.w:
                print('\nNote: Your data has warnings. To save warnings, run again with -w argument.')
        print('\nAll files have finished validating.')

        # Test if no files passed validation, exit
        if not any(map(lambda x: not validation.uuid_dict[x]['errors'], validation.uuid_dict)):
            exit_client(signal=signal.SIGINT,
                        message='No files passed validation, please correct any errors and validate again.')
        # If some files passed validation, show files with and without errors
        else:
            print('\nThe following files passed validation:')
            for uuid in validation.uuid_dict:
                if not validation.uuid_dict[uuid]['errors']:
                    print('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
            if validation.e:
                print('\nThese files contain errors:')
                for uuid in validation.uuid_dict:
                    if validation.uuid_dict[uuid]['errors']:
                        print('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
        # If some files had errors, give option to submit just the files that passed
        if validation.e:
            while True:
                proceed = input('Some files have errors, do you want to continue '
                                'and submit ONLY the files that have passed validation? <Yes/No>: ')
                if str(proceed).lower() == 'no':
                    sys.exit()
                elif str(proceed).lower() == 'yes':
                    validation.uuid = []
                    for uuid in validation.uuid_dict:
                        if not validation.uuid_dict[uuid]['errors']:
                            validation.uuid.append(uuid)
                    break
                else:
                    print('Your answer <{}> was not recognized, please enter yes or no.'.format(str(proceed)))
                    continue
        # If user requested to build a package
        if args.buildPackage:
            validation.file_search()
            package = SubmissionPackage(validation.uuid, validation.full_file_path, config=config)
            package.build_package()
            package.download_package()
            print('\nA copy of your submission package has been saved to: {}'.
                  format(os.path.join(package.package_folder, package.config.submission_packages)))
            submission = Submission(package.package_id, package.full_file_path, config=config)
            submission.submit()
            if validation.associated_files:
                submission.submission_upload()
            else:
                print('You have successfully completed uploading your files from package {}!'.format(
                    submission.submission_id))
                sys.exit(0)
