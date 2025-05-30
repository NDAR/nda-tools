import json
import logging
import multiprocessing
import os
import queue
import sys
import threading
import time
from typing import List

import requests
from pydantic import BaseModel, Field
from tqdm import tqdm

from NDATools.Utils import get_request, put_request, Protocol, post_request

logger = logging.getLogger(__name__)


class Manifests(BaseModel):
    local_file_name: str = Field(..., alias='localFileName')


class ValidationV1(BaseModel):
    uuid: str = Field(..., alias='id')
    associated_file_paths: List[str]
    manifests: List[Manifests]


class ValidationV1Api:
    def __init__(self, validation_api_endpoint):
        self.api_endpoint = f"{validation_api_endpoint}"

    def get_validation(self, uuid):
        tmp = get_request(f"{self.api_endpoint}/{uuid}")
        return ValidationV1(**tmp)


class Validation:
    def __init__(self, file_list, config, hide_progress, allow_exit=False, thread_num=None):
        self.config = config
        self.hide_progress = hide_progress
        self.api = self.config.validation_api_endpoint.strip('/')
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num
        self.scope = self.config.scope
        self.api_scope = self.api
        if self.scope is not None:
            self.api_scope = "".join([self.api, '/?scope={}'.format(self.scope)])

        self.file_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.file_list = file_list
        self.directory_list = self.config.directory_list
        self.associated_files_to_upload = set()
        self.uuid = []
        self.uuid_dict = {}
        self.responses = []
        self.date = time.strftime("%Y%m%dT%H%M%S")
        self.e = False
        self.w = False
        self.manifest_path = self.config.manifest_path
        self.field_names = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'COLUMN', 'MESSAGE', 'RECORD']
        self.validation_progress = None
        self.exit = allow_exit
        if self.config.password:
            self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        else:
            self.auth = None

    """
    Validates a list of csv files and saves a new csv file with the results in the Home directory.
    If data includes associated files and user has not entered any directories where they are saved, user is prompted to 
    enter a list of directories only if they have also indicated to build a package for submission.
    """

    def validate(self):
        if not self.hide_progress:
            self.validation_progress = tqdm(total=len(self.file_list), position=0, ascii=os.name == 'nt')
        workers = []
        for x in range(self.thread_num):
            worker = Validation.ValidationTask(self.file_queue, self.result_queue, self.api, self.api_scope,
                                               self.responses, self.validation_progress, self.exit,
                                               self.config.validation_timeout, self.auth)

            workers.append(worker)
            worker.daemon = True
            worker.start()
        for file in self.file_list:
            self.file_queue.put(file)
        self.file_queue.put("STOP")
        while any(map(lambda x: x.is_alive(), workers)):
            time.sleep(2)
        if not self.hide_progress:
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
            if response['status'] == Status.SYSERROR:
                self.e = True
                error = " ".join([response['status'], response['errors']['system'][0]['message']])
                raise Exception(error)
            elif response['errors'] != {}:
                self.e = True
            if response['associated_file_paths'] and response['errors'] == {}:
                self.associated_files_to_upload.update(set(response['associated_file_paths']))
            if response['warnings'] != {}:
                self.w = True
            if response['status'] == Status.PENDING_MANIFEST and response['errors'] == {}:
                self.process_manifests(response)
                response = get_request("/".join([self.api, response['id']]), auth=self.auth)
                self.associated_files_to_upload.update(set(response['associated_file_paths']))

            self.uuid.append(response['id'])
            self.uuid_dict[response['id']] = {
                'file': file, 'errors': response['errors'] != {},
                'short_name': response['short_name'],
                'rows': response['rows'],
                'manifests': {manifest['localFileName'] for manifest in response['manifests']}
            }

    def process_manifests(self, r, validation_results=None, yes_manifest=set(), bulk_upload=False):
        if not self.manifest_path:
            if not self.exit:
                error = 'Missing Manifest File: You must include the path to your manifests files'
                raise Exception(error)
            else:
                manifest_path_input = input("\nYour data contains manifest files. Please enter a list of "
                                            "the complete paths to the folder where your manifests are located,"
                                            "separated by a space:")
                self.manifest_path = manifest_path_input.split(' ')
        no_manifest = set()

        if validation_results:
            self.validation_result = validation_results
        else:
            self.validation_result = Validation.ValidationManifestResult(r, self)
        if not bulk_upload:
            files_to_upload = len(self.validation_result.manifests) - len(yes_manifest)
            uploaded_count = 0
            for validation_manifest in self.validation_result.manifests:
                for m in self.manifest_path:
                    if validation_manifest.local_file_name not in yes_manifest:
                        try:
                            manifest_path = os.path.join(m, validation_manifest.local_file_name)
                            validation_manifest.upload_manifest(open(manifest_path, 'rb'))
                            yes_manifest.add(validation_manifest.local_file_name)
                            no_manifest.discard(validation_manifest.local_file_name)
                            uploaded_count += 1
                            sys.stdout.write(
                                '\rUploaded manifest file {} of {}\n'.format(uploaded_count, files_to_upload))
                            sys.stdout.flush()
                            break
                        except IOError:
                            no_manifest.add(validation_manifest.local_file_name)

                        except json.decoder.JSONDecodeError as e:
                            error = 'JSON Error: There was an error in your json file: {}\nPlease review and try again: {}\n'.format \
                                (validation_manifest.local_file_name, e)
                            raise Exception(error)

        for file in no_manifest:
            logger.info('Manifest file not found: %s', file)

        if no_manifest:
            logger.info(
                '\nYou must make sure all manifest files listed in your validation file'
                ' are located in the specified directory. Please try again.')

            retry = input('Press the "Enter" key to specify location(s) for manifest files and try again:')
            self.manifest_path = retry.split(' ')
            self.process_manifests(r, yes_manifest=yes_manifest, validation_results=self.validation_result)
        else:
            logger.info('\r\nFinished uploading all manifest files')
        while not self.validation_result.status.startswith(Status.COMPLETE):
            time.sleep(1.1)
            response = get_request("/".join([self.api, r['id']]), auth=self.auth)
            self.validation_result = Validation.ValidationManifestResult(response, self)
            for m in self.validation_result.manifests:
                if m.status == Status.ERROR:
                    error = 'JSON Error: There was an error in your json file: {}\nPlease review and try again: {}\n'.format(
                        m.local_file_name, m.errors[0])
                    raise Exception(error)

    class ValidationManifest:

        def __init__(self, _manifest, validation):
            self.config = validation.config
            self.status = _manifest['status']
            self.local_file_name = _manifest['localFileName']
            self.manifestUuid = _manifest['manifestUuid']
            self.errors = _manifest['errors']
            self.url = _manifest['_links']['self']['href']
            self.hide_progress = validation.hide_progress
            self.auth = validation.auth

        def upload_manifest(self, _fp):
            manifest_object = json.load(_fp)
            put_request(self.url, payload=manifest_object, auth=self.auth)

    class ValidationManifestResult:

        def __init__(self, _validation_result, validation):
            self.done = _validation_result['done']
            self.id = _validation_result['id']
            self.status = _validation_result['status']
            self.errors = _validation_result['errors']
            self.short_name = _validation_result['short_name']
            self.scope = _validation_result['scope']
            manifests = []
            for _manifest in _validation_result['manifests']:
                manifests.append(Validation.ValidationManifest(_manifest, validation))
            self.manifests = manifests

    class ValidationTask(threading.Thread, Protocol):
        def __init__(self, file_queue, result_queue, api, scope, responses, validation_progress, exit,
                     validation_timeout, auth):
            threading.Thread.__init__(self)
            self.file_queue = file_queue
            self.result_queue = result_queue
            self.api = api
            self.api_scope = scope
            self.responses = responses
            self.progress_bar = validation_progress
            self.shutdown_flag = threading.Event()
            self.exit = exit
            self.validation_timeout = validation_timeout
            self.auth = auth

        @staticmethod
        def get_protocol(cls):
            return cls.CSV

        def _get_validation(self, validation_id):
            return get_request("/".join([self.api, validation_id]), auth=self.auth)

        def _create_validation(self, data):
            return post_request(self.api_scope, data, timeout=self.validation_timeout,
                                headers={'content-type': 'text/csv'}, auth=self.auth)

        def run(self):
            while True and not self.shutdown_flag.is_set():
                polling = 0
                file_name = self.file_queue.get()
                if file_name == "STOP":
                    self.file_queue.put("STOP")
                    self.shutdown_flag.set()
                    break
                try:
                    file = open(file_name, 'r', encoding='utf-8')
                except IOError:
                    if self.progress_bar:
                        self.progress_bar.close()
                    message = 'This file does not exist in current directory: {}'.format(file_name)
                    logger.error(message)
                    exit_error()

                response = self._create_validation(file.read().encode('utf-8'))
                while response and not response['done']:
                    response = self._get_validation(response['id'])
                    time.sleep(polling)
                    polling += 1
                if response:
                    self.result_queue.put((response, file_name))
                    if self.progress_bar:
                        self.progress_bar.update(n=1)
                # Stop thread after adding validation response
                self.file_queue.task_done()


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    PENDING_MANIFEST = 'PendingManifestFiles'
    COMPLETE = 'Complete'
    ERROR = 'Error'
