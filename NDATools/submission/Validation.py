from __future__ import absolute_import, with_statement

import csv
import json
import multiprocessing
import sys

from tqdm import tqdm

from NDATools.utils.ThreadPool import ThreadPool

if sys.version_info[0] < 3:
    import Queue as queue
    input = raw_input
else:
    import queue
from NDATools.submission.Configuration import *
from NDATools.utils.Utils import *


class Validation:
    def __init__(self, file_list, config, hide_progress, allow_exit=False, thread_num=None):
        self.config = config
        self.hide_progress = hide_progress
        self.api = self.config.validation_api
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
        self.associated_files = []
        self.uuid = []
        self.uuid_dict = {}
        self.responses = []
        self.date = time.strftime("%Y%m%dT%H%M%S")
        self.e = False
        self.w = False
        self.manifest_data_files = []
        self.manifest_path = self.config.manifest_path
        if self.config.JSON:
            self.log_file = os.path.join(NDATools.NDA_TOOLS_VAL_FOLDER, 'validation_results_{}.json'.format(self.date))
        else:
            self.log_file = os.path.join(NDATools.NDA_TOOLS_VAL_FOLDER, 'validation_results_{}.csv'.format(self.date))

        self.field_names = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'COLUMN', 'MESSAGE', 'RECORD']
        self.validation_progress = None
        self.exit = allow_exit

    def get_validation_request(self, validation_id):
        response = advanced_request(self.api+'/{}', path_params=[validation_id])
        return response

    def post_validation_request(self, data):
        response = advanced_request(self.api, verb=Verb.POST, data=data, content_type=ContentType.CSV)
        return response

    """
    Validates a list of csv files and saves a new csv file with the results in the Home directory.
    If data includes associated files and user has not entered any directories where they are saved, user is prompted to 
    enter a list of directories only if they have also indicated to build a package for submission.
    """

    def validate(self):
        def validate_task_helper(file_name):
            with open(file_name, 'r') as f:
                data = f.read()
                response = self.post_validation_request(data)
                polling=0
                while response and not response['done']:
                    response = self.get_validation_request(response['id'])
                    time.sleep(0.1)
                    polling += 1
                    if polling >= 50:
                        raise Exception('File {} failed to validate.'.format(file_name))
                return response, file_name

        def validate_task(file):
            response, file = validate_task_helper(file)
            self.responses.append((response, file))
            if self.validation_progress:
                self.validation_progress.update(n=1)

        if not self.hide_progress:
            self.validation_progress = tqdm(total=len(self.file_list), position=0, ascii=os.name == 'nt')

        validate_pool = ThreadPool(self.thread_num)
        for file in self.file_list:
            validate_pool.map(validate_task, [file])
        validate_pool.wait_completion()

        if not self.hide_progress:
            self.validation_progress.close()
        for (response, file) in self.responses:
            if response['status'] == Status.SYSERROR:
                self.e = True
                error = " ".join([response['status'], response['errors']['system'][0]['message']])
                raise Exception(error)
            elif response['errors'] != {}:
                self.e = True
            if response['associated_file_paths'] and response['errors'] == {}:
                self.associated_files.append(response['associated_file_paths'])
            if response['warnings'] != {}:
                self.w = True
            if response['status'] == Status.PENDING_MANIFEST and response['errors'] == {}:
                self.process_manifests(response)
                response = self.get_validation_request(response['id'])
                self.associated_files.append(response['associated_file_paths'])

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
                         'EXPIRATION_DATE': response['expiration_date'], 'ERRORS': 'None', 'COLUMN': 'None',
                         'MESSAGE':'None','RECORD': 'None'})
                else:
                    for error, value in response['errors'].items():
                        for v in value:
                            column = v['columnName'] if 'columnName' in v else ' '
                            message = v['message']
                            try:
                                record = v['recordNumber']
                            except KeyError:
                                record = ' '
                            writer.writerow(
                                {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'], 'ERRORS': error, 'COLUMN': column,
                                 'MESSAGE': message, 'RECORD': record})
            csvfile.close()

    def get_warnings(self):
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
                new_path = ''.join([NDATools.NDA_TOOLS_VAL_FOLDER, '/validation_warnings_', self.date, '.json'])
                with open(new_path, 'w') as outfile:
                    json.dump(json_data, outfile)
        else:
            new_path = ''.join([NDATools.NDA_TOOLS_VAL_FOLDER, '/validation_warnings_', self.date, '.csv'])
            if sys.version_info[0] < 3:
                csvfile = open(new_path, 'wb')
            else:
                csvfile = open(new_path, 'w', newline='')
            fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'WARNINGS', 'MESSAGE', 'COUNT']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for (response, file) in self.responses:
                file_name = self.uuid_dict[response['id']]['file']
                if response['warnings'] == {}:
                    writer.writerow(
                        {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                         'EXPIRATION_DATE': response['expiration_date'], 'WARNINGS': 'None', 'MESSAGE': 'None',
                         'COUNT': '0'})
                else:
                    for warning, value in response['warnings'].items():
                        m = {}
                        warning_list = []
                        for v in value:
                            count = 1
                            if v not in warning_list:
                                warning_list.append(v)
                                message = v['message']
                                m[message] = count
                            else:
                                count = m[message] + 1
                                m[message] = count
                        for x in m:
                            writer.writerow(
                                {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'], 'WARNINGS': warning, 'MESSAGE': x,
                                 'COUNT': m[x]})
            csvfile.close()

    def verify_uuid(self):
        uuid_list = []
        for uuid in self.uuid_dict:
            if not self.uuid_dict[uuid]['errors']:
                uuid_list.append(uuid)

        for response in self.manifest_data_files:
            if response['id'] not in uuid_list:
                self.manifest_data_files.remove(response)

        return uuid_list

    def process_manifests(self, r, validation_results = None, yes_manifest = None, bulk_upload=False):
        if not self.manifest_path:
            if not self.exit:
                error = 'Missing Manifest File: You must include the path to your manifests files'
                raise Exception(error)
            else:
                manifest_path_input = input("\nYour data contains manifest files. Please enter a list of "
                                            "the complete paths to the folder where your manifests are located,"
                                            "separated by a space:")
                self.manifest_path = manifest_path_input.split(' ')

        if not yes_manifest:
            yes_manifest = []

        no_manifest = set()

        if validation_results:
            self.validation_result = validation_results
        else:
            self.validation_result = ValidationManifestResult(r, self.hide_progress)
        if not bulk_upload:

            for validation_manifest in self.validation_result.manifests:
                for m in self.manifest_path:
                    if validation_manifest.local_file_name not in yes_manifest:
                        try:
                            manifest_path = os.path.join(m, validation_manifest.local_file_name)
                            validation_manifest.upload_manifest(manifest_path)
                            yes_manifest.append(validation_manifest.local_file_name)
                            if validation_manifest.local_file_name in no_manifest:
                                no_manifest.remove(validation_manifest.local_file_name)
                            break
                        except IOError:
                            no_manifest.add(validation_manifest.local_file_name)
                        except FileNotFoundError:
                            no_manifest.add(validation_manifest.local_file_name)

                        except json.decoder.JSONDecodeError as e:
                            error = 'JSON Error: There was an error in your json file: {}\nPlease review and try again: {}\n'.format\
                                (validation_manifest.local_file_name, e)
                            raise Exception(error)

        for file in no_manifest:
            print('Manifest file not found:',file)

        if no_manifest:
            print(
                '\nYou must make sure all manifest files listed in your validation file'
                ' are located in the specified directory. Please try again.')

            retry = input('Press the "Enter" key to specify location(s) for manifest files and try again:')
            self.manifest_path = retry.split(' ')
            self.process_manifests(r, yes_manifest=yes_manifest, validation_results = self.validation_result)

        while not self.validation_result.status.startswith(Status.COMPLETE):
            response = self.get_validation_request(r['id'])
            self.validation_result = ValidationManifestResult(response, self.hide_progress)
            for m in self.validation_result.manifests:
                if m.status == Status.ERROR:
                    error = 'JSON Error: There was an error in your json file: {}\nPlease review and try again: {}\n'.format(m.local_file_name, m.errors[0])
                    raise Exception(error)

class ValidationManifest:

    def __init__(self, _manifest, hide):
        self.config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
        self.status = _manifest['status']
        self.local_file_name = _manifest['localFileName']
        self.manifestUuid = _manifest['manifestUuid']
        self.errors = _manifest['errors']
        self.url = _manifest['_links']['self']['href']
        self.associated_files = []
        self.hide_progress = hide

    def upload_manifest(self, _fp):
        with open (_fp,'r') as f:
            data = f.read()
            advanced_request(self.url, verb=Verb.PUT, data=data, content_type=ContentType.JSON)

class ValidationManifestResult:

    def __init__(self, _validation_result, hide):
        self.done = _validation_result['done']
        self.id = _validation_result['id']
        self.status = _validation_result['status']
        self.errors = _validation_result['errors']
        self.short_name = _validation_result['short_name']
        self.scope = _validation_result['scope']
        manifests = []
        for _manifest in _validation_result['manifests']:
            manifests.append(ValidationManifest(_manifest, hide))
        self.manifests = manifests


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    PENDING_MANIFEST = 'PendingManifestFiles'
    COMPLETE = 'Complete'
    ERROR = 'Error'
