from __future__ import absolute_import, with_statement

import csv
import multiprocessing
import sys

from tqdm import tqdm

import NDATools

if sys.version_info[0] < 3:
    import Queue as queue

    input = raw_input
else:
    import queue
from NDATools.Configuration import *
from NDATools.Utils import *
import threading
import signal

logger = logging.getLogger(__name__)
class Validation:
    def __init__(self, file_list, config, hide_progress, allow_exit=False, thread_num=None,
                 pending_changes=None, original_uuids=None):
        self.config = config
        self.hide_progress = hide_progress
        self.api = self.config.validation_api.strip('/')
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
        self.manifest_data_files = []
        self.manifest_path = self.config.manifest_path
        if self.config.JSON:
            self.log_file = os.path.join(NDATools.NDA_TOOLS_VAL_FOLDER, 'validation_results_{}.json'.format(self.date))
        else:
            self.log_file = os.path.join(NDATools.NDA_TOOLS_VAL_FOLDER, 'validation_results_{}.csv'.format(self.date))

        self.field_names = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'COLUMN', 'MESSAGE', 'RECORD']
        self.validation_progress = None
        self.pending_changes = pending_changes
        self.exit = allow_exit
        self.original_uuids = original_uuids
        self.data_structures_with_missing_rows = None
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
                'associated_file_paths': set(response['associated_file_paths']),
                'rows': response['rows'],
                'manifests': {manifest['localFileName'] for manifest in response['manifests']}
            }

        if self.pending_changes:
            # remove the associated_files that have already been uplaoded
            structure_to_new_associated_files = {}
            unrecognized_ds = set()
            for uuid in self.uuid_dict:
                short_name = self.uuid_dict[uuid]['short_name']
                structure_to_new_associated_files[short_name] = set()
            for uuid in self.uuid_dict:
                short_name = self.uuid_dict[uuid]['short_name']
                structure_to_new_associated_files[short_name].update(
                    {file for file in self.uuid_dict[uuid]['associated_file_paths']})

            files_to_upload = set()
            for data_structure in structure_to_new_associated_files:
                expected_change_for_data_structure = next(
                    filter(lambda pending_change: pending_change['shortName'] == data_structure, self.pending_changes),
                    None)
                if expected_change_for_data_structure is not None:
                    original_associated_file_set = {associated_file for associated_file in
                                                    expected_change_for_data_structure['associatedFiles']}
                    for new_asssociated_file in structure_to_new_associated_files[data_structure]:
                        if new_asssociated_file not in original_associated_file_set:
                            files_to_upload.update({new_asssociated_file})
                else:
                    unrecognized_ds.update({data_structure})


            self.associated_files_to_upload = set(files_to_upload)
            if len(files_to_upload) > 0:
                logger.info ('\nDetected {} new files that need to be uploaded for submission.\n'.format(len(files_to_upload)))
            else:
                logger.info('\nDetected that all associated files have been previously uploaded from previous submission\n')

            # Find datastructures with missing data
            # create a map of short_name to num-rows for files that the user uploaded
            structure_to_new_row_count = {}
            for uuid in self.uuid_dict:
                short_name = self.uuid_dict[uuid]['short_name']
                structure_to_new_row_count[short_name] = 0
            for uuid in self.uuid_dict:
                short_name = self.uuid_dict[uuid]['short_name']
                structure_to_new_row_count[short_name] += self.uuid_dict[uuid]['rows']

            data_structures_with_missing_rows = []
            for data_structure in structure_to_new_row_count:
                expected_change_for_data_structure = next(
                    filter(lambda pending_change: pending_change['shortName'] == data_structure, self.pending_changes),
                    None)
                if expected_change_for_data_structure is not None:
                    if structure_to_new_row_count[data_structure] < expected_change_for_data_structure['rows']:
                        data_structures_with_missing_rows.append((data_structure,
                                                                  expected_change_for_data_structure['rows'],
                                                                  structure_to_new_row_count[data_structure]))
                else:
                    unrecognized_ds.update({data_structure})

            # update list of validation-uuids to be used during the packaging step
            new_uuids, unrecognized_ds = self.generate_uuids_for_qa_workflow(unrecognized_ds)

            if unrecognized_ds:
                message = 'ERROR - The following datastructures cannot be used with the qa token provided: '
                message += "\r\n" + "\r\n".join(unrecognized_ds)
                exit_client(signal=signal.SIGTERM, message=message)
            else:
                self.data_structures_with_missing_rows = data_structures_with_missing_rows
                self.uuid = new_uuids

    def get_existing_manifests(self):
        if not self.pending_changes:
            return set()
        # create a set of manifest files from the set of pending changes to enforce no duplicates
        # convert back into a list because the process_manifests method expects a list
        return {manifest for manifests in list(map(lambda change: change['manifests'], self.pending_changes)) for manifest
             in manifests}


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
                         'MESSAGE': 'None', 'RECORD': 'None'})
                else:
                    for error, value in response['errors'].items():
                        for v in value:
                            column = v['columnName']
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
                                {'FILE': file_name,
                                 'ID': response['id'],
                                 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'],
                                 'WARNINGS': warning,
                                 'MESSAGE': x,
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
        if self.original_uuids:
            uuid_list = self.generate_uuids_for_qa_workflow()[0]
        return uuid_list

    def generate_uuids_for_qa_workflow(self, unrecognized_ds=set()):
        unrecognized_structures = set(unrecognized_ds)
        new_uuids = set(self.original_uuids)
        val_by_short_name = {}
        for uuid in self.uuid_dict:
            short_name = self.uuid_dict[uuid]['short_name']
            val_by_short_name[short_name] = set()
        for uuid in self.uuid_dict:
            short_name = self.uuid_dict[uuid]['short_name']
            val_by_short_name[short_name].update({uuid})
        for short_name in val_by_short_name:
            # find the pending change with the same short name
            matching_change = {}
            for change in self.pending_changes:
                if change['shortName'] == short_name:
                    matching_change = change
            if not matching_change:
                unrecognized_structures.add(short_name)
            else:
                # prevValidationUuids is the set of validation-uuids on the pending changes resource
                new_uuids = new_uuids.difference(set(matching_change['validationUuids']))
                new_uuids.update({res for res in val_by_short_name[short_name]})

        return list(new_uuids), unrecognized_structures

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
                            sys.stdout.write('\rUploaded manifest file {} of {}'.format(uploaded_count, files_to_upload))
                            sys.stdout.flush()
                            break
                        except IOError:
                            no_manifest.add(validation_manifest.local_file_name)
                        except FileNotFoundError:
                            no_manifest.add(validation_manifest.local_file_name)

                        except json.decoder.JSONDecodeError as e:
                            error = 'JSON Error: There was an error in your json file: {}\nPlease review and try again: {}\n'.format \
                                (validation_manifest.local_file_name, e)
                            raise Exception(error)

        for file in no_manifest:
            logger.info('Manifest file not found: %s',file)

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
        def __init__(self, file_queue, result_queue, api, scope, responses, validation_progress, exit, validation_timeout, auth):
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

        def run(self):
            while True and not self.shutdown_flag.is_set():
                polling = 0
                file_name = self.file_queue.get()
                if file_name == "STOP":
                    self.file_queue.put("STOP")
                    self.shutdown_flag.set()
                    break
                try:
                    file = open(file_name, 'r')
                except IOError:
                    if self.progress_bar:
                        self.progress_bar.close()
                    message = 'This file does not exist in current directory: {}'.format(file_name)
                    logger.error(message)

                    exit_client()

                data = file.read()

                response = post_request(self.api_scope, data, timeout=self.validation_timeout, headers = {'content-type':'text/csv'}, auth=self.auth)
                while response and not response['done']:
                    response = get_request("/".join([self.api, response['id']]), auth=self.auth)
                    time.sleep(polling)
                    polling += 1
                if response:
                    response = get_request("/".join([self.api, response['id']]), auth=self.auth)
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
