from __future__ import with_statement
from __future__ import absolute_import
import sys
import os
import time
import csv
import threading
import multiprocessing
import json
from tqdm import tqdm

if sys.version_info[0] < 3:
    import Queue as queue

    input = raw_input
else:
    import queue
from NDATools.Configuration import *





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

        self.field_names = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'MESSAGE', 'COUNT']
        self.validation_progress = None

    """
    Validates a list of csv files and saves a new csv file with the results in the Home directory.
    If data includes associated files and user has not entered any directories where they are saved, user is prompted to 
    enter a list of directories only if they have also indicated to build a package for submission.
    """

    def validate(self, hide_progress=True):

        # find out how many cpu in your computer to get max threads
        if not hide_progress:
            self.validation_progress = tqdm(total=len(self.file_list), position=0, ascii=os.name == 'nt')
        cpu_num = multiprocessing.cpu_count()
        if cpu_num > 1:
            cpu_num -= 1
        workers = []
        for x in range(cpu_num):
            if hide_progress:
                worker = Validation.ValidationTask(self.file_queue, self.result_queue, self.api, self.responses)
            else:
                worker = Validation.ValidationTask(self.file_queue, self.result_queue, self.api, self.responses, self.validation_progress)
            workers.append(worker)
            worker.daemon = True
            worker.start()
        for file in self.file_list:
            self.file_queue.put(file)
        self.file_queue.put("STOP")
        while any(map(lambda x: x.is_alive(), workers)):
            time.sleep(0.1)
        if not hide_progress:
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
                self.e = True
                response['errors'].update({'SystemError': [
                    {'message': 'SystemError while validating {}'.format(file)}
                ]})
            elif response['errors'] != {}:
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
                         'COUNT': '0'})
                else:
                    for error, value in response['errors'].items():
                        m = {}
                        error_list = []
                        for v in value:
                            count = 1
                            if v not in error_list:
                                error_list.append(v)
                                message = v['message']
                                m[message] = count
                            else:
                                m[message] = m[message] + 1
                        for x in m:
                            writer.writerow(
                                {'FILE': file_name, 'ID': response['id'], 'STATUS': response['status'],
                                 'EXPIRATION_DATE': response['expiration_date'], 'ERRORS': error, 'MESSAGE': x,
                                 'COUNT': m[x]})
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
                new_path = ''.join([self.validation_result_dir, '/validation_warnings_', self.date, '.json'])
                with open(new_path, 'w') as outfile:
                    json.dump(json_data, outfile)
        else:
            new_path = ''.join([self.validation_result_dir, '/validation_warnings_', self.date, '.csv'])
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
        return uuid_list


    class ValidationTask(threading.Thread):
        def __init__(self, file_queue, result_queue, api, responses, validation_progress=None):
            threading.Thread.__init__(self)
            self.file_queue = file_queue
            self.result_queue = result_queue
            self.api = api
            self.responses = responses
            self.progress_bar = validation_progress
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
                    if self.progress_bar:
                        self.progress_bar.update(n=1)
                # Stop thread after adding validation response
                self.file_queue.task_done()

