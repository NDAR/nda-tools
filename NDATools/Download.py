from __future__ import absolute_import
from __future__ import with_statement

import sys

from requests import HTTPError

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
    from io import open
else:
    from queue import Queue
import csv
from threading import Thread
import botocore
import multiprocessing
from NDATools.Utils import *
import requests

class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(e)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()


class Download(Protocol):

    def __init__(self, directory, config=None, verbose=False):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration()
        self.url = self.config.datamanager_api  # todo: delete me
        self.package_url = self.config.package_api
        self.datadictionary_url = self.config.datadictionary_api
        self.username = config.username
        self.password = config.password
        self.directory = directory
        self.download_queue = Queue()
        self.path_list = set()
        self.local_file_names = {}
        self.access_key = None
        self.secret_key = None
        self.session = None
        self.associated_files = False
        self.dsList = []
        self.verbose = verbose
        self.s3_links = {}
        self.package_file_id_list = set()
        self.package_file_download_errors = set()

    @staticmethod
    def get_protocol(cls):
        return cls.XML

    def verbose_print(self, *args):
        if self.verbose:
            print(' '.join(list(args)))

    def get_presigned_urls(self):
        """ Download package files (not associated files) """
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}
        url = self.package_url + '/{}/files?page=1&size=all'.format(self.package)
        tmp = requests.get(url, headers, auth=auth)
        tmp.raise_for_status()
        response = json.loads(tmp.text)

        for element in response['results']:
            associated = element['associatedFile']
            # if not associated:  # Does not loaded associated files
            alias = element['download_alias']
            package_file_id = element['package_file_id']
            self.package_file_id_list.add(package_file_id)
            self.local_file_names[package_file_id] = alias

        logging.debug("Generating download links...")
        self.__chunk_file_id_list()

    def __chunk_file_id_list(self):
        """
        Chunk requests due to max batch size 0f 50,000. Subsequently calls __post_for_s3_links to get presigned urls.
        """
        self.package_file_id_list = list(self.package_file_id_list)  # Convert set to list
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}
        max_batch_size = 50000
        if len(self.package_file_id_list) > max_batch_size:
            chunks = [self.package_file_id_list[i:i + max_batch_size] for i in
                      range(0, len(self.package_file_id_list), max_batch_size)]
            for chunk in chunks:
                self.__post_for_s3_links(chunk, auth, headers)
        else:
            self.__post_for_s3_links(self.package_file_id_list, auth, headers)

        self.verbose_print('Downloading package files for package {}.'.format(self.package))

    def __post_for_s3_links(self, id_list, auth, headers):
        """
        Stores key-value pairs of (key: package_file_id, value: presigned URL)
        :param id_list: List of package file IDs with max size of 50,000
        """
        url = self.package_url + '/{}/files/batchGeneratePresignedUrls'.format(self.package)
        tmp = requests.post(url, headers=headers, json=id_list, auth=auth)
        tmp.raise_for_status()
        response = json.loads(tmp.text)

        for element in response['presignedUrls']:
            download_url = element['downloadURL']
            package_file_id = element['package_file_id']
            self.s3_links[package_file_id] = download_url

    def use_s3_links_file(self):
        """
        Reads a text file line by line, collects all S3 URLs, and requests the package file ids
        from the package service for those files.
        """
        try:
            with open(self.dataStructure, 'r', encoding='utf-8') as s3_links_file:
                for line in s3_links_file:
                    if line.startswith('s3://'):
                        self.path_list.add(line.strip())
            self.get_package_file_ids()
        except IOError as e:
            self.verbose_print(
                '{} not found. Please enter the correct path to your file and try again.'.format(self.dataStructure))
            raise e

    def use_data_structure(self):
        """
        Downloads a data structure file from the package, the data_structure_manifest file,
        parses both files to identify all associated files for the data structure, and then
        requests the package file ids for those files from the package service
        """
        data_structure_name = self.dataStructure.split(os.path.sep)[-1].split('.txt')[0]
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}
        url = self.package_url + '/{}/files?page=1&size=all&types=Data'.format(self.package)
        tmp = requests.get(url, headers, auth=auth)
        tmp.raise_for_status()
        response = json.loads(tmp.text)
        has_data_structure = False
        has_data_structure_manifest = False
        for element in response['results']:
            # api response is always lower-case
            if data_structure_name.lower() == element['download_alias'].split('.txt')[0]:
                pfi = list()
                pfi.append(element['package_file_id'])
                self.__post_for_s3_links(pfi, auth, headers)
                self.local_file_names[element['package_file_id']] = element['download_alias']
                self.download_from_s3link(element['package_file_id'], True, self.directory)
                has_data_structure = True
            if element['download_alias'] == 'datastructure_manifest.txt':
                pfi = list()
                pfi.append(element['package_file_id'])
                self.__post_for_s3_links(pfi, auth, headers)
                self.local_file_names[element['package_file_id']] = element['download_alias']
                self.download_from_s3link(element['package_file_id'], True, self.directory)
                has_data_structure_manifest = True
            if has_data_structure_manifest and has_data_structure:
                break
        if not has_data_structure:
            structures = [f['download_alias'].replace('.txt', '') for f in response['results'] if
                          f['nda_file_type'] == 'Data']
            structures.sort()
            print('{} data structure is not included in the package'.format(self.dataStructure))
            print('Valid structures for this package are:\n{}'.format('\n'.join(structures)))
            sys.exit(1)

        def __get_manifest_and_file_elements(data_structure_name):

            file_elements = []
            manifest_elements = []

            # Parse the data dictionary api results and create a list of file elements and manifest elements.
            tmp = requests.get(self.datadictionary_url + '/{}'.format(data_structure_name))
            tmp.raise_for_status()
            for el in tmp.json()['dataElements']:
                if el['type'] == 'File':
                    file_elements.append(el['name'])
                elif el['type'] == 'Manifest':
                    manifest_elements.append(el['name'])
            return file_elements, manifest_elements

        file_elements, manifest_elements = __get_manifest_and_file_elements(data_structure_name)
        manifest_names = {}

        data_structure_file_links = set()
        data_structure_manifest_file_links = set()

        with open(os.path.normpath(os.path.join(self.directory, data_structure_name + '.txt'))) as data_structure_file:
            reader = csv.DictReader(data_structure_file, dialect='excel-tab')
            next(reader)  # skip both description lines

            for row in reader:
                for file_element in file_elements:
                    if file_element in row and row[file_element]:
                        data_structure_file_links.add(row[file_element])
                for manifest_element in manifest_elements:
                    if manifest_element in row and row[manifest_element]:
                        if row['dataset_id'] in manifest_names:
                            manifest_names[row['dataset_id']].append(row[manifest_element])
                        else:
                            manifest_names.update({row['dataset_id']: [row[manifest_element]]})
        if manifest_names:
            with open(os.path.normpath(os.path.join(self.directory, 'datastructure_manifest.txt'))) as \
                    data_structure_manifest:
                reader = csv.DictReader(data_structure_manifest, dialect='excel-tab')
                for row in reader:
                    if row['dataset_id'] in manifest_names:
                        if row['manifest_name'] in manifest_names[row['dataset_id']]:
                            data_structure_manifest_file_links.add(row['associated_file'])

        self.path_list.update(data_structure_file_links)
        self.path_list.update(data_structure_manifest_file_links)
        if len(self.path_list) > 0:
            self.get_package_file_ids()

    def get_package_file_ids(self):
        self.verbose_print('Requesting files from package: {}'.format(self.package))
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        url = self.package_url + '/{}/files'.format(self.package)
        try:
            response = requests.post(url, auth=auth, json=list(self.path_list))
            response.raise_for_status()
            for package_file in response.json():
                self.package_file_id_list.add(package_file['package_file_id'])
                self.local_file_names[package_file['package_file_id']] = package_file['download_alias']
        except HTTPError as e:
            if e.response.status_code == 404:
                message = e.response.text
                invalid_s3_links = set(map(lambda x: x.rstrip(), message.split('\n')[1:]))
                print('WARNING: The following associated files were not found in the package '
                      'and will not be downloaded\n{}'.format('\n'.join(invalid_s3_links)))
                print()
                for i in invalid_s3_links:
                    self.path_list.remove(i)

                if not self.path_list:
                    print('Error detected in package config. Please contact NDAHelp@mail.nih.gov for assistance in resolving this error.')
                    print('Please note - this error may be encountered if you did not select the "include associated files" '
                          'option when before your data package. If you wish to download associated files, you will need to create a new '
                          'package')
                    sys.exit(1)
                else:
                    self.get_package_file_ids() # retry request after excluding the invalid s3links
                    return
            else:
                raise e

        self.__chunk_file_id_list()

    def get_links(self, links, files, package, filters=None):
        self.package = package
        if links == 'datastructure':
            self.verbose_print('Downloading S3 links from a data structure (i.e., image03): {}'.format(files))
            self.dataStructure = files
            self.use_data_structure()
        elif links == 'text':
            self.verbose_print('Downloading S3 links from text file: {}'.format(files))
            self.dataStructure = files
            self.use_s3_links_file()
        elif links == 'package':
            self.verbose_print('Downloading all files from package with id: {}'.format(package))
            self.get_presigned_urls()
        else:
            self.path_list = files
            self.get_package_file_ids()

    def download_from_s3link(self, package_file_id, resume, prev_directory):

        # use this instead of exists_ok in order to work with python v.2
        def mk_dir_ignore_err(dir):
            try:
                os.makedirs(os.path.normpath(dir))
            except OSError as e:
                # Raise exception for any errors other than FileExists error
                # Using OSError over FileExistsError for version compatibility
                if e.errno != 17:
                    raise
                pass

        s3_link = self.s3_links[package_file_id]
        alias = self.local_file_names[package_file_id]
        completed_download = os.path.normpath(os.path.join(self.directory, alias))
        partial_download = os.path.normpath(
            os.path.join(prev_directory, alias + '.partial'))
        downloaded = False
        resume_header = None

        if os.path.isfile(completed_download):
            return

        if os.path.isfile(partial_download):
            downloaded = True
            downloaded_size = os.path.getsize(partial_download)
            resume_header = {'Range': 'bytes={}-'.format(downloaded_size)}
            download_file = open(partial_download, "ab")
            self.verbose_print('Resuming download: {}'.
                               format(partial_download))
        if not downloaded:
            mk_dir_ignore_err(os.path.dirname(partial_download))
            download_file = open(partial_download, "wb")
            self.verbose_print('Starting download: {}'.format(partial_download))

        try:
            s = requests.session()
            if resume_header:
                s.headers.update(resume_header)
            with s.get(s3_link, stream=True) as response:
                for chunk in response.iter_content(chunk_size=512 * 1024):
                    if chunk:
                        download_file.write(chunk)
            download_file.close()
            try:
                os.rename(partial_download, completed_download)
            except WindowsError:
                os.remove(completed_download)
                os.rename(partial_download, completed_download)
            self.verbose_print('Completed download {}'.format(completed_download))
        # except botocore.exceptions.ClientError as e:
        except Exception as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            self.package_file_download_errors.add(package_file_id)
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                message = 'This path is incorrect: {}. Please try again.'.format(s3_link)
                self.verbose_print(message)
                raise Exception(e)
            if error_code == 403:
                message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(s3_link)
                self.verbose_print(message)
                raise Exception(e)

    def start_workers(self, resume, prev_directory, thread_num=None):
        def download(package_file_id):
            self.download_from_s3link(package_file_id, resume, prev_directory)

        # Instantiate a thread pool with i worker threads
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num

        pool = ThreadPool(self.thread_num)

        # Add the jobs in bulk to the thread pool
        pool.map(download, self.package_file_id_list)
        print('Beginning download of {} files to {}'.format(len(self.package_file_id_list), self.directory))
        pool.wait_completion()
        print('Finished processing download requests for {} files. Total errors encountered: {}'
              .format(len(self.package_file_id_list), len(self.package_file_download_errors)))
