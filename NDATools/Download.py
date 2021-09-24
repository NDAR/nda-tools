from __future__ import absolute_import
from __future__ import with_statement

import datetime
import sys

from requests import HTTPError
import threading


IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
    from io import open
    from urlparse import urlparse
else:
    from queue import Queue
    from urllib.parse import urlparse
import csv
from threading import Thread
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
        self.tasks = Queue(num_threads * 100)
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

    def __init__(self, directory, config=None, quiet=False, thread_num=None, regex_file_filter=None):
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
        self.local_file_names = {}  # map of package-file-id to alias
        self.access_key = None
        self.secret_key = None
        self.session = None
        self.associated_files = False
        self.dsList = []
        self.quiet = quiet
        self.s3_links = {}
        self.package_file_id_list = set()
        self.package_file_download_errors = set()
        # self.package_file_download_errors needs a lock if multiple threads will be adding to it simultaneously
        self.package_file_download_errors_lock = threading.Lock()

        self.download_all_files_flg = False
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

        # Instantiate a thread pool with i worker threads
        self.thread_num = thread_num if thread_num else max([1, multiprocessing.cpu_count() - 1])
        self.regex_file_filter = regex_file_filter
        if not thread_num:
            self.verbose_print()
            self.verbose_print('No value specified for --workerThreads. Using the default option of {}'.format(
                self.thread_num))
            self.verbose_print(
                'Important - You can configure the thread count setting using the --workerThreads argument to maximize your download speed.')
            self.verbose_print()

    @staticmethod
    def get_protocol(cls):
        return cls.XML

    @staticmethod
    def request_header():
        return {'content-type': 'application/json'}

    def verbose_print(self, *args):
        if not self.quiet:
            print(' '.join(list(args)))

    def get_presigned_urls(self, id_list):
        """
        Stores key-value pairs of (key: package_file_id, value: presigned URL)
        :param id_list: List of package file IDs with max size of 50,000
        """
        if len(id_list) == 1:
            file_id = id_list[0]
            url = self.package_url + '/{}/files/{}/download_url'.format(self.package, file_id)
            with requests.session() as session:
                session.mount(url, HTTPAdapter(max_retries=5))
                tmp = session.get(url, headers=self.request_header(), json=id_list, auth=self.auth)
                tmp.raise_for_status()
            response = json.loads(tmp.text)
            return response['downloadURL']
        else:
            # Use the batchGeneratePresignedUrls when retrieving multiple files
            self.verbose_print('Retrieving credentials for {} files'.format(len(id_list)))
            url = self.package_url + '/{}/files/batchGeneratePresignedUrls'.format(self.package)
            with requests.session() as session:
                session.mount(url, HTTPAdapter(max_retries=5))
                tmp = session.post(url, headers=self.request_header(), json=id_list, auth=self.auth)
                tmp.raise_for_status()
            response = json.loads(tmp.text)

            creds = {e['package_file_id']: e['downloadURL'] for e in response['presignedUrls']}
            self.verbose_print('Finished retrieving credentials')
            return creds

    def use_s3_links_file(self):
        """
        Reads a text file line by line, collects all S3 URLs, and requests the package file ids
        from the package service for those files.
        """
        path_list = set()
        try:
            with open(self.dataStructure, 'r', encoding='utf-8') as s3_links_file:
                for line in s3_links_file:
                    if line.startswith('s3://'):
                        if self.regex_file_filter and not re.search(self.regex_file_filter, line):
                            pass
                        else:
                            path_list.add(line.strip())
            if not path_list:
                print('No valid paths found in s3-links file. If you specified a regular expression, make sure your regular expression is correct before re-running the command.')
                exit_client()

            self.get_package_file_ids(path_list)
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
        url = self.package_url + '/{}/files?page=1&size=all&types=Package%20Metadata&types=Data'.format(self.package)
        tmp = requests.get(url, self.request_header(), auth=self.auth)
        tmp.raise_for_status()
        response = json.loads(tmp.text)
        has_data_structure = False
        has_data_structure_manifest = False
        for element in response['results']:
            # api response is always lower-case
            if data_structure_name.lower() == element['download_alias'].split('.txt')[0]:
                pfi = list()
                pfi.append(element['package_file_id'])
                self.local_file_names[element['package_file_id']] = element['download_alias']
                self.download_from_s3link(element['package_file_id'], True, self.directory, err_if_exists=True)
                has_data_structure = True
            if element['download_alias'] == 'datastructure_manifest.txt':
                pfi = list()
                pfi.append(element['package_file_id'])
                self.local_file_names[element['package_file_id']] = element['download_alias']
                self.download_from_s3link(element['package_file_id'], True, self.directory, err_if_exists=True)
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
        path_list = set()

        path_list.update(data_structure_file_links)
        path_list.update(data_structure_manifest_file_links)
        if self.regex_file_filter:
            path_list = set(filter(lambda x: re.search(self.regex_file_filter, x), path_list))

        if len(path_list) > 0:
            print('Getting file information for {} files in structure {}...'.format(len(path_list), data_structure_name))
            self.get_package_file_ids(path_list)
            print('Finished retreiving file information')
        else:
            print('No valid paths found in data-structure file. If you specified a regular expression, make sure your regular expression is correct before re-running the command.')
            exit_client()



    def get_package_file_ids(self, path_list):
        if not path_list:
            exit_client(signal=signal.SIGTERM, message='Illegal Argument - path_list cannot be empty')
        url = self.package_url + '/{}/files'.format(self.package)
        try:
            response = requests.post(url, auth=self.auth, json=list(path_list))
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
                    path_list.remove(i)

                if not path_list:
                    print('All files requested for download were invalid.')
                    print('This error may be encountered if you did not select the "include associated files" '
                          'option when before your data package. If you wish to download associated files, you will need to create a new '
                          'package')
                    print(
                        'If you are sure you created your package with this option selected, please contact NDAHelp@mail.nih.gov '
                        'for assistance in resolving this error.')
                    sys.exit(1)
                else:
                    self.get_package_file_ids(path_list)  # retry request after excluding the invalid s3links
                    return
            else:
                raise e

    def get_links(self, links, files, package, filters=None):
        self.package = package
        if links == 'datastructure':
            self.verbose_print('Downloading S3 links from data structure: {}'.format(files))
            self.dataStructure = files
            self.use_data_structure()  # path list to package-file-ids+file-paths to presigned-urls
        elif links == 'text':
            self.verbose_print('Downloading S3 links from text file: {}'.format(files))
            self.dataStructure = files
            self.use_s3_links_file()
        elif links == 'package':
            self.verbose_print('Downloading all files from package with id: {}'.format(package))
            self.download_all_files_flg = True
            # self.get_all_files_in_package() # package-ids+file-paths directly to presigned-urls
        else:
            self.get_package_file_ids(files)

    def download_from_s3link(self, package_file_id, resume, prev_directory, err_if_exists=False, failed_s3_links_file=None):

        # use this instead of exists_ok in order to work with python v.2
        def mk_dir_ignore_err(dir):
            try:
                os.makedirs(os.path.normpath(dir))
            except FileExistsError as e:
                pass
            except OSError as e:
                # Raise exception for any errors other than FileExists error
                # Using OSError for version compatibility
                if e.errno != 17:
                    raise
                pass

        try:
            alias = self.local_file_names[package_file_id]
            completed_download = os.path.normpath(os.path.join(self.directory, alias))
            partial_download = os.path.normpath(
                os.path.join(prev_directory, alias + '.partial'))
            downloaded = False
            resume_header = None
            bytes_written = 0

            if os.path.isfile(completed_download):
                if err_if_exists:
                    msg = "File {} already exists. Move or rename the file before re-running the command to continue".format(
                        completed_download)
                    print(msg)
                    print('Exiting...')
                    sys.exit(1)

                self.verbose_print('Skipping download (already exists): {}'.format(completed_download))
                return bytes_written

            if os.path.isfile(partial_download):
                downloaded = True
                downloaded_size = os.path.getsize(partial_download)
                resume_header = {'Range': 'bytes={}-'.format(downloaded_size)}
                self.verbose_print('Resuming download: {}'.
                                   format(partial_download))
            else:
                mk_dir_ignore_err(os.path.dirname(partial_download))
                self.verbose_print('Starting download: {}'.format(partial_download))

            s3_link = self.get_presigned_urls([package_file_id])
            s = requests.session()
            if resume_header:
                s.headers.update(resume_header)
            with open(partial_download, "ab" if downloaded else "wb") as download_file:
                with s.get(s3_link, stream=True) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=512 * 1024):
                        if chunk:
                            bytes_written += download_file.write(chunk)
            os.rename(partial_download, completed_download)
            self.verbose_print('Completed download {}'.format(completed_download))
            return bytes_written
        except Exception as e:
            if not s3_link:
                # we couldnt get credentials, which means the service has become un-responsive.
                # Instruct the user to retry at another time
                print()
                print('Unexpected Error During File Download - Service Unresponsive. Unable to obtain credentials for file-id {}'.format(package_file_id))
                print('Please re-try downloading files at a later time. ')
                print('You may contact NDAHelp@mail.nih.gov for assistance in resolving this error.')
                exit_client()

            tmp = urlparse(s3_link)

            # presigned urls are either https://bucketname.s3.amazonaws.com/key... or
            # https://s3.amazonaws.com/bucket/path
            if tmp.hostname == 's3.amazonaws.com':
                bucket = tmp.path.split('/')[1]
                path = '/'.join(tmp.path.split('/')[2:])
                path = '/' + path
            else:
                bucket = tmp.hostname.replace('.s3.amazonaws.com', '')
                path = tmp.path

            s3_address = 's3://' + bucket + path
            with self.package_file_download_errors_lock:
                self.package_file_download_errors.add(s3_address)
                if failed_s3_links_file:
                    failed_s3_links_file.write(s3_address + "\n")
                    failed_s3_links_file.flush()

            error_code = -1 if not isinstance(e, HTTPError) else int(e.response.status_code)
            if error_code == 404:
                message = 'This path is incorrect: {}. Please try again.'.format(s3_link)
                self.verbose_print(message)
            elif error_code == 403:
                message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(s3_link)
                self.verbose_print(message)
            else:
                self.verbose_print(str(e))
                self.verbose_print(get_traceback())

            if bytes_written == 0:
                try:
                    os.remove(partial_download)
                except:
                    self.verbose_print('error removing partial file {}'.format(partial_download))
            return 0

    def start_workers(self, resume, prev_directory):
        success_files = set()
        download_request_count = 0
        download_start_date = datetime.datetime.now()

        failed_s3_links_file = open('failed_s3_links_file_{}.txt'.format(time.strftime("%Y%m%dT%H%M%S")), 'a')
        message = 'Beginning download of files to {} using {} threads'.format(self.directory, self.thread_num)
        print()
        print(message)
        time.sleep(1.5)

        message = 'S3 links for files that failed to download will be written out to {}. You can attempt to download these files later by running: '\
            .format(failed_s3_links_file.name)
        message +='\n\tdownloadcmd -dp {} -t "{}"'.format(self.package, os.path.realpath(failed_s3_links_file.name))
        print(message)
        print()
        time.sleep(5)

        # These are all arrays just so that the print_download_progress_report method can update the variables inside them
        trailing_50_file_bytes = []
        trailing_50_timestamp = [datetime.datetime.now()]

        def print_download_progress_report(num_downloaded):
            self.verbose_print()
            byte_total = sum(trailing_50_file_bytes)
            download_progress_message = 'Download Progress Report [{}] - {}/{} files downloaded so far. ' \
                .format(datetime.datetime.now().strftime('%b %d %Y %H:%M:%S'), num_downloaded, download_request_count)
            download_progress_message += 'Last 50 files contained ~ {} bytes and finished in {} (Hours:Minutes:Seconds). ' \
                .format(byte_total, str(datetime.datetime.now() - trailing_50_timestamp[0]).split('.')[0])

            seconds_last_50_files = (datetime.datetime.now() - trailing_50_timestamp[0]).seconds
            if seconds_last_50_files == 0:
                seconds_last_50_files = 1  # avoid a 'division by 0' error
            download_progress_message += ' Avg download rate for the last 50 files is ~ {} bytes/sec.' \
                .format(byte_total // seconds_last_50_files)

            download_progress_message += ' Download has been in progress for {} (Hours:Minutes:Seconds).' \
                .format(str(datetime.datetime.now() - download_start_date).split('.')[0])

            self.verbose_print(download_progress_message)
            trailing_50_file_bytes.clear()
            trailing_50_timestamp[0] = datetime.datetime.now()
            self.verbose_print()

        def download(package_file_id):
            # check if  these exist, and if not, get and set:
            downloaded_bytes = self.download_from_s3link(package_file_id, resume, prev_directory, failed_s3_links_file=failed_s3_links_file)
            trailing_50_file_bytes.append(downloaded_bytes)
            success_files.add(package_file_id)
            num_downloaded = len(success_files)

            if num_downloaded % 50 == 0:
                print_download_progress_report(num_downloaded)


        pool = ThreadPool(self.thread_num)

        for package_file_id_list in self.generate_download_batch_file_ids():
            additional_file_ct = len(package_file_id_list)
            download_request_count += additional_file_ct
            self.verbose_print('Adding {} files to download queue. Queue contains {} files'.format(additional_file_ct,
                                                                                                   download_request_count))
            pool.map(download, package_file_id_list)

        pool.wait_completion()
        failed_s3_links_file.close()
        # dont generate a file if there were no failures
        if not self.package_file_download_errors:
            print('No failures detected. Removing file {}'.format(failed_s3_links_file.name))
            os.remove(failed_s3_links_file.name)

        print()

        print('Finished processing all download requests @ {}.'.format(datetime.datetime.now()))
        print('     Total download requests {}'
              .format(download_request_count))
        print('     Total errors encountered: {}'.format(len(self.package_file_download_errors)))

        print()
        print(' Exiting Program...')


    def get_package_files_by_page(self, page, batch_size):

        url = self.package_url + '/{}/files?page={}&size={}'.format(self.package, page, batch_size)
        if self.regex_file_filter:
            url += '&regex={}'.format(self.regex_file_filter)
        aliases = {}
        package_file_ids = set()
        try:
            tmp = requests.get(url, self.request_header(), auth=self.auth)
            tmp.raise_for_status()
            response = json.loads(tmp.text)

            for element in response['results']:
                associated = element['associatedFile']
                # if not associated:  # Does not loaded associated files
                alias = element['download_alias']
                package_file_id = element['package_file_id']
                package_file_ids.add(package_file_id)
                aliases[package_file_id] = alias
            return aliases, package_file_ids

        except HTTPError as e:
            if 'Cannot navigate past last page' in e.response.text:
                # empty alias and package-file return means there are no files for given page parameter
                return aliases, package_file_ids
            else:
                raise e
        except Exception as e:
            raise e

    def generate_download_batch_file_ids(self):
        batch_size = 50  # arbitrary number of files to add to job queue at once.

        if self.download_all_files_flg:
            #  write generator function that goes through each page in file listing
            #  and yields file-ids for files in package. Before returning, self.local_file_path must be set
            page = 1
            while True:
                aliases, package_file_ids = self.get_package_files_by_page(page, batch_size)
                self.local_file_names.update(aliases)
                self.package_file_id_list.update(package_file_ids)
                page += 1
                if not package_file_ids:
                    break

                yield package_file_ids

        else:
            package_file_list = list(self.package_file_id_list)
            batches = [package_file_list[i:i + batch_size] for i in range(0, len(package_file_list), batch_size)]
            for batch in batches:
                yield batch
