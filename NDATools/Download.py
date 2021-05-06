from __future__ import absolute_import
from __future__ import with_statement

import sys

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
        response = json.loads(requests.get(url, headers, auth=auth).text)

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
        response = json.loads(requests.post(url, headers=headers, json=id_list, auth=auth).text)

        for element in response['presignedUrls']:
            download_url = element['downloadURL']
            package_file_id = element['package_file_id']
            self.s3_links[package_file_id] = download_url

    def useDataStructure(self, data_structure):
        try:
            with open(self.dataStructure, 'r', encoding='utf-8') as tsv_file:
                tsv = csv.reader(tsv_file, delimiter="\t")
                if data_structure:
                    ds = next(tsv)[1].split("_")[0]
                for row in tsv:
                    for element in row:
                        if element.startswith('s3://'):
                            self.path_list.add(element)  # todo: replace with package_file_id_list
                self.get_package_file_ids()

        except IOError as e:
            self.verbose_print(
                '{} not found. Please enter the correct path to your file and try again.'.format(self.dataStructure))
            raise e

    def get_package_file_ids(self):
        print(self.path_list)
        print(self.package)
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}
        url = self.package_url + '/{}/files?page=1&size=all'.format(self.package)
        response = json.loads(requests.get(url, headers, auth=auth).text)
        print(response)
        for path in self.path_list:
            for element in response['results']:
                if element['s3_path'] == path:
                    alias = element['download_alias']
                    package_file_id = element['package_file_id']
                    self.package_file_id_list.add(package_file_id)
                    self.local_file_names[package_file_id] = alias
                    break
            else:
                print('{} not found in package'.format(path))

        self.__chunk_file_id_list()

    def get_links(self, links, files, package, filters=None):
        self.package = package
        print(files)
        if links == 'datastructure':
            self.dataStructure = files
            self.useDataStructure(data_structure=True)
        elif links == 'text':
            self.dataStructure = files
            self.useDataStructure(data_structure=False)
        elif links == 'package':
            self.get_presigned_urls()
        else:
            self.path_list = files
            self.get_package_file_ids()

    def download_from_s3link(self, package_file_id, resume, prev_directory):

        # use this instead of exists_ok in order to work with python v.2
        def mk_dir_ignore_err(dir):
            try:
                os.makedirs(os.path.normpath(dir))
            except FileExistsError as e:
                pass

        s3_link = self.s3_links[package_file_id]
        print('s3_link: {}'.format(s3_link))

        alias = self.local_file_names[package_file_id]
        print('alias: {}'.format(alias))

        local_file = os.path.normpath(os.path.join(self.directory, alias))
        # split on '/' and then substring until alias
        print('local_file: {}'.format(local_file))

        downloaded = False

        if resume:
            prev_local_filename = os.path.normpath(os.path.join(prev_directory, alias))
            if os.path.isfile(prev_local_filename):
                downloaded = True

        if not downloaded:
            mk_dir_ignore_err(os.path.dirname(local_file))

            try:
                with requests.get(s3_link, stream=True) as response:
                    with open(local_file, "wb") as downloaded_file:
                        for chunk in response.iter_content(chunk_size=512 * 1024):
                            if chunk:
                                downloaded_file.write(chunk)
            # except botocore.exceptions.ClientError as e:
            except Exception as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
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
        def download(path):
            self.download_from_s3link(path, resume, prev_directory)

        # Instantiate a thread pool with i worker threads
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num

        pool = ThreadPool(self.thread_num)

        # Add the jobs in bulk to the thread pool
        pool.map(download, self.package_file_id_list)
        pool.wait_completion()
