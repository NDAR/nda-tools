from __future__ import with_statement
from __future__ import absolute_import

import sys

from NDATools.S3Authentication import S3Authentication

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
    from io import open
else:
    from queue import Queue
import csv
from threading import Thread
import boto3
import botocore
import datetime
from boto3.s3.transfer import S3Transfer
import multiprocessing

from NDATools.TokenGenerator import *
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
        self.url = self.config.datamanager_api  #todo: delete me
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

    # def useDataManager(self):
    #     """ Download package files (not associated files) """
    #
    #     payload = ('<?xml version="1.0" ?>\n' +
    #                '<S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">\n' +
    #                '<S:Body> <ns3:QueryPackageFileElement\n' +
    #                'xmlns:ns4="http://dataManagerService"\n' +
    #                'xmlns:ns3="http://gov/nih/ndar/ws/datamanager/server/bean/jaxb"\n' +
    #                'xmlns:ns2="http://dataManager/transfer/model">\n' +
    #                '<packageId>' + self.package + '</packageId>\n' +
    #                '<associated>true</associated>\n' +
    #                '</ns3:QueryPackageFileElement>\n' +
    #                '</S:Body>\n' +
    #                '</S:Envelope>')
    #     print(payload)
    #     response, session = api_request(self, "POST", self.url, data=payload)
    #     print(response.text)
    #     root = ET.fromstring(response.text)
    #     packageFiles = root.findall(".//queryPackageFiles")
    #     for element in packageFiles:
    #         associated = element.findall(".//isAssociated")
    #         path = element.findall(".//path")
    #         alias = element.findall(".//alias")
    #         for a in associated:
    #             if a.text == 'false':
    #                 for p in path:
    #                     file = 's3:/' + p.text
    #                     print(file)
    #                     self.path_list.add(file)
    #                 for al in alias:
    #                     alias_path = al.text
    #
    #                 self.local_file_names[file] = alias_path
    #
    #     self.verbose_print('Downloading package files for package {}.'.format(self.package))

    def get_presigned_urls(self):
        """ Download package files (not associated files) """
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}
        url = self.package_url + '/{}/files?page=1&size=all'.format(self.package)
        response = json.loads(requests.get(url, headers, auth=auth).text)

        for element in response['results']:
            associated = element['associatedFile']
            if not associated:  # Does not loaded associated files
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

    # todo: not being used
    # def searchForDataStructure(self, resume, dir):
    #     """ Download associated files listed in data structures """
    #
    #     all_paths = self.path_list
    #     self.path_list = set()
    #
    #     for path in all_paths:
    #         if 'Package_{}'.format(self.package) in path:
    #             file = path.split('/')[-1]
    #             shortName = file.split('.')[0]
    #             try:
    #                 ddr = requests.request("GET", "https://nda.nih.gov/api/datadictionary/v2/datastructure/{}".format(
    #                     shortName))
    #                 ddr.raise_for_status()
    #                 dataStructureFile = path.split('gpop/')[1]
    #                 dataStructureFile = os.path.join(self.directory, dataStructureFile)
    #                 self.dataStructure = dataStructureFile
    #                 self.useDataStructure()
    #                 self.get_tokens()
    #                 self.start_workers(resume, prev_directory=dir)
    #             except requests.exceptions.HTTPError as e:
    #                 if e.response.status_code == 404:
    #                     continue

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
                            # if data_structure:
                            #     e = element.split('/')
                            #     path = "".join(e[4:])
                            #     path = "/".join([ds, path])
                            #     self.local_file_names[element] = path

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

    # def download_path(self, path, resume, prev_directory):
    #     filename = path.split('/')
    #     self.filename = filename[3:]
    #     key = '/'.join(self.filename)
    #     bucket = filename[2]
    #
    #     if self.local_file_names:
    #         dir = (self.local_file_names[path]).split('/')
    #         self.newdir = dir[:-1]
    #         alias = self.local_file_names[path]
    #     else:
    #         self.newdir = filename[3:-1]
    #         alias = key
    #     self.newdir = '/'.join(self.newdir)
    #     self.newdir = os.path.join(self.directory, self.newdir)
    #     local_filename = os.path.join(self.directory, alias)
    #     downloaded = False
    #
    #     # check previous downloads
    #     if resume:
    #         prev_local_filename = os.path.join(prev_directory, key)
    #         if os.path.isfile(prev_local_filename):
    #             downloaded = True
    #
    #     if not downloaded:
    #         try:
    #             os.makedirs(self.newdir)
    #         except OSError as e:
    #             pass
    #
    #         # check tokens
    #         self.check_time()
    #         # todo: can likely skip this step since using presigned url
    #         s3transfer = S3Transfer(S3Authentication.get_s3_client_with_config(self.access_key,
    #                                                                            self.secret_key,
    #                                                                            self.session))
    #
    #         try:
    #             s3transfer.download_file(bucket, key, local_filename) # todo: replace with GET request
    #             # GET request and download to local file path
    #
    #             self.verbose_print('downloaded: {}'.format(path))
    #
    #         except botocore.exceptions.ClientError as e:
    #             # If a client error is thrown, then check that it was a 404 error.
    #             # If it was a 404 error, then the bucket does not exist.
    #             error_code = int(e.response['Error']['Code'])
    #             if error_code == 404:
    #                 message = 'This path is incorrect: {}. Please try again.'.format(path)
    #                 self.verbose_print(message)
    #                 raise Exception(e)
    #
    #             if error_code == 403:
    #                 message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(path)
    #                 self.verbose_print(message)
    #                 raise Exception(e)

    def download_from_s3link(self, package_file_id, resume, prev_directory):

        s3_link = self.s3_links[package_file_id]
        print('s3_link: {}'.format(s3_link))

        alias = self.local_file_names[package_file_id]
        print('alias: {}'.format(alias))

        local_path = s3_link.split('?')
        local_path = ('/').join(str(local_path).split('/')[3:-1])
        self.newdir = os.path.join(self.directory, local_path)
        print('local_path: {}'.format(local_path))
        local_file = os.path.join(self.directory, local_path, alias)
        # split on '/' and then substring until alias
        print('local_file: {}'.format(local_file))

        downloaded = False

        if resume:
            prev_local_filename = os.path.join(prev_directory, alias)
            if os.path.isfile(prev_local_filename):
                downloaded = True

        if not downloaded:
            try:
                os.makedirs(self.newdir)
            except OSError as e:
                pass
        try:
            response = requests.get(s3_link)
            with open(local_file, "wb") as downloaded_file:
                downloaded_file.write(response.content)
        except botocore.exceptions.ClientError as e:
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
        # pool.map(download, self.path_list)
        pool.map(download, self.package_file_id_list)
        pool.wait_completion()
