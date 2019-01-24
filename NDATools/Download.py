from __future__ import with_statement
from __future__ import absolute_import
import sys

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

from NDATools.Configuration import *
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
        self.url = self.config.datamanager_api
        self.username = config.username
        self.password = config.password
        self.directory = directory
        self.download_queue = Queue()
        self.path_list = set()
        self.access_key = None
        self.secret_key = None
        self.session = None
        self.associated_files = False
        self.dsList = []
        self.verbose = verbose

    @staticmethod
    def get_protocol(cls):
        return cls.XML

    def verbose_print(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)

    def useDataManager(self):
        """ Download package files (not associated files) """

        payload = ('<?xml version="1.0" ?>\n' +
                   '<S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">\n' +
                   '<S:Body> <ns3:QueryPackageFileElement\n' +
                   'xmlns:ns4="http://dataManagerService"\n' +
                   'xmlns:ns3="http://gov/nih/ndar/ws/datamanager/server/bean/jaxb"\n' +
                   'xmlns:ns2="http://dataManager/transfer/model">\n' +
                   '<packageId>' + self.package + '</packageId>\n' +
                   '<associated>true</associated>\n' +
                   '</ns3:QueryPackageFileElement>\n' +
                   '</S:Body>\n' +
                   '</S:Envelope>')


        response, session = api_request(self, "POST", self.url, data=payload)

        root = ET.fromstring(response.text)
        packageFiles = root.findall(".//queryPackageFiles")
        for element in packageFiles:
            associated = element.findall(".//isAssociated")
            path = element.findall(".//path")
            for a in associated:
                if a.text == 'false':
                    for p in path:
                        file = 's3:/' + p.text
                        self.path_list.add(file)

        self.verbose_print('Downloading package files for package {}.'.format(self.package))



    def searchForDataStructure(self, resume, dir):
        """ Download associated files listed in data structures """

        all_paths = self.path_list
        self.path_list = set()

        for path in all_paths:
            if 'Package_{}'.format(self.package) in path:
                file = path.split('/')[-1]
                shortName = file.split('.')[0]
                try:
                    ddr = requests.request("GET", "https://ndar.nih.gov/api/datadictionary/v2/datastructure/{}".format(
                        shortName))
                    ddr.raise_for_status()
                    dataStructureFile = path.split('gpop/')[1]
                    dataStructureFile = os.path.join(self.directory, dataStructureFile)
                    self.dataStructure = dataStructureFile
                    self.useDataStructure()
                    self.get_tokens()
                    self.start_workers(resume, prev_directory=dir)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        continue

    def useDataStructure(self):
        try:
            with open(self.dataStructure, 'r', encoding='utf-8') as tsv_file:
                tsv = csv.reader(tsv_file, delimiter="\t")
                for row in tsv:
                    for element in row:
                        if element.startswith('s3://'):
                            self.path_list.add(element)
        except IOError as e:
            self.verbose_print(
                '{} not found. Please enter the correct path to your file and try again.'.format(self.dataStructure))
            raise e
        except FileNotFoundError:
            self.verbose_print(
                '{} not found. Please enter the correct path to your file and try again.'.format(self.dataStructure))
            raise FileNotFoundError

    def get_links(self, links, files, filters=None):

        if links == 'datastructure':
            self.dataStructure = files[0]
            self.useDataStructure()

        elif links == 'text':
            self.dataStructure = files[0]
            self.useDataStructure()

        elif links == 'package':
            self.package = files[0]
            self.useDataManager()

        else:
            self.path_list = files

    def check_time(self):
        now_time = datetime.datetime.now()
        if now_time >= self.refresh_time:
            self.get_tokens()

    def get_tokens(self):
        start_time = datetime.datetime.now()
        generator = NDATokenGenerator(self.url)
        self.token = generator.generate_token(self.username, self.password)
        self.refresh_time = start_time + datetime.timedelta(hours=23, minutes=00)
        self.access_key = self.token.access_key
        self.secret_key = self.token.secret_key
        self.session = self.token.session

    def download_path(self, path, resume, prev_directory):
        filename = path.split('/')
        self.filename = filename[3:]
        key = '/'.join(self.filename)
        bucket = filename[2]
        self.newdir = filename[3:-1]
        self.newdir = '/'.join(self.newdir)
        self.newdir = os.path.join(self.directory, self.newdir)
        local_filename = os.path.join(self.directory, key)

        downloaded = False

        # check previous downloads
        if resume:
            prev_local_filename = os.path.join(prev_directory, key)
            if os.path.isfile(prev_local_filename):
                downloaded = True


        if not downloaded:
            try:
                os.makedirs(self.newdir)
            except OSError as e:
                pass

            # check tokens
            self.check_time()

            session = boto3.session.Session(self.access_key, self.secret_key, self.session)
            s3client = session.client('s3')
            s3transfer = S3Transfer(s3client)

            try:
                s3transfer.download_file(bucket, key, local_filename)
                self.verbose_print('downloaded: ', path)

            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    message = 'This path is incorrect: {}. Please try again.'.format(path)
                    self.verbose_print(message)
                    raise Exception(e)

                if error_code == 403:
                    message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(path)
                    self.verbose_print(message)
                    raise Exception(e)


    def start_workers(self, resume, prev_directory, thread_num=None):
        def download(path):
            self.download_path(path, resume, prev_directory)

        # Instantiate a thread pool with i worker threads
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num

        pool = ThreadPool(self.thread_num)

        # Add the jobs in bulk to the thread pool
        pool.map(download, self.path_list)
        pool.wait_completion()

