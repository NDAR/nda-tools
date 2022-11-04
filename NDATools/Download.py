from __future__ import absolute_import, with_statement

import copy
import datetime
from shutil import copyfile
import sys
import uuid

from boto3.s3.transfer import TransferConfig
from requests import HTTPError

import NDATools
from NDATools import Utils
from NDATools.AltEndpointSSLAdapter import AltEndpointSSLAdapter

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
    from io import open
    from urllib import quote_plus
else:
    from queue import Queue
    from urllib.parse import quote_plus
import csv
from threading import Thread
import multiprocessing
from NDATools.Utils import *
import requests

logger = logging.getLogger(__name__)


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
                logger.info(str(e))
                logger.info(get_traceback())

            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads, queue_size=None):
        queue_size = queue_size or num_threads * 100
        self.tasks = Queue(queue_size)
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

    def __init__(self, download_config, args):

        # Instance variables from config
        self.config = download_config
        self.package_url = self.config.package_api
        self.datadictionary_url = self.config.datadictionary_api
        self.username = download_config.username
        self.password = download_config.password
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

        # Instance Variables from 'args'
        if args.directory:
            download_directory = args.directory[0]
        else:
            download_directory = os.path.join(NDATools.NDA_TOOLS_DOWNLOADS_FOLDER, str(args.package))
        self.downloadcmd_package_metadata_directory = os.path.join(NDATools.NDA_TOOLS_DOWNLOADS_FOLDER,
                                                                   str(args.package))
        self.package_download_directory = Utils.convert_to_abs_path(download_directory)
        self.s3_links_file = args.txt
        self.inline_s3_links = args.paths
        self.package_id = args.package
        self.data_structure = args.datastructure
        self.thread_num = args.workerThreads if args.workerThreads else max([1, multiprocessing.cpu_count() - 1])
        self.regex_file_filter = args.file_regex
        if self.s3_links_file:
            self.download_mode = 'text'
        elif self.data_structure:
            self.download_mode = 'datastructure'
        elif self.inline_s3_links:
            self.download_mode = 'paths'
        else:
            self.download_mode = 'package'
        self.verify_flg = args.verify

        if not self.verify_flg and not args.workerThreads:
            logger.warning('\nNo value specified for --workerThreads. Using the default option of {}'.format(
                self.thread_num))
            logger.info(
                'Important - You can configure the thread count setting using the --workerThreads argument to maximize your download speed.\n')
        # for copying files directly to another s3 bucket
        self.custom_user_s3_endpoint = args.s3_destination

        # non-configurable default instance variables
        self.download_queue = Queue()
        self.local_file_names = {}  # map of package-file-id to alias
        self.package_file_download_errors = set()
        # self.package_file_download_errors needs a lock if multiple threads will be adding to it simultaneously
        self.package_file_download_errors_lock = threading.Lock()

        self.download_job_uuid = None

        self.download_job_manifest_column_defs = {
            'uuid': self.download_job_uuid,
            'run_date': time.strftime("%Y%m%dT%H%M%S"),
            'package_id': self.package_id,
            'download_directory': self.package_download_directory,
            's3_destination': self.custom_user_s3_endpoint,
            'data_structure': self.data_structure,
            's3_links_file': self.s3_links_file,
            'regex': self.regex_file_filter
        }
        self.download_job_progress_report_column_defs = {
            'package_file_id': None,
            'package_file_expected_location': None,
            'nda_s3_url': None,
            'exists': False,
            'expected_file_size': None,
            'actual_file_size': 0,
            'e_tag': None,
            'download_complete_time': None
        }
        self.download_progress_report_file_path = self.initialize_verification_files()
        self.default_download_batch_size = 50

    # exlcude arg list is the long-parameter name
    def build_rerun_download_cmd(self, exclude_arg_list):
        download_cmd = 'downloadcmd -dp {}'.format(self.package_id)

        if self.download_mode == 'text' and '--txt' not in exclude_arg_list:
            download_cmd += ' -t {}'.format(self.s3_links_file)
        if self.download_mode == 'datastructure' and '--datastructure' not in exclude_arg_list:
            download_cmd += ' -ds {}'.format(self.data_structure)
        if self.regex_file_filter and '--file-regex' not in exclude_arg_list:
            download_cmd += ' --file-regex {}'.format(self.regex_file_filter)
        if self.username and '--username' not in exclude_arg_list:
            download_cmd += ' -u {}'.format(self.username)
        if self.package_download_directory and '--directory' not in exclude_arg_list:
            download_cmd += ' -d {}'.format(self.package_download_directory)
        if self.verify_flg and '--verify' not in exclude_arg_list:
            download_cmd += ' --verify'
        if self.thread_num and '--workerThreads' not in exclude_arg_list:
            download_cmd += ' -wt {}'.format(self.thread_num)
        if self.custom_user_s3_endpoint and '--s3-destination' not in exclude_arg_list:
            download_cmd += ' -s3 {}'.format(self.custom_user_s3_endpoint)

        return download_cmd

    def start(self):
        logger.info('')
        logger.info('Getting Package Information...')
        package_resource = self.get_package_info()
        logger.info('')
        logger.info('Package-id: {}'.format(self.package_id))
        logger.info('Name: {}'.format(package_resource['description']))
        logger.info('Has associated files?: {}'.format('Yes' if package_resource['has_associated_files'] else 'No'))
        # Dont print this out because at the moment, the number coming back from the service includes duplicates.
        # uncomment when that is fixed
        print('Number of files in package: {}'.format(package_resource['file_count']))
        logger.info('Total Package Size: {}'.format(Utils.human_size(package_resource['total_package_size'])))
        logger.info('')

        files = []
        if self.download_mode == 'datastructure':
            logger.info('Downloading S3 links from data structure: {}'.format(self.data_structure))
            if not package_resource['has_associated_files']:
                logger.info(''''No Associated files detected in this package. In order to download associated files, you must create a new package
        on the NDA website and make sure that you check the option to "Include associated files"''')
                exit_client()
            files = self.use_data_structure()
        elif self.download_mode == 'text':
            logger.info('Downloading S3 links from text file: {}'.format(self.s3_links_file))
            files = self.use_s3_links_file()
        elif self.download_mode == 'package':
            if self.regex_file_filter:
                logger.info('Downloading files from package {} matching regex {}'.format(self.package_id, self.regex_file_filter))
            else:
                logger.info('Downloading all files from package with id: {}'.format(self.package_id))
        else:
            files = self.query_files_by_s3_path(self.inline_s3_links)

        if files:
            self.local_file_names = {int(f['package_file_id']): f for f in files}
        logger.info('')

        success_files = set()
        download_request_count = 0
        download_start_date = datetime.datetime.now()

        download_progress_report = open(self.download_progress_report_file_path, 'a', newline='')
        download_progress_report_writer = csv.DictWriter(download_progress_report,
                                                         fieldnames=self.download_job_progress_report_column_defs)

        failed_s3_links_file = open(os.path.join(NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER,
                                                 'failed_s3_links_file_{}.txt'.format(time.strftime("%Y%m%dT%H%M%S"))),
                                    'a')

        message = 'S3 links for files that failed to download will be written out to {}. You can attempt to download these files later by running: ' \
            .format(failed_s3_links_file.name)
        message += '\n\t{} -t "{}"' \
            .format(self.build_rerun_download_cmd(['--text', '--datastructure']), failed_s3_links_file.name)
        logger.info(message)
        logger.info('')
        time.sleep(1.5)

        if self.download_mode == 'package':
            file_ct = package_resource['file_count']
            file_sz = int(package_resource['total_package_size'])
        else:
            file_ct = len(self.local_file_names.keys())
            file_sz = sum(map(lambda x: x['file_size'], self.local_file_names.values()))

        #remove files that have already been completed
        completed_files = self.get_completed_files_in_download()
        tmp = {int(f['package_file_id']):int(f['actual_file_size']) for f in completed_files}
        completed_file_sz = sum(tmp.values())
        completed_file_ids = set(tmp.keys())
        completed_file_ct = len(completed_file_ids)
        completed_files = tmp = None #remove large structures from memory

        if self.download_mode == 'package' and self.regex_file_filter:
            # cant display file number because its not known
            message = 'Beginning download of files from package matching {} using {} threads'.format(
                self.regex_file_filter,
                self.thread_num)
        else:
            skipping_message = ''
            if completed_file_ct>0:
                download_progress_report_path = os.path.join(self.downloadcmd_package_metadata_directory,
                                                             '.download-progress', self.download_job_uuid,
                                                             'download-progress-report.csv')

                skipping_message = 'Skipping {} files ({}) which have already been downloaded according to log file {}.\n'.format(completed_file_ct, Utils.human_size(completed_file_sz), download_progress_report_path)
                file_ct -= completed_file_ct
                file_sz -= completed_file_sz
            message = '{}Beginning download of {}{} files ({}) to {} using {} threads'.format(
                skipping_message,
                'the remaining ' if skipping_message else '',
                file_ct,
                Utils.human_size(file_sz),
                self.custom_user_s3_endpoint or self.package_download_directory,
                self.thread_num)

        logger.info('')
        logger.info(message)
        time.sleep(5)

        # These are all arrays just so that the print_download_progress_report method can update the variables inside them
        trailing_50_file_bytes = []
        trailing_50_timestamp = [datetime.datetime.now()]


        download_progress_flush_date=[datetime.datetime.now()]
        def write_to_download_progress_report_file(download_record):
            # if file-size =0, there could have been an error. Dont add to file
            if download_record['actual_file_size'] > 0:
                download_progress_report_writer.writerow(download_record)
                if (datetime.datetime.now()-download_progress_flush_date[0]).seconds>10:
                    download_progress_report.flush()
                    download_progress_flush_date[0]=datetime.datetime.now()
                else:
                    pass

        def print_download_progress_report(num_downloaded):

            byte_total = sum(trailing_50_file_bytes)
            download_progress_message = 'Download Progress Report [{}]: \n    {}/{} queued files downloaded so far. ' \
                .format(datetime.datetime.now().strftime('%b %d %Y %H:%M:%S'), num_downloaded, download_request_count)
            download_progress_message += '\n    Last 50 files contained ~ {} bytes and finished in {} (Hours:Minutes:Seconds). ' \
                .format(Utils.human_size(byte_total),
                        str(datetime.datetime.now() - trailing_50_timestamp[0]).split('.')[0])

            seconds_last_50_files = (datetime.datetime.now() - trailing_50_timestamp[0]).seconds
            if seconds_last_50_files == 0:
                seconds_last_50_files = 1  # avoid a 'division by 0' error

            # convert download speed to bits per second
            avg_speed_bps = Utils.human_size((8 * byte_total) // seconds_last_50_files)
            if avg_speed_bps[-1:] == 'B':
                avg_speed_bps = avg_speed_bps.replace('B', 'bps')
            else:
                avg_speed_bps = avg_speed_bps.replace('bytes', 'bps')

            download_progress_message += '\n    Avg download rate (in bits per second) for the last 50 files is ~ {}.' \
                .format(avg_speed_bps)

            download_progress_message += '\n    Download has been in progress for {} (Hours:Minutes:Seconds).\n' \
                .format(str(datetime.datetime.now() - download_start_date).split('.')[0])

            download_progress_message = '\n' + download_progress_message + '\n'
            logger.info(download_progress_message)
            trailing_50_file_bytes.clear()
            trailing_50_timestamp[0] = datetime.datetime.now()

        def download(file_id_to_cred_list):
            package_file_id = file_id_to_cred_list[0]
            cred = file_id_to_cred_list[1]
            # check if  these exist, and if not, get and set:
            download_record = self.download_from_s3link(package_file_id, cred,
                                                        failed_s3_links_file=failed_s3_links_file)
            # dont add bytes if file-existed and didnt need to be downloaded
            if download_record['download_complete_time']:
                trailing_50_file_bytes.append(download_record['actual_file_size'])
            success_files.add(package_file_id)
            num_downloaded = len(success_files)

            if num_downloaded % 50 == 0:
                print_download_progress_report(num_downloaded)

            download_progress_file_writer_pool.add_task(write_to_download_progress_report_file, download_record)

        download_pool = ThreadPool(self.thread_num)
        download_progress_file_writer_pool = ThreadPool(1, 1000)

        for package_file_id_list in self.generate_download_batch_file_ids(completed_file_ids):
            if len(package_file_id_list) > 0:
                additional_file_ct = len(package_file_id_list)
                download_request_count += additional_file_ct
                logger.info('Adding {} files to download queue. Queue contains {} files\n'.format(additional_file_ct,
                                                                                                       download_request_count))
                file_id_to_cred_list = self.get_presigned_urls(list(package_file_id_list))
                download_pool.map(download, file_id_to_cred_list.items())

        download_pool.wait_completion()
        failed_s3_links_file.close()
        download_progress_report.close()

        # dont generate a file if there were no failures
        if not self.package_file_download_errors:
            logger.info('No failures detected. Removing file {}'.format(failed_s3_links_file.name))
            os.remove(failed_s3_links_file.name)

        logger.info('')

        logger.info('Finished processing all download requests @ {}.'.format(datetime.datetime.now()))
        logger.info('     Total download requests {}'
                    .format(download_request_count))
        logger.info('     Total errors encountered: {}'.format(len(self.package_file_download_errors)))

        logger.info('')
        logger.info(' Exiting Program...')

    def use_s3_links_file(self):
        """
        Reads a text file line by line, collects all S3 URLs, and requests the package file ids
        from the package service for those files.
        """
        path_list = set()
        try:
            with open(self.s3_links_file, 'r', encoding='utf-8') as s3_links_file:
                for line in s3_links_file:
                    if line.startswith('s3://'):
                        if self.regex_file_filter and not re.search(self.regex_file_filter, line):
                            pass
                        else:
                            path_list.add(line.strip())
            if not path_list:
                logger.info(
                    'No valid paths found in s3-links file. If you specified a regular expression, make sure your regular expression is correct before re-running the command.')
                exit_client()

            return self.query_files_by_s3_path(path_list)
        except IOError as e:
            logger.error(
                '{} not found. Please enter the correct path to your file and try again.'.format(self.data_structure))
            raise e

    def use_data_structure(self):
        """
        Downloads a data structure file from the package, the data_structure_manifest file,
        parses both files to identify all associated files for the data structure, and then
        requests the package file ids for those files from the package service
        """
        # TODO - add paging in case the number of files is large
        path_list = self.get_files_from_datastructure(self.data_structure)
        if not path_list:
            logger.info(
                '{} data structure is not included in the package {}'.format(self.data_structure, self.package_id))
            exit_client()
        if self.regex_file_filter:
            path_list = list(filter(lambda x: re.search(self.regex_file_filter, x['download_alias']), path_list))

        if not path_list:
            logger.info(
                'No valid paths found in data-structure file. If you specified a regular expression, make sure your regular expression is correct before re-running the command.')
            exit_client()
        return path_list

    def download_from_s3link(self, package_file_id, s3_link, err_if_exists=False, failed_s3_links_file=None):

        # check if we are downloading from alt endpoint where bucket name contains dots.
        def get_http_adapter(s3_link):
            bucket, path = deconstruct_s3_url(s3_link)
            config = {'max_retries': 10}
            if ('.' in bucket):
                return AltEndpointSSLAdapter(**config)
            return HTTPAdapter(**config)

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

        # declare to avoid 'reference before declared' error
        source_uri = None
        bytes_written = 0
        return_value = copy.deepcopy(self.download_job_progress_report_column_defs)
        return_value['package_file_id'] = str(package_file_id)
        try:
            alias = self.local_file_names[package_file_id]['download_alias']
            return_value['expected_file_size'] = self.local_file_names[package_file_id]['file_size']
            return_value['package_file_expected_location'] = alias
            completed_download = os.path.normpath(os.path.join(self.package_download_directory, alias))
            partial_download = os.path.normpath(
                os.path.join(self.package_download_directory, alias + '.partial'))
            downloaded = False
            resume_header = None

            if not self.custom_user_s3_endpoint:
                if os.path.isfile(completed_download):
                    if err_if_exists:
                        msg = "File {} already exists. Move or rename the file before re-running the command to continue".format(
                            completed_download)
                        logger.info(msg)
                        logger.info('Exiting...')
                        sys.exit(1)

                    logger.info('Skipping download (already exists): {}'.format(completed_download))
                    actual_size = os.path.getsize(completed_download)
                    return_value['actual_file_size'] = actual_size
                    return_value['exists'] = True
                    return return_value

                if os.path.isfile(partial_download):
                    downloaded = True
                    downloaded_size = os.path.getsize(partial_download)
                    resume_header = {'Range': 'bytes={}-'.format(downloaded_size)}
                    logger.info('Resuming download: {}'.
                                format(partial_download))
                else:
                    mk_dir_ignore_err(os.path.dirname(partial_download))
                    logger.info('Starting download: {}'.format(partial_download))

            if self.custom_user_s3_endpoint:
                # downloading directly to s3 bucket
                # get cred for file
                response = self.get_temp_creds_for_file(package_file_id, self.custom_user_s3_endpoint)
                ak = response['access_key']
                sk = response['secret_key']
                sess_token = response['session_token']
                source_uri = response['source_uri']
                dest_uri = response['destination_uri']

                dest_bucket, dest_path = Utils.deconstruct_s3_url(dest_uri)
                src_bucket, src_path = Utils.deconstruct_s3_url(source_uri)

                logger.info('Starting download: s3://{}/{}'.format(dest_bucket, dest_path))

                # boto3 copy
                sess = boto3.session.Session(aws_access_key_id=ak,
                                             aws_secret_access_key=sk,
                                             aws_session_token=sess_token,
                                             region_name='us-east-1')

                s3_client = sess.client('s3')
                response = s3_client.head_object(Bucket=src_bucket, Key=src_path)
                return_value['actual_file_size'] = response['ContentLength']
                return_value['e_tag'] = response['ETag'].replace('"', '')
                return_value['nda_s3_url'] = 's3://{}/{}'.format(src_bucket, src_path)

                s3 = sess.resource('s3')
                copy_source = {
                    'Bucket': src_bucket,
                    'Key': src_path
                }

                def print_upload_part_info(bytes):
                    logger.info('Transferred {} for {}'.format(Utils.human_size(bytes), return_value['nda_s3_url']))

                KB = 1024
                MB = KB * KB
                GB = KB ** 3
                LARGE_OBJECT_THRESHOLD = 5 * GB
                args = {
                    'ExtraArgs': {'ACL': 'bucket-owner-full-control'}
                }

                if int(return_value['actual_file_size']) >= LARGE_OBJECT_THRESHOLD:
                    logger.info('Transferring large object {} ({}) in multiple parts'
                                .format(return_value['nda_s3_url'],
                                        Utils.human_size(int(return_value['actual_file_size']))))
                    config = TransferConfig(multipart_threshold=LARGE_OBJECT_THRESHOLD, multipart_chunksize=1 * GB)
                    args['Config'] = config
                    args['Callback'] = print_upload_part_info

                s3.meta.client.copy(copy_source,
                                    dest_bucket,
                                    dest_path,
                                    **args)

            else:
                # downloading to local machine

                with requests.session() as s:
                    s.mount(s3_link, get_http_adapter(s3_link))
                    if resume_header:
                        s.headers.update(resume_header)
                    with open(partial_download, "ab" if downloaded else "wb") as download_file:
                        with s.get(s3_link, stream=True) as response:
                            response.raise_for_status()
                            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):  # iterate 5MB chunks
                                if chunk:
                                    bytes_written += download_file.write(chunk)
                os.rename(partial_download, completed_download)
                logger.info('Completed download {}'.format(completed_download))
                return_value['actual_file_size'] = bytes_written
                bucket, key = Utils.deconstruct_s3_url(s3_link)
                return_value['nda_s3_url'] = 's3://{}/{}'.format(bucket, key)
            return_value['exists'] = True
            return_value['download_complete_time'] = time.strftime("%Y%m%dT%H%M%S")
            return return_value

        except Exception as e:
            if not s3_link and not source_uri:
                # we couldnt get credentials, which means the service has become un-responsive.
                # Instruct the user to retry at another time
                logger.info('')
                logger.info(
                    'Unexpected Error During File Download - Service Unresponsive. Unable to obtain credentials for file-id {}'.format(
                        package_file_id))
                logger.info('Please re-try downloading files at a later time. ')
                logger.info('You may contact NDAHelp@mail.nih.gov for assistance in resolving this error.')
                # use os._exit to kill the whole program. This works even if this is called in a child thread, unlike sys.exit()
                os._exit(1)

            self.write_to_failed_download_link_file(failed_s3_links_file, s3_link=s3_link, source_uri=source_uri)

            error_code = -1 if not isinstance(e, HTTPError) else int(e.response.status_code)
            if error_code == 404:
                message = 'This path is incorrect: {}. Please try again.'.format(s3_link)
                logger.error(message)
            elif error_code == 403:
                message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(s3_link)
                logger.error(message)
            else:
                logger.error(str(e))
                logger.error(get_traceback())
                if 'operation: Access Denied' in str(e):
                    logger.error('')
                    logger.error(
                        'This error is likely caused by a misconfiguration on the target s3 bucket')
                    logger.error(
                        "For more information about how to correctly configure the target bucket, run 'downloadcmd -h' and read the description of the s3 argument")
                    logger.error('')
                    time.sleep(2)

            # if source_uri is set, it means they're downloading to s3 bucket and there will not be any partial file
            if bytes_written == 0 and not source_uri:
                try:
                    os.remove(partial_download)
                except:
                    logger.error('error removing partial file {}'.format(partial_download))
            return return_value

    def write_to_failed_download_link_file(self, failed_s3_links_file, s3_link, source_uri):
        src_bucket, src_path = Utils.deconstruct_s3_url(s3_link if s3_link else source_uri)
        s3_address = 's3://' + src_bucket + '/' + src_path

        with self.package_file_download_errors_lock:
            self.package_file_download_errors.add(s3_address)
            if failed_s3_links_file:
                failed_s3_links_file.write(s3_address + "\n")
                failed_s3_links_file.flush()

    def generate_download_batch_file_ids(self, completed_file_ids):
        if self.download_mode == 'package':
            already_downloaded = len(completed_file_ids)

            #  write generator function that goes through each page in file listing
            #  and yields file-ids for files in package. Before returning, self.local_file_path must be set
            page = (already_downloaded // self.default_download_batch_size) + 1
            while True:
                logger.debug(f'getting package files for package {self.package_id} and page {page} at time {datetime.datetime.now()}')
                files = self.get_package_files_by_page(page, self.default_download_batch_size)
                logger.debug(f'finished getting package files for package {self.package_id} and page {page} at time {datetime.datetime.now()}')
                tmp = {r['package_file_id']: r for r in files if int(r['package_file_id']) not in completed_file_ids}
                self.local_file_names.update(tmp)
                package_file_ids = tmp.keys()
                page += 1
                if not package_file_ids:
                    break
                yield package_file_ids

        else:

            package_file_list = [f for f in {k:v for k,v in self.local_file_names.items() if int(k) not in completed_file_ids}]
            batches = [package_file_list[i:i + self.default_download_batch_size] for i in range(0, len(package_file_list), self.default_download_batch_size)]
            for batch in batches:
                yield batch

    def find_matching_download_job(self, download_job_manifest_path):
        def is_job_match(possible_match):
            must_match = [
                'data_structure',
                'download_directory',
                's3_destination',
                's3_links_file',
                'package_id',
                'regex'
            ]

            def test_match(key):
                # None gets converted to empty string.
                # Convert it back if empty string is detected so that None values can be compared using == operator
                val1 = possible_match[key] or None
                # values from download_job_manifest_column_defs will never be None, instead they will be an empty string ''
                val2 = self.download_job_manifest_column_defs[key]
                if key == 'download_directory':
                    val2 = Utils.convert_to_abs_path(val2)
                    val1 = Utils.convert_to_abs_path(val1)
                elif key == 's3_links_file':
                    # only convert to basename if values are specified for both
                    if val2 and val1:
                        val2 = os.path.basename(val2)
                        val1 = os.path.basename(val1)
                return val1 == val2

            return all(map(test_match, must_match))

        # FIND MATCHING JOB RECORD IF IT EXISTS
        with open(download_job_manifest_path, newline='') as csvfile:
            job_reader = csv.DictReader(csvfile)
            for job in job_reader:
                if is_job_match(job):
                    return job
        return None

    def initialize_verification_files(self):

        download_job_manifest_columns = self.download_job_manifest_column_defs.keys()

        def initialize_download_progress_file(fp):
            with open(fp, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=self.download_job_progress_report_column_defs)
                writer.writeheader()

        def add_entry_to_job_manifest(fp):
            self.download_job_uuid = self.download_job_manifest_column_defs['uuid'] = str(uuid.uuid4())
            with open(fp, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=download_job_manifest_columns)
                writer.writerow(self.download_job_manifest_column_defs)

        def initialize_job_manifest_file(fp):
            with open(fp, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=download_job_manifest_columns)
                writer.writeheader()

        if not os.path.exists(self.downloadcmd_package_metadata_directory):
            os.mkdir(self.downloadcmd_package_metadata_directory)

        DOWNLOAD_PROGRESS_FOLDER = os.path.join(self.downloadcmd_package_metadata_directory, '.download-progress')
        if not os.path.exists(DOWNLOAD_PROGRESS_FOLDER):
            os.mkdir(DOWNLOAD_PROGRESS_FOLDER)

        download_job_manifest_path = os.path.join(DOWNLOAD_PROGRESS_FOLDER, 'download-job-manifest.csv')
        if not os.path.exists(download_job_manifest_path):
            initialize_job_manifest_file(download_job_manifest_path)

        job_record = self.find_matching_download_job(download_job_manifest_path)
        if job_record is not None:
            self.download_job_uuid = job_record['uuid']

        if not self.download_job_uuid:
            add_entry_to_job_manifest(download_job_manifest_path)

        DOWNLOAD_JOB_UUID_DIR = os.path.join(DOWNLOAD_PROGRESS_FOLDER, str(self.download_job_uuid))
        if not os.path.exists(DOWNLOAD_JOB_UUID_DIR):
            os.mkdir(DOWNLOAD_JOB_UUID_DIR)

        download_progress_report_file = os.path.join(DOWNLOAD_JOB_UUID_DIR, 'download-progress-report.csv')
        if not os.path.exists(download_progress_report_file):
            initialize_download_progress_file(download_progress_report_file)
        return download_progress_report_file

    '''        
            Steps -
            1. Find path of correct download-progress-report file from .download-progress/download-job-manifest.csv (if it exists),
                for the given target directory and download mode
            2. Make a copy of the download-progress-report and save into verification folder. Name it 'download-verification-report.csv'
            3. Consider everything currently in the download-verification-report.csv where expected file-size=actual file-size as being downloaded
                     a. there really shouldnt be any entries where the expected filesize doesnt match actual size , but run the code anyway
            4. Read file-names of download-verification-report.csv into set() in memory
            *5. Get the complete file-listing for the download (using the provided arguments -d, -ds, -t and -dp)
            6. Add anything that is not in the set in step 4 into the download-verification-report.csv
            7. Run os.path.exists() on each added entry - if it exists, add file-size info to download-verification-report.csv. if not, set
            exists columns and file-size columns
            8. Run through the file download-verification-report.csv and collect everything where the actual and expected file-sizes dont match up.
            9. Create the download-verification-retry-s3-links.csv file from the entries found in step 8
    '''

    def verify_download(self):

        if self.custom_user_s3_endpoint:
            raise Exception(
                'The --verify command does not yet support checking for files in s3 endpoints. This feature will be added in a future iteration...')
            exit_client()

        verification_report_path = os.path.join(self.downloadcmd_package_metadata_directory,
                                                'download-verification-report.csv')
        err_mess_template = 'Cannot start verification process - {} already exists \nYou must move or rename the file in order to continue'
        if os.path.exists(verification_report_path):
            logger.info('')
            logger.info(err_mess_template.format(verification_report_path))
            exit_client()

        fpath = os.path.join(self.downloadcmd_package_metadata_directory, 'download-verification-retry-s3-links.csv')
        if os.path.exists(fpath):
            logger.info(err_mess_template.format(fpath))
            exit_client()

        def get_download_progress_report_path():
            return os.path.join(self.downloadcmd_package_metadata_directory, '.download-progress',
                                self.download_job_uuid, 'download-progress-report.csv')

        def parse_download_progress_report_for_files(download_progress_report_path):
            files = []
            if os.path.exists(download_progress_report_path):
                with open(download_progress_report_path, newline='') as csvfile:
                    file_reader = csv.DictReader(csvfile)
                    files = [f for f in file_reader]
            return files

        def get_complete_file_list():
            if self.download_mode in ['text', 'datastructure']:
                if self.download_mode == 'datastructure':
                    files = self.use_data_structure()
                elif self.download_mode == 'text':
                    files = self.use_s3_links_file()
                self.local_file_names = {int(f['package_file_id']): f for f in files}
                return set(self.local_file_names)
            elif self.download_mode == 'package':
                logger.info('Getting list of all files in package. If your package is large, this may take some time')
                page = 1
                batch_size = 1000
                all_results = []
                while True:
                    results = self.get_package_files_by_page(page, batch_size)
                    aliases = {r['package_file_id']: r for r in results}
                    self.local_file_names.update(aliases)
                    if not results:
                        break
                    else:
                        all_results.append(results)
                        logger.info('Retrieved {} 1000 files. At file #{}'.format('first' if page == 1 else 'next',
                                                                                  ((page - 1) * batch_size) + 1))
                    page += 1

                return set(self.local_file_names)
            else:
                raise Exception('Unsupported download mode: {}'.format(self.download_mode))

        def create_download_verification_retry_links_file(s3_links):
            fpath = os.path.join(self.downloadcmd_package_metadata_directory,
                                 'download-verification-retry-s3-links.csv')
            with open(fpath, 'w') as retry_file:
                for link in s3_links:
                    retry_file.write(link + '\n')

        def add_files_to_report(download_progress_report_path, verification_report_path, probably_missing_files_list):
            copyfile(download_progress_report_path, verification_report_path)

            all_records = []
            for file_id in probably_missing_files_list:
                file_info = self.local_file_names[file_id]
                record = copy.deepcopy(self.download_job_progress_report_column_defs)
                # TODO - consider making these the same names
                record['package_file_expected_location'] = file_info['download_alias']
                record['expected_file_size'] = file_info['file_size']
                record['package_file_id'] = file_id
                download_path = os.path.join(self.package_download_directory,
                                             self.local_file_names[file_id]['download_alias'])
                if os.path.exists(download_path):
                    record['exists'] = True
                    stat = os.stat(download_path)
                    record['actual_file_size'] = stat.st_size
                    if record['actual_file_size'] == record['expected_file_size']:
                        record['download_complete_time'] = time.strftime("%Y%m%dT%H%M%S", time.localtime(stat.st_ctime))

                all_records.append(record)

            # batch requests to reduce overhead
            # TODO - add s3 location to file resource in order to eliminate this step?
            logger.info('Retrieving s3 url information for {} nda file records'.format(len(all_records)))
            batch_size = 1000
            batches = [all_records[i:i + batch_size] for i in range(0, len(all_records), batch_size)]
            for batch in batches:
                result = self.get_presigned_urls([record['package_file_id'] for record in batch])
                # result is a dictionary of package-file-id to presignedUrl
                for record in batch:
                    ps_url = result[record['package_file_id']]
                    dest_bucket, dest_path = Utils.deconstruct_s3_url(ps_url)
                    record['nda_s3_url'] = 's3://{}/{}'.format(dest_bucket, dest_path)

            with open(verification_report_path, 'a', newline='') as verification_report:
                download_progress_report_writer = csv.DictWriter(verification_report,
                                                                 fieldnames=self.download_job_progress_report_column_defs)
                download_progress_report_writer.writerows(all_records)

            return [record['nda_s3_url'] for record in all_records if
                    record['actual_file_size'] < record['expected_file_size']]

        logger.info('')
        logger.info(
            'Running verification process. This process will check whether all of the files from the following downloadcmd were successfully downloaded to the computer:')

        verification_report_path = os.path.join(self.downloadcmd_package_metadata_directory,
                                                'download-verification-report.csv')

        logger.info('{}'.format(self.build_rerun_download_cmd(['--verify'])))
        logger.info('')
        pr_path = get_download_progress_report_path()
        logger.info('Getting expected file list for download...')
        complete_file_set = get_complete_file_list()

        # Sometimes there are dupes in the qft table. eliminate to get accurate file count
        accurate_file_ct = len(set(map(lambda x: x['download_alias'], self.local_file_names.values())))
        file_sz = Utils.human_size(sum(map(lambda x: x['file_size'], self.local_file_names.values())))

        logger.info(
            '{} files are expected to have been downloaded from the command above, totaling {}'.format(accurate_file_ct,
                                                                                                       file_sz))
        logger.info('')
        logger.info('Parsing program system logs for history of completed downloads...')
        logger.info(
            'Important - if you think files may have been deleted from your system after the download was run, you should remove the system log at {}'
            ' and re-run the --verify command. This will force the program to check for these files instead of assuming they exist based on system log entries. This will cause the --verify step to take longer'
            ' to finish but will be necessary for accurate results.'.format(pr_path))
        downloaded_file_records = parse_download_progress_report_for_files(pr_path)
        # There shouldn't be duplicates in the system logs, but check anyway
        downloaded_file_records_count = len({f['package_file_expected_location'] for f in downloaded_file_records})
        downloaded_file_set = {int(f['package_file_id']) for f in downloaded_file_records}
        logger.info('')
        logger.info(
            'Found {} complete file downloads according to log file {}'.format(downloaded_file_records_count, pr_path))
        logger.info('')
        probably_missing_files = complete_file_set - downloaded_file_set
        logger.info(
            'Checking {} for all files which were not found in the program system logs. Detailed report will be created at {}...'
            .format(self.package_download_directory, verification_report_path))
        undownloaded_s3_links = add_files_to_report(pr_path, verification_report_path, probably_missing_files)
        logger.info('')
        if undownloaded_s3_links:
            logger.info(
                'Finished verification process and file check. Found {} files that were missing or whose size on disk were less than expected'.format(
                    len(undownloaded_s3_links)))
            logger.info('')
            logger.info(
                'Generating list of s3 links for all missing/incomplete files...'.format(len(undownloaded_s3_links)))
            create_download_verification_retry_links_file(undownloaded_s3_links)
            incomplete_s3_fp = os.path.join(self.downloadcmd_package_metadata_directory,
                                            'download-verification-retry-s3-links.csv')
            logger.info(
                'Finished creating {} file. \nThis file contains s3-links for all files that were found to be missing or incomplete. You may '
                'download these files by running:\n'
                '   {} -t {}'.format(incomplete_s3_fp,
                                     self.build_rerun_download_cmd(['--verify', '--text', '--datastructure']),
                                     incomplete_s3_fp))
        else:
            logger.info(
                'Finished verification process and file check. No missing files found. All files match expected size. Download 100% complete.')

        logger.info('')
        logger.info(
            'Details about status of files in download can be found at {} (This file can be opened with Excel or Google Spreadsheets)'.format(
                verification_report_path))
        exit_client()

        # Step 5 should be made into a modular function because creating a file listing per download is something the user might be interested in having
        # potential future feature of the tool.

        # Note 1 - when implementing this for s3, we will need to add instructions to provide the list-bucket permission to user
        # Note 2 - eventually, we might be able to add the e-tag data-attribute and start verifying download content by comparing the e-tag attribute

    def query_files_by_s3_path(self, path_list):
        if not path_list:
            exit_client(signal=signal.SIGTERM, message='Illegal Argument - path_list cannot be empty')
        url = self.package_url + '/{}/files'.format(self.package_id)
        try:
            response = post_request(url, list(path_list), auth=self.auth,
                                    error_handler=HttpErrorHandlingStrategy.reraise_status,
                                    deserialize_handler=DeserializeHandler.none)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            if e.response.status_code == 404:
                message = e.response.text
                invalid_s3_links = set(map(lambda x: x.rstrip(), message.split('\n')[1:]))
                logger.info('WARNING: The following associated files were not found in the package '
                            'and will not be downloaded\n{}'.format('\n'.join(invalid_s3_links)))
                logger.info('')
                for i in invalid_s3_links:
                    path_list.remove(i)

                if not path_list:
                    logger.info('All files requested for download were invalid.')
                    logger.info('This error may be encountered if you did not select the "include associated files" '
                                'option when before your data package. If you wish to download associated files, you will need to create a new '
                                'package')
                    logger.info(
                        'If you are sure you created your package with this option selected, please contact NDAHelp@mail.nih.gov '
                        'for assistance in resolving this error.')
                    sys.exit(1)
                else:
                    return self.query_files_by_s3_path(path_list)  # retry request after excluding the invalid s3links
            else:
                HttpErrorHandlingStrategy.print_and_exit(e.response)

    def get_temp_creds_for_file(self, package_file_id, custom_user_s3_endpoint=None):
        url = self.package_url + '/{}/files/{}/download_token'.format(self.package_id, package_file_id)
        if custom_user_s3_endpoint:
            s3_dest_bucket, s3_dest_prefix = Utils.deconstruct_s3_url(custom_user_s3_endpoint)
            url += '?s3SourceBucket={}'.format(s3_dest_bucket)
            if s3_dest_prefix:
                url += '&s3SourcePrefix={}'.format(s3_dest_prefix)
        tmp = get_request(url, auth=self.auth,
                          error_handler=HttpErrorHandlingStrategy.reraise_status,
                          deserialize_handler=DeserializeHandler.none)
        return json.loads(tmp.text)

    def get_files_from_datastructure(self, data_structure):
        data_structure_regex = quote_plus('{}/.*'.format(data_structure))
        url = self.package_url + \
              '/{}/files?page=1&size=all&regex={}'.format(self.package_id, data_structure_regex)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        results = json.loads(tmp.text)['results']
        return results

    def get_data_structure_manifest_file_info(self):
        url = self.package_url + \
              '/{}/files?page=1&size=all&types=Package%20Metadata&regex={}'.format(self.package_id,
                                                                                   'datastructure_manifest.txt')
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        results = json.loads(tmp.text)['results']
        # return None instead of empty list, since this method is always supposed to return 1 thing
        return results[0] if results else None

    def get_data_structure_files(self):
        url = self.package_url + \
              '/{}/files?page=1&size=all&types=Data'.format(self.package_id)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        tmp.raise_for_status()
        results = json.loads(tmp.text)['results']
        return [r for r in results if r['nda_file_type'] == 'Data']

    def get_data_structure_file_info(self, short_name):
        url = self.package_url + \
              '/{}/files?page=1&size=all&types=Package%20Metadata&types=Data&regex={}'.format(self.package_id,
                                                                                              short_name)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        tmp.raise_for_status()
        results = json.loads(tmp.text)['results']
        # return None instead of empty list, since this method is always supposed to return 1 thing
        return results[0] if results else None

    def get_package_file_info(self, file_id):
        url = self.package_url + '/{}/files/{}'.format(self.package_id, file_id)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        return json.loads(tmp.text)

    def get_package_info(self):
        url = self.package_url + '/{}'.format(self.package_id)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        return json.loads(tmp.text)

    def get_package_files_by_page(self, page, batch_size):
        url = self.package_url + '/{}/files?page={}&size={}'.format(self.package_id, page, batch_size)
        if self.regex_file_filter:
            url += '&regex={}'.format(self.regex_file_filter)
        try:
            tmp = get_request(url, auth=self.auth,
                              error_handler=HttpErrorHandlingStrategy.reraise_status, deserialize_handler=DeserializeHandler.none)
            tmp.raise_for_status()
            response = json.loads(tmp.text)
            return response['results']

        except HTTPError as e:
            if 'Cannot navigate past last page' in e.response.text:
                # empty alias and package-file return means there are no files for given page parameter
                return []
            else:
                raise e
        except Exception as e:
            raise e

    def get_presigned_urls(self, id_list):
        """
        Stores key-value pairs of (key: package_file_id, value: presigned URL)
        :param id_list: List of package file IDs with max size of 50,000
        """
        # Use the batchGeneratePresignedUrls when retrieving multiple files
        if not self.verify_flg:
            logger.debug('Retrieving credentials for {} files'.format(len(id_list)))
        url = self.package_url + '/{}/files/batchGeneratePresignedUrls'.format(self.package_id)
        tmp = post_request(url,  payload=id_list, auth=self.auth,
                           error_handler=HttpErrorHandlingStrategy.reraise_status, deserialize_handler=DeserializeHandler.none)
        response = json.loads(tmp.text)
        creds = {e['package_file_id']: e['downloadURL'] for e in response['presignedUrls']}
        if not self.verify_flg:
            logger.debug('Finished retrieving credentials')
        return creds

    def get_completed_files_in_download(self):
        download_progress_report_path = os.path.join(self.downloadcmd_package_metadata_directory,
                                                     '.download-progress', self.download_job_uuid,
                                                     'download-progress-report.csv')

        files = []
        if os.path.exists(download_progress_report_path):
            with open(download_progress_report_path, newline='') as csvfile:
                file_reader = csv.DictReader(csvfile)
                files = [f for f in file_reader if bool(f['exists'])]
        return files