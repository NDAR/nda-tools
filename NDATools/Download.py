import copy
import csv
import gzip
import multiprocessing
import os.path
import pathlib
import platform
import shutil
import tempfile
import uuid
from queue import Queue
from shutil import copyfile
from threading import Thread

import pandas as pd
from boto3.s3.transfer import TransferConfig
from requests import HTTPError
from tqdm import tqdm

import NDATools
from NDATools.AltEndpointSSLAdapter import AltEndpointSSLAdapter
from NDATools.Utils import *

logger = logging.getLogger(__name__)


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    class Worker(Thread):
        """ Thread executing tasks from a given tasks queue """

        def __init__(self, tasks):
            Thread.__init__(self)
            self.tasks = tasks
            self.daemon = True
            self.start()

        def run(self):
            while True:
                func, args = self.tasks.get()
                try:
                    func(*args)
                except Exception as e:
                    # An exception happened in this thread
                    logger.info(str(e))
                    logger.info(get_traceback())
                finally:
                    # Mark this task as done, whether an exception happened or not
                    self.tasks.task_done()

    def __init__(self, num_threads, queue_size=None):
        queue_size = queue_size or num_threads * 100
        self.tasks = Queue(queue_size)
        for _ in range(num_threads):
            ThreadPool.Worker(self.tasks)

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.tasks.put((func, args))

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()


class DownloadRequest():

    def __init__(self, package_file, presigned_url, package_id, download_dir):
        operating_system = platform.system()
        if operating_system == 'Windows':
            download_dir = sanitize_windows_download_filename(download_dir)
        self.presigned_url = presigned_url
        self.package_file_id = str(package_file['package_file_id'])
        self.package_file_relative_path = package_file['download_alias']
        self.completed_download_abs_path = os.path.normpath(convert_to_abs_path(os.path.join(download_dir,
                                                                                             sanitize_windows_download_filename(
                                                                                                 self.package_file_relative_path)
                                                                                             if operating_system == 'Windows' else self.package_file_relative_path)
                                                                                ))
        self.package_download_directory = convert_to_abs_path(
            os.path.join(NDATools.NDA_TOOLS_DOWNLOADS_FOLDER, str(package_id)))
        self.nda_s3_url = None
        self.exists = False
        self.expected_file_size = package_file['file_size']
        self.actual_file_size = 0
        self.e_tag = None,
        self.download_complete_time = None
        self.partial_download_abs_path = self.completed_download_abs_path + '.partial'


class Download(Protocol):

    def __init__(self, download_config, args):

        # Instance variables from config
        self.config = download_config
        self.package_url = self.config.package_api
        self.package_creation_url = self.config.package_creation_api
        self.datadictionary_url = self.config.datadictionary_api
        self.username = download_config.username
        self.password = download_config.password
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

        # Instance Variables from 'args'
        if args.directory:
            download_directory = args.directory[0]
        else:
            download_directory = os.path.join(NDATools.NDA_TOOLS_DOWNLOADS_FOLDER, str(args.package))
        self.package_metadata_directory = os.path.join(NDATools.NDA_TOOLS_DOWNLOADS_FOLDER,
                                                       str(args.package))
        self.download_directory = convert_to_abs_path(download_directory)
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
        # self.download_queue_metadata = {}  # map of package-file-id to alias
        self.package_file_download_errors = set()
        # self.package_file_download_errors needs a lock if multiple threads will be adding to it simultaneously
        self.package_file_download_errors_lock = threading.Lock()

        self.download_job_uuid = None

        self.download_job_manifest_column_defs = {
            'uuid': self.download_job_uuid,
            'run_date': time.strftime("%Y%m%dT%H%M%S"),
            'package_id': self.package_id,
            'download_directory': self.download_directory,
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
        self.metadata_file_path = os.path.join(self.package_metadata_directory,
                                               NDATools.NDA_TOOLS_PACKAGE_FILE_METADATA_TEMPLATE % self.package_id)

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
        if self.download_directory and '--directory' not in exclude_arg_list:
            download_cmd += ' -d {}'.format(self.download_directory)
        if self.verify_flg and '--verify' not in exclude_arg_list:
            download_cmd += ' --verify'
        if self.thread_num and '--workerThreads' not in exclude_arg_list:
            download_cmd += ' -wt {}'.format(self.thread_num)
        if self.custom_user_s3_endpoint and '--s3-destination' not in exclude_arg_list:
            download_cmd += ' -s3 {}'.format(self.custom_user_s3_endpoint)

        return download_cmd

    def get_and_display_package_info(self):
        logger.info('')
        logger.info('Getting Package Information...')
        package_resource = self.get_package_info()
        logger.info('')
        logger.info('Package-id: {}'.format(self.package_id))
        logger.info('Name: {}'.format(package_resource['description']))
        logger.info('Has associated files?: {}'.format('Yes' if package_resource['has_associated_files'] else 'No'))
        # Dont print this out because at the moment, the number coming back from the service includes duplicates.
        # uncomment when that is fixed
        logger.info('Number of files in package: {}'.format(package_resource['file_count']))
        logger.info('Total Package Size: {}'.format(human_size(package_resource['total_package_size'])))
        logger.info('')
        return package_resource

    def start(self):
        package_resource = self.get_and_display_package_info()

        # self.save_package_file_metadata()
        logger.debug('downloading package metadata-file')
        self.download_package_metadata_file()
        df = []
        if self.download_mode == 'datastructure':
            logger.info('Downloading S3 links from data structure: {}'.format(self.data_structure))
            if not package_resource['has_associated_files']:
                logger.info(''''No Associated files detected in this package. In order to download associated files, you must create a new package
        on the NDA website and make sure that you check the option to "Include associated files"''')
                exit_error()
            df = self.use_data_structure()
        elif self.download_mode == 'text':
            logger.info('Downloading S3 links from text file: {}'.format(self.s3_links_file))
            df = self.use_s3_links_file()
        elif self.download_mode == 'package':
            df = self.get_all_files_in_package()
        else:
            df = self.query_files_by_s3_path(self.inline_s3_links)

        if self.regex_file_filter:
            df = df[df.download_alias.str.contains(self.regex_file_filter)]

        logger.info('')

        success_files = set()
        download_request_count = 0
        download_start_date = datetime.datetime.now()

        download_progress_report = open(self.download_progress_report_file_path, 'a', newline='')
        download_progress_report_writer = csv.DictWriter(download_progress_report,
                                                         fieldnames=self.download_job_progress_report_column_defs,
                                                         extrasaction='ignore')
        failed_s3_links_file = tempfile.NamedTemporaryFile(mode='a',
                                                           delete=False,
                                                           prefix='failed_s3_links_file_{}'.format(
                                                               time.strftime("%Y%m%dT%H%M%S")),
                                                           suffix='.csv',
                                                           dir=NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER
                                                           )

        message = 'S3 links for files that failed to download will be written out to {}. You can attempt to download these files later by running: ' \
            .format(failed_s3_links_file.name)
        message += '\n\t{} -t "{}"' \
            .format(self.build_rerun_download_cmd(['--text', '--datastructure']), failed_s3_links_file.name)
        logger.info(message)
        logger.info('')
        # time.sleep(1.5)

        tmp = df[df['download_alias'] != f'package_file_metadata_{self.package_id}.txt.gz']
        file_ct_all = tmp['download_alias'].unique().size
        file_ct_remaining = file_ct_all
        file_sz = tmp['file_size'].sum()
        tmp = {}

        # remove files that have already been completed
        completed_files = self.get_completed_files_in_download()
        tmp = {int(f['package_file_id']): int(f['actual_file_size']) for f in completed_files}

        completed_file_sz = sum(tmp.values())
        completed_file_ids = set(tmp.keys())
        completed_file_ct = len(completed_file_ids)
        completed_files = tmp = None  # remove large structures from memory
        skipping_message = ''

        if completed_file_ct > 0:
            if self.download_directory:
                skipping_message = 'Skipping {} files ({}) which have already been downloaded in {}\n'.format(
                    completed_file_ct, human_size(completed_file_sz), self.download_directory)
            else:
                download_progress_report_path = os.path.join(self.package_metadata_directory,
                                                             '.download-progress', self.download_job_uuid,
                                                             'download-progress-report.csv')
                skipping_message = 'Skipping {} files ({}) which have already been downloaded according to log file {}.\n'.format(
                    completed_file_ct, human_size(completed_file_sz), download_progress_report_path)
            file_ct_remaining = file_ct_all - completed_file_ct
            file_sz -= completed_file_sz

        if file_ct_remaining <= 0:
            if self.regex_file_filter:
                if file_ct_all > 0:
                    logger.info(
                        'All files matching the regex pattern {} have been downloaded'.format(self.regex_file_filter))
                else:
                    logger.info('No file was found that matched the regex pattern {}'.format(self.regex_file_filter))
            else:
                logger.info('All files have been downloaded')
            logger.info('')
            logger.info('Exiting Program...')
            return

        message = '{}Beginning download of {}{} files ({}){} to {} using {} threads'.format(
            skipping_message,
            'the remaining ' if skipping_message else '',
            file_ct_all,
            human_size(file_sz),
            f' matching {self.regex_file_filter}' if self.regex_file_filter else '',
            self.custom_user_s3_endpoint or self.download_directory,
            self.thread_num)

        logger.info('')
        logger.info(message)

        # These are all arrays just so that the print_download_progress_report method can update the variables inside them
        trailing_50_file_bytes = []
        trailing_50_timestamp = [datetime.datetime.now()]

        download_progress_flush_date = [datetime.datetime.now()]

        def write_to_download_progress_report_file(download_record):
            # if file-size =0, there could have been an error. Dont add to file
            newRecord = vars(download_record)
            if type(newRecord['actual_file_size']) is tuple:
                check = newRecord['actual_file_size'][0] > 0
            else:
                check = newRecord['actual_file_size'] > 0
            if check:
                download_progress_report_writer.writerow(newRecord)
                if (datetime.datetime.now() - download_progress_flush_date[0]).seconds > 10:
                    download_progress_report.flush()
                    download_progress_flush_date[0] = datetime.datetime.now()
                else:
                    pass

        def print_download_progress_report(num_downloaded):

            byte_total = sum(trailing_50_file_bytes)
            download_progress_message = 'Download Progress Report [{}]: \n    {}/{} queued files downloaded so far. ' \
                .format(datetime.datetime.now().strftime('%b %d %Y %H:%M:%S'), num_downloaded, download_request_count)
            download_progress_message += '\n    Last 50 files contained ~ {} and finished in {} (Hours:Minutes:Seconds). ' \
                .format(human_size(byte_total),
                        str(datetime.datetime.now() - trailing_50_timestamp[0]).split('.')[0])

            seconds_last_50_files = (datetime.datetime.now() - trailing_50_timestamp[0]).seconds
            if seconds_last_50_files == 0:
                seconds_last_50_files = 1  # avoid a 'division by 0' error

            # convert download speed to bits per second
            avg_speed_bps = human_size((8 * byte_total) // seconds_last_50_files)
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

        def download(package_file, temp_credentials=None):
            # check if  these exist, and if not, get and set:
            download_record = self.download_from_s3link(package_file, temp_credentials,
                                                        failed_s3_links_file=failed_s3_links_file)
            # dont add bytes if file-existed and didnt need to be downloaded
            if download_record.download_complete_time:
                trailing_50_file_bytes.append(download_record.actual_file_size)
            success_files.add(package_file['package_file_id'])
            num_downloaded = len(success_files)

            if num_downloaded % 50 == 0:
                print_download_progress_report(num_downloaded)

            download_progress_file_writer_pool.map(write_to_download_progress_report_file, [[download_record]])

        download_pool = ThreadPool(self.thread_num, self.thread_num * 6)
        download_progress_file_writer_pool = ThreadPool(1, 1000)

        for package_files in self.generate_download_batch_file_ids(completed_file_ids, df):
            if len(package_files) > 0:
                additional_file_ct = len(package_files)
                download_request_count += additional_file_ct
                logger.info('Adding {} files to download queue. Queue contains {} files\n'.format(additional_file_ct,
                                                                                                  download_request_count))
                pkfiles = {f['package_file_id']: f for f in package_files}
                if self.custom_user_s3_endpoint:
                    # we dont need presigned urls if the user is transferring to their own s3 bucket
                    file_id_to_cred_list = {f['package_file_id']: None for f in package_files}
                else:
                    file_id_to_cred_list = self.get_presigned_urls(list(pkfiles.keys()))
                download_pool.map(download, [[pkfiles[file_id], file_credentials] for file_id, file_credentials in
                                             file_id_to_cred_list.items()])

        download_pool.wait_completion()
        download_progress_file_writer_pool.wait_completion()
        failed_s3_links_file.flush()
        failed_s3_links_file.close()
        download_progress_report.flush()
        download_progress_report.close()

        # dont generate a file if there were no failures
        if not self.package_file_download_errors:
            logger.info('No failures detected. Removing file {}'.format(failed_s3_links_file.name))
            os.remove(failed_s3_links_file.name)

        logger.info('')

        logger.info('Finished processing all download requests @ {}.'.format(datetime.datetime.now()))
        logger.info('     Total download requests: {}'.format(download_request_count))

        download_error_count = len(self.package_file_download_errors)
        logger.info('     Total errors encountered: {}'.format(download_error_count))

        if download_error_count > 0:
            logger.info('     Failed to download {} files. See {} for more details'.format(download_error_count,
                                                                                           failed_s3_links_file.name))

        logger.info('')
        logger.info(' Exiting Program...')

    def download_local(self, download_request, err_if_exists=False):
        # completed_download = os.path.normpath(os.path.join(self.download_directory, download_request.package_file_relative_path))
        downloaded = False
        resume_header = None

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

        if os.path.isfile(download_request.completed_download_abs_path):
            if err_if_exists:
                msg = "File {} already exists. Move or rename the file before re-running the command to continue".format(
                    download_request.completed_download_abs_path)
                logger.info(msg)
                logger.info('Exiting...')
                sys.exit(1)

            logger.info('Skipping download (already exists): {}'.format(download_request.completed_download_abs_path))
            actual_size = os.path.getsize(download_request.completed_download_abs_path)
            download_request.actual_file_size = actual_size
            download_request.exists = True
            return download_request

        if os.path.isfile(download_request.partial_download_abs_path):
            downloaded = True
            downloaded_size = os.path.getsize(download_request.partial_download_abs_path)
            resume_header = {'Range': 'bytes={}-'.format(downloaded_size)}
            logger.info('Resuming download: {}'.
                        format(download_request.partial_download_abs_path))
        else:
            mk_dir_ignore_err(os.path.dirname(download_request.partial_download_abs_path))
            logger.info('Starting download: {}'.format(download_request.partial_download_abs_path))
            # downloading to local machine
        bytes_written = 0
        with requests.session() as s:
            s.mount(download_request.presigned_url, get_http_adapter(download_request.presigned_url))
            if resume_header:
                s.headers.update(resume_header)
            with open(download_request.partial_download_abs_path, "ab" if downloaded else "wb") as download_file:
                with s.get(download_request.presigned_url, stream=True) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):  # iterate 5MB chunks
                        if chunk:
                            bytes_written += download_file.write(chunk)
        # TODO - this doesnt work when using s3fs...add ticket to make it easy to download using s3fs
        os.rename(download_request.partial_download_abs_path, download_request.completed_download_abs_path)
        logger.info('Completed download {}'.format(download_request.completed_download_abs_path))
        download_request.actual_file_size = bytes_written
        bucket, key = deconstruct_s3_url(download_request.presigned_url)
        download_request.nda_s3_url = 's3://{}/{}'.format(bucket, key)

    def download_to_s3(self, download_request):
        # downloading directly to s3 bucket
        # get cred for file
        response = self.get_temp_creds_for_file(download_request.package_file_id, self.custom_user_s3_endpoint)
        ak = response['access_key']
        sk = response['secret_key']
        sess_token = response['session_token']
        source_uri = response['source_uri']
        dest_uri = response['destination_uri']

        download_request.nda_s3_url = source_uri

        dest_bucket, dest_path = deconstruct_s3_url(dest_uri)
        src_bucket, src_path = deconstruct_s3_url(source_uri)

        logger.info('Starting download: s3://{}/{}'.format(dest_bucket, dest_path))

        # boto3 copy
        sess = boto3.session.Session(aws_access_key_id=ak,
                                     aws_secret_access_key=sk,
                                     aws_session_token=sess_token,
                                     region_name='us-east-1')

        s3_client = sess.client('s3')
        response = s3_client.head_object(Bucket=src_bucket, Key=src_path)
        download_request.actual_file_size = response['ContentLength']
        download_request.e_tag = response['ETag'].replace('"', '')

        s3 = sess.resource('s3')
        copy_source = {
            'Bucket': src_bucket,
            'Key': src_path
        }

        def print_upload_part_info(bytes):
            logger.info('Transferred {} for {}'.format(human_size(bytes), download_request.nda_s3_url))

        KB = 1024
        MB = KB * KB
        GB = KB ** 3
        LARGE_OBJECT_THRESHOLD = 5 * GB
        args = {
            'ExtraArgs': {'ACL': 'bucket-owner-full-control'}
        }

        if int(download_request.actual_file_size) >= LARGE_OBJECT_THRESHOLD:
            logger.info('Transferring large object {} ({}) in multiple parts'
                        .format(download_request.nda_s3_url,
                                human_size(int(download_request.actual_file_size))))
            config = TransferConfig(multipart_threshold=LARGE_OBJECT_THRESHOLD, multipart_chunksize=1 * GB)
            args['Config'] = config
            args['Callback'] = print_upload_part_info

        s3.meta.client.copy(copy_source,
                            dest_bucket,
                            dest_path,
                            **args)

    def handle_download_exception(self, download_request, e, download_local, err_if_exists, package_file,
                                  failed_s3_links_file=None):
        if not download_request.presigned_url and download_local:
            # we couldnt get credentials, which means the service has become un-responsive.
            # Instruct the user to retry at another time
            logger.info('')
            logger.info(
                'Unexpected Error During File Download - Service Unresponsive. Unable to obtain credentials for file-id {}'.format(
                    download_request.package_file_id))
            logger.info('Please re-try downloading files at a later time. ')
            logger.info('You may contact NDAHelp@mail.nih.gov for assistance in resolving this error.')
            # use os._exit to kill the whole program. This works even if this is called in a child thread, unlike sys.exit()
            os._exit(1)

        self.write_to_failed_download_link_file(failed_s3_links_file, s3_link=download_request.presigned_url,
                                                source_uri=download_request.nda_s3_url)

        error_code = -1 if not isinstance(e, HTTPError) else int(e.response.status_code)
        if error_code == 404:
            message = 'This path is incorrect: {}. Please try again.'.format(download_request.presigned_url)
            logger.error(message)
        elif error_code == 403:
            # if we are using expired credentials, regenerate and resume the download
            credentials_are_expired = False
            if download_local:
                if isinstance(e, requests.exceptions.HTTPError) and 'Request has expired' in e.response.text:
                    credentials_are_expired = True

            if credentials_are_expired:
                logger.warning(
                    f'Temporary credentials have expired for file {download_request.package_file_id}. Regenerating credentials and restarting download')
                presigned_url = self.get_temp_creds_for_file(download_request.package_file_id)
                return self.download_from_s3link(package_file, presigned_url, download_local, err_if_exists,
                                                 failed_s3_links_file)
            else:
                message = '\nThis is a private bucket. Please contact NDAR for help: {}'.format(
                    download_request.presigned_url)
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
        if download_request.actual_file_size == 0 and not download_local:
            try:
                os.remove(download_request.partial_download_abs_path)
            except:
                logger.error('error removing partial file {}'.format(download_request.partial_download_abs_path))

    def download_from_s3link(self, package_file, presigned_url, download_local=None, err_if_exists=False,
                             failed_s3_links_file=None, download_dir=None):
        if download_local is None:
            download_local = False if self.custom_user_s3_endpoint else True
        if not download_dir:
            download_dir = self.download_directory

        download_request = DownloadRequest(package_file, presigned_url, self.package_id, download_dir)
        try:
            if download_local:
                self.download_local(download_request, err_if_exists)
            else:
                self.download_to_s3(download_request)
            download_request.exists = True
            download_request.download_complete_time = time.strftime("%Y%m%dT%H%M%S")
            return download_request
        except Exception as e:
            self.handle_download_exception(download_request, e, download_local, err_if_exists, package_file,
                                           failed_s3_links_file)
            return download_request

    def write_to_failed_download_link_file(self, failed_s3_links_file, s3_link, source_uri):
        src_bucket, src_path = deconstruct_s3_url(s3_link if s3_link else source_uri)
        s3_address = 's3://' + src_bucket + '/' + src_path

        with self.package_file_download_errors_lock:
            self.package_file_download_errors.add(s3_address)
            if failed_s3_links_file:
                failed_s3_links_file.write(s3_address + "\n")
                failed_s3_links_file.flush()

    def generate_download_batch_file_ids(self, completed_file_ids, df):
        batch = []
        size = 0
        for _, row in df.iterrows():
            if row['package_file_id'] not in completed_file_ids:
                batch.append(row)
                size += 1
                if size % self.default_download_batch_size == 0:
                    yield batch
                    batch = []
                    size = 0
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
                    val2 = convert_to_abs_path(val2)
                    val1 = convert_to_abs_path(val1)
                elif key == 's3_links_file':
                    # only convert to basename if values are specified for both
                    if val2 and val1:
                        val2 = os.path.basename(val2)
                        val1 = os.path.basename(val1)
                elif key == 'package_id':
                    if val2 and val1:
                        val2 = int(val2)
                        val1 = int(val1)
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

        if not os.path.exists(self.package_metadata_directory):
            os.mkdir(self.package_metadata_directory)

        DOWNLOAD_PROGRESS_FOLDER = os.path.join(self.package_metadata_directory, '.download-progress')
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
        self.get_and_display_package_info()
        self.download_package_metadata_file()

        if self.custom_user_s3_endpoint:
            raise Exception(
                'The --verify command does not yet support checking for files in s3 endpoints. This feature will be added in a future iteration...')
            exit_error()

        verification_report_path = os.path.join(self.package_metadata_directory,
                                                'download-verification-report.csv')
        err_mess_template = 'Cannot start verification process - {} already exists \nYou must move or rename the file in order to continue'
        if os.path.exists(verification_report_path):
            logger.info('')
            logger.info(err_mess_template.format(verification_report_path))
            exit_error()

        fpath = os.path.join(self.package_metadata_directory, 'download-verification-retry-s3-links.csv')
        if os.path.exists(fpath):
            logger.info(err_mess_template.format(fpath))
            exit_error()

        def get_download_progress_report_path():
            return os.path.join(self.package_metadata_directory, '.download-progress',
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
                    df = self.use_data_structure()
                elif self.download_mode == 'text':
                    df = self.use_s3_links_file()
                return df
            elif self.download_mode == 'package':
                df = self.get_all_files_in_package()
                return df
            else:
                raise Exception('Unsupported download mode: {}'.format(self.download_mode))

        def create_download_verification_retry_links_file(s3_links):
            fpath = os.path.join(self.package_metadata_directory,
                                 'download-verification-retry-s3-links.csv')
            with open(fpath, 'w') as retry_file:
                for link in s3_links:
                    retry_file.write(link + '\n')

        def add_files_to_report(download_progress_report_path, verification_report_path, probably_missing_files_list,
                                df):
            copyfile(download_progress_report_path, verification_report_path)

            all_records = []
            missing_file_records = df[df.package_file_id.isin(probably_missing_files_list)].to_dict('records')
            print('adding files to report...')
            for file_info in tqdm(missing_file_records):
                record = copy.deepcopy(self.download_job_progress_report_column_defs)
                # TODO - consider making these the same names
                record['package_file_expected_location'] = file_info['download_alias']
                record['expected_file_size'] = min(abs(int(file_info['file_size'])), 1)
                record['package_file_id'] = int(file_info['package_file_id'])
                record['nda_s3_url'] = file_info['nda_s3_url']
                if file_info['download_alias'] == (pathlib.Path(self.metadata_file_path).name + '.gz'):
                    download_path = os.path.join(self.package_metadata_directory,
                                                 file_info['download_alias'])
                else:
                    download_path = os.path.join(self.download_directory,
                                                 file_info['download_alias'])
                if os.path.exists(download_path):
                    record['exists'] = True
                    stat = os.stat(download_path)
                    record['actual_file_size'] = stat.st_size
                    if record['actual_file_size'] == record['expected_file_size']:
                        record['download_complete_time'] = time.strftime("%Y%m%dT%H%M%S", time.localtime(stat.st_ctime))

                all_records.append(record)

            with open(verification_report_path, 'a', newline='') as verification_report:
                download_progress_report_writer = csv.DictWriter(verification_report,
                                                                 fieldnames=self.download_job_progress_report_column_defs)
                download_progress_report_writer.writerows(all_records)

            return [record['nda_s3_url'] for record in all_records if
                    record['actual_file_size'] < record['expected_file_size']]

        logger.info('')
        logger.info(
            'Running verification process. This process will check whether all of the files from the following downloadcmd were successfully downloaded to the computer:')

        verification_report_path = os.path.join(self.package_metadata_directory,
                                                'download-verification-report.csv')

        logger.info('{}'.format(self.build_rerun_download_cmd(['--verify'])))
        logger.info('')
        pr_path = get_download_progress_report_path()
        logger.info('Getting expected file list for download...')
        df = get_complete_file_list()
        df = df.rename(columns={c: c.lower() for c in df.columns})
        complete_file_set = set(df['package_file_id'].values)
        # Sometimes there are dupes in the qft table. eliminate to get accurate file count

        accurate_file_ct = df['download_alias'].unique().size
        file_sz = human_size(df['file_size'].sum())

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
            .format(self.download_directory, verification_report_path))
        undownloaded_s3_links = add_files_to_report(pr_path, verification_report_path, probably_missing_files, df)
        logger.info('')
        if undownloaded_s3_links:
            logger.info(
                'Finished verification process and file check. Found {} files that were missing or whose size on disk were less than expected'.format(
                    len(undownloaded_s3_links)))
            logger.info('')
            logger.info(
                'Generating list of s3 links for all missing/incomplete files...'.format(len(undownloaded_s3_links)))
            create_download_verification_retry_links_file(undownloaded_s3_links)
            incomplete_s3_fp = os.path.join(self.package_metadata_directory,
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
        exit_normal()

    def get_temp_creds_for_file(self, package_file_id, custom_user_s3_endpoint=None):
        url = self.package_url + '/{}/files/{}/download_token'.format(self.package_id, package_file_id)
        if custom_user_s3_endpoint:
            s3_dest_bucket, s3_dest_prefix = deconstruct_s3_url(custom_user_s3_endpoint)
            url += '?s3SourceBucket={}'.format(s3_dest_bucket)
            if s3_dest_prefix:
                url += '&s3SourcePrefix={}'.format(s3_dest_prefix)
        tmp = get_request(url, auth=self.auth,
                          error_handler=HttpErrorHandlingStrategy.reraise_status,
                          deserialize_handler=DeserializeHandler.none)
        return json.loads(tmp.text)

    def generate_metadata_and_get_creds(self):
        logger.info(f'Getting list of all files in package at {time.strftime("%H:%M:%S")} ....')
        print('This is a one time operation that will take about 1 min for every 1 million files in your package.')
        print('Your download will start after this process completes.... ')
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=30)
        self.request_metadata_file_creation()
        while True:
            try:
                creds = self.get_package_file_metadata_creds()
                logger.info(f'List of files retrieved at {time.strftime("%H:%M:%S")}...')
                return creds
            except:
                time.sleep(15)
                if datetime.datetime.now() > end_time:
                    logger.error('Error during creation of package meta-data file')
                    logger.error('\nPlease contact NDAHelp@mail.nih.gov for help in resolving this error')
                    exit_error()

    def download_package_metadata_file(self):
        if os.path.exists(self.metadata_file_path):
            logger.debug('Metadata file exists')
            return
        try:
            creds = self.get_package_file_metadata_creds()
        except:
            creds = self.generate_metadata_and_get_creds()

        file_resource = self.get_package_file(creds['package_file_id'])
        self.download_from_s3link(file_resource, creds['downloadURL'], download_local=True,
                                  download_dir=self.package_metadata_directory)
        download_location = f"{self.metadata_file_path}.gz"
        outfile = download_location.rstrip('.gz')
        logger.debug(f'unzipping metadata file at {time.strftime("%H:%M:%S")}...')
        with gzip.open(download_location, 'rb') as f_in:
            with open(outfile, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return outfile

    def get_package_file_metadata_creds(self):
        url = self.package_url + \
              '/{}/files/package_file_metadata'.format(self.package_id)
        tmp = get_request(url, auth=self.auth,
                          error_handler=HttpErrorHandlingStrategy.reraise_status,
                          deserialize_handler=DeserializeHandler.convert_json)
        return tmp

    def get_package_file(self, file_id):
        url = self.package_url + \
              '/{}/files/{}'.format(self.package_id, file_id)
        tmp = get_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.convert_json)
        return tmp

    def get_files_from_datastructure(self, data_structure):
        df = pd.read_csv(self.metadata_file_path, header=0)
        df = self.rename_df_columns_to_lowercase(df)
        return df[df['short_name'] == data_structure]

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
                              error_handler=HttpErrorHandlingStrategy.reraise_status,
                              deserialize_handler=DeserializeHandler.none)
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
        logger.debug('Retrieving credentials for {} files'.format(len(id_list)))
        url = self.package_url + '/{}/files/batchGeneratePresignedUrls'.format(self.package_id)
        response = post_request(url, payload=id_list, auth=self.auth,
                                error_handler=HttpErrorHandlingStrategy.reraise_status,
                                deserialize_handler=DeserializeHandler.convert_json)
        creds = {e['package_file_id']: e['downloadURL'] for e in response['presignedUrls']}
        if not self.verify_flg:
            logger.debug('Finished retrieving credentials')
        return creds

    def get_completed_files_in_download(self):
        download_progress_report_path = os.path.join(self.package_metadata_directory,
                                                     '.download-progress', self.download_job_uuid,
                                                     'download-progress-report.csv')

        files = []
        if os.path.exists(download_progress_report_path):
            with open(download_progress_report_path, newline='') as csvfile:
                file_reader = csv.DictReader(csvfile)
                files = [f for f in file_reader if bool(f['exists'])]
        return files

    def get_all_files_in_package(self):
        df = pd.read_csv(self.metadata_file_path, header=0)
        return self.rename_df_columns_to_lowercase(df)

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
                        path_list.add(line.strip())

            if not path_list:
                logger.info(
                    'No valid paths found in s3-links file. If you specified a regular expression, make sure your regular expression is correct before re-running the command.')
                exit_error()

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
        df = self.get_files_from_datastructure(self.data_structure)
        if df.empty:
            logger.info(
                '{} data structure is not included in the package {}'.format(self.data_structure, self.package_id))
            exit_error()
        return df

    def rename_df_columns_to_lowercase(self, df):
        return df.rename(columns={c: c.lower() for c in df.columns})

    def query_files_by_s3_path(self, path_list):
        if not path_list:
            exit_error(message='Illegal Argument - path_list cannot be empty')
        df = pd.read_csv(self.metadata_file_path, header=0)
        df = self.rename_df_columns_to_lowercase(df)
        return df[df['nda_s3_url'].isin(path_list)]

    def request_metadata_file_creation(self):
        url = self.package_creation_url + \
              '/{}/create-package-metadata-file'.format(self.package_id)
        tmp = post_request(url, auth=self.auth, deserialize_handler=DeserializeHandler.none)
        return tmp
