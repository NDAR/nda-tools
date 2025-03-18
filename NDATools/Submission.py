import concurrent
import copy
import datetime
import math
import multiprocessing
import queue
from pathlib import Path

import requests.exceptions
from boto3.s3.transfer import TransferConfig
from s3transfer.constants import GB
from tqdm import tqdm

from NDATools.Configuration import *
from NDATools.Utils import deconstruct_s3_url
from NDATools.Utils import get_request, put_request, DeserializeHandler, post_request, get_s3_client_with_config, \
    collect_directory_list

logger = logging.getLogger(__name__)


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'In Progress'
    READY = 'Ready'


class LocalAssociatedFile:
    sys_path: Path
    size_bytes: int
    csv_value: str

    def __init__(self, path, size_bytes, csv_value, file_resource):
        self.sys_path = path
        self.size_bytes = size_bytes
        self.csv_value = csv_value
        self.file_resource = file_resource

    def to_api_resource(self):
        tmp = copy.deepcopy(self.file_resource)
        tmp['size'] = self.size_bytes
        return tmp


def to_local_associated_file(file_resource, directory_search_list) -> LocalAssociatedFile:
    # TODO consider adding cwd to directory_search_list by default
    csv_path = file_resource['file_user_path']
    for directory in directory_search_list:
        try:
            sys_path = Path(directory, csv_path)
            size = os.path.getsize(sys_path)
            return LocalAssociatedFile(sys_path, size, csv_path, file_resource)
        except OSError:
            pass
    logger.debug(f'file {csv_path} not found in any of the following directories: {",".join(directory_search_list)}')
    return None


class Submission:
    def __init__(self, config, submission_id=None, package_id=None, allow_exit=False, username=None, password=None,
                 thread_num=None, batch_size=None):
        assert submission_id or package_id, "Either submission-id or package-id must be specified"
        self.config = config
        self.api = self.config.submission_api
        if username:
            self.config.username = username
        if password:
            self.config.password = password
        self.username = self.config.username
        self.password = self.config.password
        self.__files = []
        self.upload_queue = queue.Queue()
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num
        self.batch_size = 10000
        if batch_size:
            self.batch_size = batch_size
        self.credentials_list = []
        self.status = None
        self.source_bucket = None
        self.upload_tries = 0
        self.max_submit_time = 120
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        self.submission_id = submission_id
        self.package_id = package_id
        self.no_read_access = set()
        self.exit = allow_exit
        self.all_mpus = []
        if not self.config.directory_list:
            self.config.directory_list = [os.getcwd()]

    def get_submission_versions(self):
        return get_request("/".join([self.api, self.submission_id, 'change-history']), auth=self.auth)

    def _replace_submission(self):
        put_request(
            "/".join([self.api, self.submission_id]) + "?submissionPackageUuid={}&async=true".format(self.package_id),
            auth=self.auth, deserialize_handler=DeserializeHandler.none)

    def replace_submission(self):
        version_count = len(self.get_submission_versions())
        self._replace_submission()

        # poll the versions endpoint until a new one is created or until we timeout
        end_time = datetime.timedelta(seconds=self.config.validation_timeout) + datetime.datetime.now()
        while True:
            if datetime.datetime.now() > end_time:
                logger.error("Timed out waiting for submission to replace.")
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
            new_version_count = len(self.get_submission_versions())
            if new_version_count > version_count:
                logger.info("Submission replaced successfully.")
                break
            time.sleep(10)

    def query_submissions_by_package_id(self, package_id):
        return get_request(f"{self.api}?packageUuid={package_id}", auth=self.auth)

    def _create_submission(self):
        post_request("/".join([self.api, self.package_id]) + "?async=true", auth=self.auth,
                     deserialize_handler=DeserializeHandler.none)

    def submit(self):
        self._create_submission()
        # poll the versions endpoint until a new one is created or until we timeout
        end_time = datetime.timedelta(seconds=self.config.validation_timeout) + datetime.datetime.now()
        while True:
            if datetime.datetime.now() > end_time:
                logger.error("Timed out waiting for submission to get created.")
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
            response = self.query_submissions_by_package_id(self.package_id)
            if response:
                logger.debug("Submission successfully created.")
                sub_id = response[0]['submission_id']
                break
            time.sleep(10)
        response = self._get_submission_by_id(sub_id)
        self.status = response['submission_status']
        self.submission_id = response['submission_id']

    def _get_submission_by_id(self, sub_id):
        return get_request("/".join([self.api, sub_id]), auth=self.auth)

    def check_status(self):
        response = get_request("/".join([self.api, self.submission_id]), auth=self.auth)
        self.status = response['submission_status']

    def get_upload_progress(self):
        response = get_request("/".join([self.api, self.submission_id, "upload-progress"]), auth=self.auth)
        return int(response['associated_file_count']), int(response['uploaded_file_count'])

    def get_multipart_credentials(self, file_ids):
        all_credentials = []
        batched_ids = [file_ids[i:i + self.batch_size] for i in range(0, len(file_ids), self.batch_size)]

        for ids in batched_ids:
            query_params = ''
            if self.config.source_bucket is not None:
                query_params = '?s3SourceBucket={}'.format(self.config.source_bucket)
                query_params += '&s3Prefix={}'.format(
                    self.config.source_prefix) if self.config.source_prefix is not None else ''
            credentials_list = post_request("/".join(
                [self.api, self.submission_id, 'files/batchMultipartUploadCredentials']) + query_params,
                                            payload=json.dumps(ids), auth=self.auth)
            all_credentials = all_credentials + credentials_list['credentials']

        return all_credentials

    def get_files_from_page(self, page_number, page_size, exclude_uploaded=False):
        excluded_q_param = f'&omitCompleted=true' if exclude_uploaded else ''
        try:
            get_files_url = "/".join([self.api, self.submission_id,
                                      f'file-listing?pageNumber={page_number}&pageSize={page_size}{excluded_q_param}'])
            response = get_request(get_files_url, auth=self.auth, error_handler=HttpErrorHandlingStrategy.ignore,
                                   deserialize_handler=DeserializeHandler.none)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 400 and 'Cannot navigate past last page' in error.response.text:
                # we got passed the last page
                return []
            else:
                logger.error(error.response.text)
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()

    def get_files_iterator(self, total_results, page_size=100_000, exclude_uploaded=False):
        last_page = math.ceil(total_results / page_size)
        page_number = last_page - 1  # pages are 0 based
        while page_number >= 0:
            tmp = self.get_files_from_page(page_number, page_size, exclude_uploaded)
            if not tmp:
                break
            yield tmp
            page_number -= 1

    def batch_update_status(self, files, status=Status.COMPLETE):
        errors = []

        def to_payload(file):
            payload = {
                "id": file['id'],
                "md5sum": "None",
                "status": status,
                "size": file['size'] if 'size' in file else 0
            }
            return payload

        list_data = list(map(to_payload, files))
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        data_to_dump = [list_data[i:i + self.batch_size] for i in range(0, len(list_data), self.batch_size)]
        for d in data_to_dump:
            data = json.dumps(d)
            response = put_request(url, payload=data, auth=self.auth)
            errors.extend(response['errors'])

        return errors

    @staticmethod
    def __upload_associated_file(local_associated_file, bucket, key, access_key, secret_key, session_token,
                                 transfer_config):
        file_name = str(local_associated_file.sys_path.resolve())
        s3 = get_s3_client_with_config(access_key,
                                       secret_key,
                                       session_token)
        s3.upload_file(file_name, bucket, key, Config=transfer_config)
        return local_associated_file

    def upload_associated_files(self, upload_progress=None, files_to_upload_count=None):
        batch_size = 50
        multipart_threshold = 5 * GB

        files_not_found = []
        file_not_found_count = 0
        errors_uploading_files = False

        if not files_to_upload_count:
            total_assoc_file_count, already_uploaded_count = self.get_upload_progress()
            files_to_upload_count = total_assoc_file_count - already_uploaded_count
        if not upload_progress:
            upload_progress = tqdm(total=files_to_upload_count, desc=f"Submission File Upload Progress",
                                   disable=self.config.hideProgress)

        transfer_config = TransferConfig(multipart_threshold=multipart_threshold, use_threads=False)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            for batch_number, submission_file_batch in enumerate(
                    self.get_files_iterator(files_to_upload_count, page_size=batch_size, exclude_uploaded=True)):
                # get credentials for this batch of files
                credentials_list = self.get_multipart_credentials(list([f['id'] for f in submission_file_batch]))
                id_to_credential = {int(cred['submissionFileId']): cred for cred in credentials_list}

                futures = []
                for file in submission_file_batch:
                    if not file['file_user_path'].startswith('s3://'):
                        local_file = to_local_associated_file(file, self.config.directory_list)
                        if not local_file:
                            if len(files_not_found) < 20:
                                files_not_found.append(file['file_user_path'])
                            file_not_found_count += 1
                            continue
                        else:
                            bucket, key = deconstruct_s3_url(file['file_remote_path'])
                            creds = id_to_credential[int(file['id'])]
                            access_key, secret_key, session_token = creds['access_key'], creds['secret_key'], creds[
                                'session_token']
                            futures.append(
                                executor.submit(self.__upload_associated_file, local_file, bucket, key, access_key,
                                                secret_key, session_token, transfer_config))
                    else:
                        raise Exception(f"Not implemented yet. File {file['file_user_path']} is in S3")
                completed_files_batch = []
                for future in concurrent.futures.as_completed(futures):
                    # noinspection PyBroadException
                    try:
                        local_associated_file = future.result()
                        completed_files_batch.append(local_associated_file.to_api_resource())
                        upload_progress.update(1)
                    except Exception:
                        errors_uploading_files = True
                        pass
                if completed_files_batch:
                    errors = self.batch_update_status(files=completed_files_batch)
                    if errors:
                        logger.warning(
                            f'Errors found when attempting to update status for batch# {batch_number}\n{errors}')

        if files_not_found or errors_uploading_files:
            if files_not_found:

                logger.error(f'Some files could not be found in {" ".join(self.config.directory_list)}:\n')
                for file in files_not_found:
                    logger.error(file)
                if file_not_found_count > 20:
                    logger.error(f'and {file_not_found_count - 20} more files.')
                self.config.directory_list = collect_directory_list()
                self.upload_associated_files(upload_progress, files_to_upload_count)
            else:
                logger.error(f'There were errors uploading files. \r\n'
                             f'Please try resuming the submission by running vtcmd -r {self.submission_id}\r\n'
                             f'If the error persists, contact NDAHelp@mail.nih.gov for help.')
                exit_error()
        else:
            upload_progress.close()

    def resume_submission(self):
        self.check_status()

        if self.status == Status.UPLOADING:
            self.upload_associated_files()
            self.check_status()
