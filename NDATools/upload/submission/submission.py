import concurrent
import copy
import math
import traceback
from pathlib import Path
from typing import Callable, List, Tuple

from boto3.s3.transfer import TransferConfig
from s3transfer.constants import GB
from tqdm import tqdm

from NDATools.Configuration import *
from NDATools.Utils import get_s3_client_with_config, \
    collect_directory_list, deconstruct_s3_url, exit_error, execute_in_threadpool
from NDATools.upload.submission.api import SubmissionPackageApi, PackagingStatus, SubmissionApi

logger = logging.getLogger(__name__)


class SubmissionPackage:
    def __init__(self, config):
        self.config = config
        self.api = SubmissionPackageApi(self.config.submission_package_api_endpoint, self.config.username,
                                        self.config.password)

    def build_package(self, validation_uuid, collection, name, description, replacement_submission_id=None):
        package = self.api.build_package(collection, name, description, validation_uuid, replacement_submission_id)
        if package.status == PackagingStatus.PROCESSING:
            self.api.wait_package_complete(package.submission_package_uuid)
        # print package info to console
        logger.info('\n\nPackage Information:')
        logger.info('validation results: {}'.format(validation_uuid))
        logger.info('submission_package_uuid: {}'.format(package.submission_package_uuid))
        logger.info('created date: {}'.format(package.created_date))
        logger.info('expiration date: {}'.format(package.expiration_date))
        return package.submission_package_uuid


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
    def __init__(self, config):
        self.config = config
        self.directory_list = self.config.directory_list or [os.getcwd()]
        self.api = SubmissionApi(self.config.submission_api_endpoint, self.config.username, self.config.password)
        self.submission = None

    def replace_submission(self, submission_id, package_id):
        self.submission = self.api.replace_submission(submission_id, package_id)

    def submit(self, package_id):
        self.submission = self.api.create_submission(package_id)

    def resume_submission(self, submission_id):
        self.submission = self.api.get_submission(submission_id)

        if self.submission.status == Status.UPLOADING:
            self.upload_associated_files(resuming_upload=True)

    def upload_associated_files(self, resuming_upload=False):
        assert self.submission is not None, 'Must call submit/resume/replace before calling this method'

        # determine value for check_uploaded_not_complete. should be true for resume, false for submit/replace
        self._upload_associated_files_helper(check_uploaded_not_complete=resuming_upload)
        self.submission = self.api.get_submission(self.submission.submission_id)

    def _upload_associated_files_helper(self,
                                        upload_progress=None,
                                        files_to_upload_count=None,
                                        check_uploaded_not_complete=False):
        multipart_threshold = 5 * GB

        files_not_found = []
        file_not_found_count = 0
        errors_uploading_files = False

        if not files_to_upload_count:
            progress = self.api.get_upload_progress(self.submission.id)
            total_assoc_file_count, already_uploaded_count = progress.associated_file_count, progress.uploaded_file_count
            files_to_upload_count = total_assoc_file_count - already_uploaded_count
        if not upload_progress:
            upload_progress = tqdm(total=files_to_upload_count, desc=f"Submission File Upload Progress",
                                   disable=self.config.hide_progress)

        transfer_config = TransferConfig(multipart_threshold=multipart_threshold, use_threads=False)

        for batch_number, submission_file_batch in enumerate(
                self._get_files_iterator(files_to_upload_count, exclude_uploaded=True)):

            if check_uploaded_not_complete:
                submission_file_batch = self._check_uploaded_not_complete(submission_file_batch)

            # get credentials for this batch of files
            credentials_list = self.get_multipart_credentials(list([f['id'] for f in submission_file_batch]))
            id_to_credential = {int(cred['submissionFileId']): cred for cred in credentials_list}

            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.worker_threads) as executor:
                for file in submission_file_batch:
                    if not file['file_user_path'].startswith('s3://'):
                        local_file = to_local_associated_file(file, self.directory_list)
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
                                executor.submit(self._upload_associated_file, local_file, bucket, key, access_key,
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
                except Exception as e:
                    logger.error(f'Encountered exception while transferring some files to s3: {e}')
                    traceback.print_exc()
                    errors_uploading_files = True
                    pass
            if completed_files_batch:
                errors = self.batch_update_status(files=completed_files_batch)
                if errors:
                    logger.warning(
                        f'Errors found when attempting to update status for batch# {batch_number}\n{errors}')

        if files_not_found or errors_uploading_files:
            if files_not_found:

                logger.error(f'Some files could not be found in {" ".join(self.directory_list)}:\n')
                for file in files_not_found:
                    logger.error(file)
                if file_not_found_count > 20:
                    logger.error(f'and {file_not_found_count - 20} more files.')
                self.directory_list = collect_directory_list()
                self.upload_associated_files(upload_progress, files_to_upload_count, check_uploaded_not_complete)
            else:
                logger.error(f'There were errors uploading files. \r\n'
                             f'Please try resuming the submission by running vtcmd -r {self.submission_id}\r\n'
                             f'If the error persists, contact NDAHelp@mail.nih.gov for help.')
                exit_error()
        else:
            upload_progress.close()

    def _check_uploaded_not_complete(self, files):
        """This method checks whether any files in the batch were already uploaded to s3 (during previous vtcmd run) but not marked as complete. rev-1389"""
        # update payload to remove file-size
        files_no_size = [{'id': f['id']} for f in files]
        errors = self.batch_update_status(files_no_size, Status.COMPLETE)
        error_file_ids = {int(error['submissionFileId']) for error in errors}
        success_file_ids = {int(f['id']) for f in files if int(f['id']) not in error_file_ids}
        if success_file_ids:
            logger.debug(
                f'{len(success_file_ids)} files that were already uploaded have been marked as completed in NDA')
        # only return files if they appear in the list of errors here....if there was no error, then the file was
        # already uploaded to s3 and the status is now Complete in NDA
        return [f for f in files if int(f['id']) in error_file_ids]

    def _get_files_iterator(self, total_results, exclude_uploaded=False):
        last_page = math.ceil(total_results / self.config.batch_size)
        page_number = last_page - 1  # pages are 0 based
        while page_number >= 0:
            tmp = self._get_files_from_page(page_number, self.config.batch_size, exclude_uploaded)
            if not tmp:
                break
            yield tmp
            page_number -= 1

    @staticmethod
    def _upload_associated_file(local_associated_file, bucket, key, access_key, secret_key, session_token,
                                transfer_config):
        file_name = str(local_associated_file.sys_path.resolve())
        s3 = get_s3_client_with_config(access_key,
                                       secret_key,
                                       session_token)
        s3.upload_file(file_name, bucket, key, Config=transfer_config)
        return local_associated_file

    def _execute_in_threadpool(self, func: Callable, args: List[Tuple]):
        return execute_in_threadpool(func, args, max_workers=self.config.worker_threads,
                                     disable_tqdm=self.config.hide_progress)
