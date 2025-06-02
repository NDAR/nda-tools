import logging
import math
import pathlib
import traceback
from typing import List

import botocore
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

from NDATools import exit_error
from NDATools.Utils import get_s3_client_with_config, deconstruct_s3_url, get_directory_input
from NDATools.upload.batch_file_uploader import BatchFileUploader, UploadContext, Uploadable, UploadError, \
    files_not_found_msg, BatchResults
from NDATools.upload.submission.api import Submission, AssociatedFile, AssociatedFileStatus, BatchUpdate, \
    AssociatedFileUploadCreds, SubmissionApi, UploadProgress

logger = logging.getLogger(__name__)


class AFUploadable(Uploadable):
    def __init__(self, file: AssociatedFile, upload_creds: AssociatedFileUploadCreds):
        super().__init__()
        self.af_file = file
        self.upload_creds = upload_creds

    @property
    def search_name(self):
        return self.af_file.file_user_path

    def __hash__(self):
        return hash(self.af_file.id)

    def __eq__(self, other):
        if isinstance(other, AFUploadable):
            return other.af_file.id == self.af_file.id
        return False


class AFUploadContext(UploadContext):
    def __init__(self, submission: Submission, resuming_upload: bool, upload_progress: UploadProgress,
                 transfer_config: TransferConfig, search_folders: List[pathlib.Path]):
        self.submission = submission
        self.resuming_upload = resuming_upload
        self.upload_progress = upload_progress
        self.transfer_config = transfer_config
        self.files_not_found = []
        self.search_folders = search_folders
        self.progress_bar = None
        # initialize this to false, and toggle to true after we prompt user to enter folder
        self.display_missing_files_message = False

    @property
    def total_files(self):
        return self.upload_progress.associated_file_count

    @property
    def remaining_file_count(self):
        return self.upload_progress.associated_file_count - self.upload_progress.uploaded_file_count


class _AssociatedBatchFileUploader(BatchFileUploader):
    def __init__(self, api, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        super().__init__(max_threads, exit_on_error, hide_progress, batch_size)
        self.api = api

    def _construct_tqdm(self):
        """Override progress bar to display total number of files and save to upload ctx"""
        progress_bar = tqdm(disable=self.hide_progress, total=self.upload_context.total_files,
                            initial=self.upload_context.upload_progress.uploaded_file_count)
        self.upload_context.progress_bar = progress_bar
        return progress_bar

    def _get_file_batches(self):
        last_page = math.ceil(self.upload_context.remaining_file_count / self.batch_size)

        page_number = last_page - 1  # pages are 0 based
        while page_number >= 0:
            submission = self.upload_context.submission
            files: List[AssociatedFile] = self.api.get_files_by_page(submission.submission_id, page_number,
                                                                     self.batch_size)
            if not files:
                break
            # hash files by id to make searching easier
            lookup = {file.id: file for file in files}
            creds: List[AssociatedFileUploadCreds] = self.api.get_upload_credentials(submission.submission_id,
                                                                                     list(lookup.keys()))
            yield [AFUploadable(lookup[c.id], c) for c in creds]
            page_number -= 1

    def _upload_file(self, up: AFUploadable):
        try:
            file_name = str(up.path.resolve())
            bucket, key = deconstruct_s3_url(up.af_file.file_remote_path)
            creds = up.upload_creds
            access_key, secret_key, session_token = creds.access_key, creds.secret_key, creds.session_token
            s3 = get_s3_client_with_config(access_key, secret_key, session_token)
            if self.upload_context.resuming_upload:
                try:
                    # REV-1389 check to see if the file has already been uploaded to s3
                    s3.head_object(Bucket=bucket, Key=key)
                except botocore.exceptions.ClientError as ce:
                    # only upload the file if it hasn't already been uploaded to s3
                    if str(ce.response['Error']['Code']) == '404':
                        s3.upload_file(file_name, bucket, key, Config=self.upload_context.transfer_config)
                    else:
                        raise UploadError(up, ce)
            else:
                s3.upload_file(file_name, bucket, key, Config=self.upload_context.transfer_config)
        except Exception as e:
            logger.error(f'Unexpected error occurred while uploading {up.search_name}: {e}')
            logger.error(traceback.format_exc())
            raise UploadError(up, e)

    def _post_batch_hook(self, batch_results: BatchResults):
        """ REST endpoint to update status of files to COMPLETE"""
        submission_id = self.upload_context.submission.submission_id
        updates = [BatchUpdate(file.af_file, AssociatedFileStatus.COMPLETE, file.calculate_size()) for file in
                   batch_results.success]
        errors = None
        if len(updates) > 0:
            errors = self.api.batch_update_associated_file_status(submission_id, updates)
            # it makes sense to show the missing files message if the program successfully processed at least 1 file
            self.upload_context.display_missing_files_message = True
        if errors:
            for error in errors:
                logger.error(f'Error updating status of file {error.search_name.file_user_path}: {error.message}')
            logger.error(f'There were errors uploading files. \r\n'
                         f'Please try resuming the submission by running vtcmd -r {submission_id}\r\n'
                         f'If the error persists, contact NDAHelp@mail.nih.gov for help.')
            exit_error()
        self.upload_context.files_not_found.extend(batch_results.files_not_found)
        self.upload_context.upload_progress.uploaded_file_count += len(updates)

    def _post_upload_hook(self):
        while self.upload_context.files_not_found:
            searched_folders = self.upload_context.search_folders
            new_dir = self._prompt_for_file_directory(searched_folders)
            progress_bar = self.upload_context.progress_bar
            # update upload_context variables
            self.upload_context.files_not_found.clear()
            self.upload_context.search_folders.clear()
            self.upload_context.search_folders.append(new_dir)
            for file_batch in self._get_file_batches():
                self._upload_batch(file_batch, self.upload_context.search_folders, lambda: progress_bar.update(1))

    def _prompt_for_file_directory(self, searched_folders: List[pathlib.Path]) -> pathlib.Path:
        # ask the user if they want to continue
        not_found: List[Uploadable] = self.upload_context.files_not_found
        msg = files_not_found_msg(not_found, searched_folders)
        if self.exit_on_error:
            exit_error(msg)
        else:
            if self.upload_context.display_missing_files_message:
                logger.info(msg)
            else:
                self.upload_context.display_missing_files_message = True
            return get_directory_input('Specify the folder containing the associated files:')


KB = 1024
GB = KB * KB * KB


class AssociatedFileUploader:

    def __init__(self, api: SubmissionApi, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        self.api = api
        self.uploader = _AssociatedBatchFileUploader(api, max_threads, exit_on_error, hide_progress, batch_size)

    def start_upload(self, submission: Submission, search_folders: List[pathlib.Path], resuming_upload: bool):
        upload_progress = self.api.get_upload_progress(submission.submission_id)
        transfer_config = TransferConfig(multipart_threshold=5 * GB, use_threads=False)
        ctx = AFUploadContext(submission, resuming_upload, upload_progress, transfer_config, search_folders)
        self.uploader.start_upload(search_folders, ctx)
