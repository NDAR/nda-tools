import logging
import os
import pathlib
import sqlite3
import traceback
from typing import List, Optional

import botocore
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

from NDATools import exit_error, NDA_TOOLS_SUBMISSIONS_FOLDER
from NDATools.Utils import get_s3_client_with_config, deconstruct_s3_url, get_directory_input, SqlUtils
from NDATools.upload.batch_file_uploader import BatchFileUploader, UploadContext, Uploadable, UploadError, \
    files_not_found_msg, BatchResults
from NDATools.upload.submission.api import Submission, AssociatedFile, AssociatedFileUploadCreds, SubmissionApi, \
    BatchUpdate, AssociatedFileStatus, UploadProgress

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
    def __init__(self, submission: Submission, resuming_upload: bool,
                 transfer_config: TransferConfig, search_folders: List[pathlib.Path], db_folder=None):
        self.submission = submission
        self.resuming_upload = resuming_upload
        self.upload_progress = None
        self.transfer_config = transfer_config
        self.files_not_found = []
        self.search_folders = search_folders
        self.progress_bar = None
        # initialize this to false, and toggle to true after we prompt user to enter folder
        self.display_missing_files_message = False
        if not db_folder:
            db_folder = NDA_TOOLS_SUBMISSIONS_FOLDER
        self.db_path = pathlib.Path(db_folder, f'{submission.submission_id}.db')
        self.db_connection = sqlite3.connect(self.db_path)

    @property
    def file_count_in_submission(self):
        return self.upload_progress.associated_file_count

    @property
    def remaining_file_count(self):
        return self.upload_progress.associated_file_count - self.upload_progress.uploaded_file_count


class _AssociatedBatchFileUploader(BatchFileUploader):
    def __init__(self, api, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        super().__init__(max_threads, exit_on_error, hide_progress, batch_size)
        self.api = api

    def _construct_tqdm(self, total: Optional[float], description: str):
        """Override progress bar to display total number of files and save to upload ctx"""
        progress_bar = tqdm(disable=self.hide_progress, total=total, initial=0, leave=False, desc=description,
                            unit='B', unit_scale=True, unit_divisor=1024)
        self.upload_context.progress_bar = progress_bar
        return progress_bar

    def _pre_upload_hook(self):
        '''Get all the files in the submission from the API and save it to a temporary sql lite db'''
        self._check_db_integrity()

    def _get_file_batches(self):

        # create temp table for current batch of files
        SqlUtils.run_ddl(self.upload_context.db_connection, "DROP TABLE if exists current_batch")
        SqlUtils.run_ddl(self.upload_context.db_connection,
                         "CREATE TEMPORARY TABLE current_batch as select * from associated_files where status <> 'Complete' order by id")

        page = 0
        while True:
            # query from temp table one page at a time
            page += 1
            files = SqlUtils.paged_query(self.upload_context.db_connection, "current_batch", page, self.batch_size,
                                         AssociatedFile)

            if not files:
                break
            # hash files by id to make searching easier
            lookup = {file.id: file for file in files}
            creds: List[AssociatedFileUploadCreds] = self.api.get_upload_credentials(
                self.upload_context.submission.submission_id,
                list(lookup.keys()))
            yield [AFUploadable(lookup[c.id], c) for c in creds]

    def _update_bytes_uploaded(self, bytes_uploaded):
        self.upload_context.progress_bar.update(bytes_uploaded)

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
                    # (according to the boto3 documentation, head_object will produce 404 or 403 depending on the permissions)
                    if str(ce.response['Error']['Code']) in ['404', '403']:
                        s3.upload_file(file_name, bucket, key, Config=self.upload_context.transfer_config,
                                       Callback=self._update_bytes_uploaded)
                    else:
                        raise UploadError(up, ce)
            else:
                s3.upload_file(file_name, bucket, key, Config=self.upload_context.transfer_config,
                               Callback=self._update_bytes_uploaded)
        except Exception as e:
            logger.error(f'Unexpected error occurred while uploading {up.search_name}: {e}')
            logger.error(traceback.format_exc())
            raise UploadError(up, e)

    def _post_batch_hook(self, batch_results: BatchResults):
        """ REST endpoint to update status of files to COMPLETE"""
        if len(batch_results.success) > 0:
            # it makes sense to show the missing files message if the program successfully processed at least 1 file
            self.upload_context.display_missing_files_message = True
            self._batch_update_associated_file_status(batch_results.success)
        self.upload_context.files_not_found.extend(batch_results.files_not_found)
        self.upload_context.upload_progress.uploaded_file_count += len(batch_results.success)

    def _post_upload_hook(self):
        if self.upload_context.remaining_file_count == 0:
            tqdm.write("\nAll associated files have been uploaded.")
            self._close_and_delete_submission_db()
        if len(self.upload_context.files_not_found) > 0:
            tqdm.write(f"{len(self.upload_context.files_not_found)} associated files are not found.")

        while self.upload_context.files_not_found:
            searched_folders = self.upload_context.search_folders
            new_dir = self._prompt_for_file_directory(searched_folders)
            # update upload_context variables
            self.upload_context.files_not_found.clear()
            self.upload_context.search_folders.clear()
            self.upload_context.search_folders.append(new_dir)
            for file_batch in self._get_file_batches():
                self._upload_batch(file_batch, self.upload_context.search_folders)

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
            return get_directory_input('\nSpecify the folder containing the associated files:')

    def _close_and_delete_submission_db(self):
        self.upload_context.db_connection.close()
        os.remove(self.upload_context.db_path)

    def _make_and_connect_submission_db(self):
        logger.debug(
            f'Creating temporary database at {self.upload_context.db_path}')
        self.upload_context.db_connection = sqlite3.connect(self.upload_context.db_path)
        SqlUtils.create_table_from_model(self.upload_context.db_connection, AssociatedFile, "associated_files")
        logger.info(
            'Retrieving a listing of the files in the submission. This may take a couple of minutes if the number of files in your submission exceeds 100,000')
        db_creation_batch_size = min(self.batch_size * 1000, 100_000)
        page_number = 0
        is_large_submission = False
        while True:
            files: List[AssociatedFile] = self.api.get_files_by_page(self.upload_context.submission.submission_id,
                                                                     page_number, db_creation_batch_size,
                                                                     exclude_uploaded=False)
            if not files:
                break
            SqlUtils.bulk_insert(self.upload_context.db_connection, "associated_files", files)

            if page_number == 0:
                is_large_submission = len(files) == db_creation_batch_size
            # display progress for large submissions
            if is_large_submission:
                logger.info(f'Retrieved {len(files)} files from page {page_number}')
            # dont bother making another api call if the number of retrieved results is less than the batch size
            if len(files) < db_creation_batch_size:
                break

            page_number += 1
        logger.debug(f'Finished retrieving all files from submission')

    def _check_db_integrity(self):
        exists = SqlUtils.does_table_exists(self.upload_context.db_connection, "associated_files")
        if not exists:
            self._make_and_connect_submission_db()
        self._calculate_upload_progress()

    def _batch_update_associated_file_status(self, updates: List[AFUploadable]):
        self._mark_files_complete_in_api(updates)
        self._mark_files_complete_in_db(updates)

    def _mark_files_complete_in_db(self, uploaded: List[Uploadable]):
        ids = [up.af_file.id for up in uploaded]
        # split ids into a list of lists each containing at most 1000 numbers
        id_chunks = [ids[i:i + 1000] for i in range(0, len(ids), 1000)]
        for id_chunk in id_chunks:
            ids_clause = ','.join([str(id) for id in id_chunk])
            SqlUtils.update(self.upload_context.db_connection, "associated_files", "status='Complete'",
                            f"id in ({ids_clause})")

    def _mark_files_complete_in_api(self, uploaded: List[AFUploadable]):
        """ REST endpoint to update status of files to COMPLETE"""
        submission_id = self.upload_context.submission.submission_id
        updates = [BatchUpdate(file.af_file, AssociatedFileStatus.COMPLETE, file.calculate_size()) for file in uploaded]
        errors = None
        if len(updates) > 0:
            errors = self.api.batch_update_associated_file_status(submission_id, updates)
        if errors:
            # filter out errors where the error message indicates that the file already has a status of COMPLETE
            real_errors = [e for e in errors if not e.message.startswith('Cannot change "status" for submission file')]
            if real_errors:
                for error in real_errors:
                    logger.error(f'Error updating status of file {error.search_name.file_user_path}: {error.message}')
                logger.error(f'There were errors uploading files. \r\n'
                             f'Please try resuming the submission by running vtcmd -r {submission_id}\r\n'
                             f'If the error persists, contact NDAHelp@mail.nih.gov for help.')
                exit_error()

    def _calculate_upload_progress(self):
        total_file_count = SqlUtils.query(self.upload_context.db_connection, "associated_files", "count(*)")[0][0]
        uploaded_file_count = SqlUtils.query(self.upload_context.db_connection, "associated_files", "count(*)",
                                             where_clause="status = 'Complete'")[0][0]
        self.upload_context.upload_progress = UploadProgress(uploaded_file_count=uploaded_file_count,
                                                             associated_file_count=total_file_count)


KB = 1024
GB = KB * KB * KB


class AssociatedFileUploader:

    def __init__(self, api: SubmissionApi, max_threads, exit_on_error=False, hide_progress=False, batch_size=50,
                 db_folder=None):
        self.api = api
        self.uploader = _AssociatedBatchFileUploader(api, max_threads, exit_on_error, hide_progress, batch_size)

    def start_upload(self, submission: Submission, search_folders: List[pathlib.Path], resuming_upload: bool,
                     db_folder=None):
        transfer_config = TransferConfig(multipart_threshold=5 * GB, use_threads=False)
        ctx = AFUploadContext(submission, resuming_upload, transfer_config, search_folders, db_folder)
        self.uploader.start_upload(search_folders, ctx)
