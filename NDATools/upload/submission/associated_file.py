import logging
import math
import pathlib
import traceback
from typing import List, Callable

from boto3.s3.transfer import TransferConfig

from NDATools import exit_error
from NDATools.Utils import get_s3_client_with_config, deconstruct_s3_url
from NDATools.upload.submission.api import Submission, AssociatedFile, AssociatedFileStatus, BatchUpdate, \
    AssociatedFileUploadCreds, SubmissionApi
from NDATools.upload.batch_file_uploader import BatchFileUploader, UploadContextABC, BatchContextABC, \
    Uploadable, UploadError

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

class AFBatchContext(BatchContextABC):
    def __init__(self, files_found: List[Uploadable], files_not_found: List[Uploadable]):
        super().__init__(files_found, files_not_found)
        self.batch_updates = []

    def add_batch_update(self, update: BatchUpdate):
        self.batch_updates.append(update)


class AFUploadContext(UploadContextABC):
    def __init__(self, submission: Submission, resuming_upload: bool, total_files: int, transfer_config: TransferConfig):
        self.submission = submission
        self.resuming_upload = resuming_upload
        self.total_files = total_files
        self.transfer_config = transfer_config

class _AssociatedBatchFileUploader(BatchFileUploader):
    def __init__(self, api, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        super().__init__(api, max_threads, exit_on_error, hide_progress, batch_size)
        self.api = api

    def _get_file_batches(self):
        last_page = math.ceil(self.upload_context.total_files/ self.batch_size)

        page_number = last_page - 1  # pages are 0 based
        while page_number >= 0:
            submission = self.upload_context.submission
            files: List[AssociatedFile] = self.api.get_files_by_page(submission.submission_id, page_number,
                                                                     self.batch_size)
            if not files:
                break
            # hash files by id to make searching easier
            lookup = {file.id: file for file in files}
            creds: List[AssociatedFileUploadCreds] = self.api.get_upload_credentials(submission.submission_id,lookup.keys())
            yield [AFUploadable(lookup[c.id], c) for c in creds]
            page_number -= 1

    def _upload_file(self, up: AFUploadable):
        try:
            file_name = str(up.path.resolve())
            bucket, key = deconstruct_s3_url(up.af_file.file_remote_path)
            creds = up.upload_creds
            access_key, secret_key, session_token = creds.access_key, creds.secret_key, creds.session_token
            s3 = get_s3_client_with_config(access_key, secret_key, session_token)
            s3.upload_file(file_name, bucket, key, Config=self.upload_context.transfer_config)
            update = BatchUpdate(up.af_file, AssociatedFileStatus.COMPLETE, up.calculate_size())
            self.batch_context.add_batch_update(update)
        except Exception as e:
            logger.error(f'Unexpected error occurred while uploading {up.search_name}: {e}')
            logger.error(traceback.format_exc())
            raise UploadError(up, e)

    def _post_batch_hook(self):
        submission_id = self.upload_context.submission.id
        updates = self.batch_context.batch_updates
        errors = self.api.batch_update_associated_file_status(submission_id, updates, AssociatedFileStatus.COMPLETE)
        if errors:
            for error in errors:
                logger.error(f'Error updating status of file {error.search_name.file_user_path}: {error.message}')
            exit_error()

    def _process_not_found_files(self):
        def batch_not_found_files():
            raise NotImplementedError()

        new_dir = self._handle_files_not_found(self.not_found_list, associated_file_dirs)
        for file_batch in batch_not_found_files():
            self.upload_batch(file_batch, [new_dir], lambda: progress_bar.update(1))

    def __handle_files_not_found(self, not_found: List[AssociatedFile], search_folders: List[pathlib.Path],
                                progress_cb: Callable = None) -> pathlib.Path:
        # ask the user if they want to continue
        msg = _files_not_found_msg(not_found, search_folders)
        exit_error(msg)
        if self.exit_on_error:

        else:
            logger.info(msg)
        while True:
            retry_associated_files_dir = pathlib.Path(input(
                'Press the "Enter" key to specify location for associated files and try again:'))
            if not retry_associated_files_dir.exists():
                logger.error(f'{retry_associated_files_dir} does not exist. Please try again.')
            else:
                return retry_associated_files_dir

KB = 1024
GB = KB * KB * KB

class AssociatedFileUploader:

    def __init__(self, api: SubmissionApi, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        self.api = api
        self.uploader = _AssociatedBatchFileUploader(api, max_threads, exit_on_error, hide_progress, batch_size)

    def start_upload(self, submission: Submission, search_folders: List[pathlib.Path], resuming_submission: bool):
        total_files = self.api.get_upload_progress(submission.submission_id).associated_file_count
        transfer_config = TransferConfig(multipart_threshold=5 * GB, use_threads=False)
        cxt = AFUploadContext(submission, resuming_submission, total_files, transfer_config)
        self.uploader.start_upload(cxt, search_folders)
