import logging
import math
import os
import pathlib
import traceback
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import List, Callable

from tqdm import tqdm

from NDATools import exit_error
from NDATools.upload.submission.api import Submission, AssociatedFile, AssociatedFileStatus, BatchUpdate, \
    AssociatedFileUploadCreds

logger = logging.getLogger(__name__)


class AssociatedFileUploadError(Exception):
    def __init__(self, file: AssociatedFile, unexpected_error: Exception = None):
        self.file = file
        self.error = unexpected_error


def _associated_files_not_found_msg(not_found, dirs):
    files_not_found = [m.name for m in not_found]
    msg = f'The following files could not be found in {dirs}:\n'
    msg += '\n'.join(files_not_found[:20])
    if len(files_not_found) > 20:
        msg += f'\n... and {len(files_not_found) - 20} more'
    return msg


class SubmissionFileWithCreds:
    '''Represents a file that needs to be uploaded for a submission, but has not yet been matched with a file on the users computer'''

    def __init__(self, submission_file: AssociatedFile, creds):
        self.file = submission_file
        self.creds = creds

    @property
    def relative_path(self):
        return self.file.file_user_path


class LocalFileUpload:
    ''' Represents a SubmissionFileWithCreds that was found on user''s computer'''

    def __init__(self, path: pathlib.Path, submission_file: SubmissionFileWithCreds):
        self.path = path
        self.size = 0
        self.file = submission_file

    def calculate_size(self):
        if self.size == 0:
            self.size = self.path.stat().st_size


class BatchContext:
    def __init__(self, files_found: List[LocalFileUpload], files_not_found: List[SubmissionFileWithCreds]):
        self.files_found = files_found
        self.files_not_found = files_not_found
        self.batch_updates = []

    def add_batch_update(self, update: BatchUpdate):
        self.batch_updates.append(update)


class UploadContext:
    def __init__(self, submission: Submission, resuming_upload: bool):
        self.submission = submission
        self.resuming_upload = resuming_upload


class AssociatedFileUploader:
    def __init__(self, api, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        self.api = api
        self.max_threads = max_threads
        self.hide_progress = hide_progress
        self.exit_on_error = exit_on_error
        self.batch_size = batch_size
        self.not_found_list = []
        self.upload_context = None
        self.batch_context = None

    def upload_associated_files(self, submission: Submission, associated_file_dirs: List[pathlib.Path],
                                resuming_upload=False):
        self.upload_context = UploadContext(submission, resuming_upload)
        total_results = self.api.get_upload_progress(submission.submission_id).associated_file_count

        if not associated_file_dirs:
            associated_file_dirs = os.getcwd()

        with tqdm(disable=self.hide_progress, total=total_results) as progress_bar:
            for file_batch in self._get_file_batches():
                self._upload_associated_files_batch(file_batch, associated_file_dirs, lambda: progress_bar.update(1))

            self.process_not_found_files()

    def _upload_associated_files_batch(self,
                                       associated_files: List[SubmissionFileWithCreds],
                                       associated_file_dirs: List[pathlib.Path],
                                       progress_cb: Callable):
        assert len(associated_files) > 0, "no associated files passed to _upload_associated_files_batch method"

        # group files by whether the path exists
        def group_files_by_path_exists():
            exists: List[LocalFileUpload] = []
            not_exists: List[SubmissionFileWithCreds] = []
            for m in associated_files:
                for folder in associated_file_dirs:
                    path = pathlib.Path(folder, m.relative_path)
                    if path.exists():
                        exists.append(LocalFileUpload(path, m))
                        break
                else:
                    not_exists.append(m)
            return exists, not_exists

        files_found, not_found = group_files_by_path_exists()
        self.batch_context = BatchContext(files_found, not_found)
        self._pre_batch_hook()

        def upload(m):
            try:
                pass
                m.calculate_size()
                self.batch_context.add_batch_update(BatchUpdate(m.file, AssociatedFileStatus.COMPLETE, m.size))
            except Exception as e:
                logger.error(f'Unexpected error occurred while uploading {m}: {e}')
                logger.error(traceback.format_exc())
                raise AssociatedFileUploadError(m, e)

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(upload, man) for man in files_found]

        for f in as_completed(futures):
            if f.exception():
                exit_error()
            else:
                progress_cb()

        if not_found:
            self.not_found_list.extend(not_found)
        self._post_batch_hook()

    def _pre_batch_hook(self):
        pass

    def _post_batch_hook(self):
        submission_id = self.upload_context.submission.id
        updates = self.batch_context.batch_updates
        errors = self.api.batch_update_associated_file_status(submission_id, updates, AssociatedFileStatus.COMPLETE)
        if errors:
            for error in errors:
                logger.error(f'Error updating status of file {error.file.file_user_path}: {error.message}')
            exit_error()

    def _handle_files_not_found(self, not_found: List[AssociatedFile], associated_file_dirs: List[pathlib.Path],
                                progress_cb: Callable = None) -> pathlib.Path:
        # ask the user if they want to continue
        msg = _associated_files_not_found_msg(not_found, associated_file_dirs)
        if self.exit_on_error:
            exit_error(msg)
        else:
            logger.info(msg)
        while True:
            retry_associated_files_dir = pathlib.Path(input(
                'Press the "Enter" key to specify location for associated files and try again:'))
            if not retry_associated_files_dir.exists():
                logger.error(f'{retry_associated_files_dir} does not exist. Please try again.')
            else:
                return retry_associated_files_dir

    # generator for file batches
    def _get_file_batches(self):
        last_page = math.ceil(total_results / self.batch_size)
        page_number = last_page - 1  # pages are 0 based
        while page_number >= 0:
            files: List[AssociatedFile] = self.api.get_files_by_page(submission.submission_id, page_number,
                                                                     self.batch_size)
            if not files:
                break
            # hash files by id to make searching easier
            lookup = {file.id: file for file in files}
            creds: List[AssociatedFileUploadCreds] = self.api.get_upload_credentials(submission.submission_id,
                                                                                     [m.id for m in files])
            yield [SubmissionFileWithCreds(lookup[c.id], c) for c in creds]
            page_number -= 1

    def process_not_found_files(self):
        def batch_not_found_files():
            raise NotImplementedError()

        new_dir = self._handle_files_not_found(self.not_found_list, associated_file_dirs)
        for file_batch in batch_not_found_files():
            self._upload_associated_files_batch(file_batch, [new_dir], lambda: progress_bar.update(1))
