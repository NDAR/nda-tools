import logging
import os
import pathlib
from abc import ABC, abstractmethod
from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import RLock
from typing import List, Callable, Union

from tqdm import tqdm

from NDATools import exit_error
from NDATools.upload.submission.api import SubmissionApi

logger = logging.getLogger(__name__)


def files_not_found_msg(not_found, dirs, limit=20):
    files_not_found = [m.name for m in not_found]
    msg = f'The following files could not be found in {dirs}:\n'
    msg += '\n'.join(files_not_found[:limit])
    if len(files_not_found) > limit:
        msg += f'\n... and {len(files_not_found) - limit} more'
    return msg


class Uploadable(ABC):
    """Represents a file that needs to be uploaded but has not yet been matched with a file on the user''s computer"""

    def __init__(self):
        self._path: Union[pathlib.Path, None] = None

    @property
    @abstractmethod
    def search_name(self):
        ...

    @property
    def path(self):
        return self._path

    @path.setter
    @abstractmethod
    def path(self, new_value):
        self._path = new_value

    def calculate_size(self):
        if not self.path.exists():
            raise Exception(f'Cannot calculate size because File {self.path} does not exist')
        return self.path.stat().st_size


class UploadError(Exception):
    def __init__(self, file: Uploadable, unexpected_error: Exception = None):
        self.file = file
        self.error = unexpected_error


class BatchContextABC(ABC):
    def __init__(self, files_found: List[Uploadable], files_not_found: List[Uploadable]):
        self.files_found = files_found
        self.files_not_found = files_not_found


class UploadContextABC(ABC):
    ...


class BatchFileUploader(ABC):
    def __init__(self, api, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        self.api: SubmissionApi = api
        self.max_threads = max_threads
        self.hide_progress = hide_progress
        self.exit_on_error = exit_on_error
        self.batch_size = batch_size
        self.not_found_list: List[Uploadable] = []
        self.upload_context = None
        self.batch_context = None
        self.upload_lock = RLock()  # lock to prevent concurrent uploads

    def start_upload(self, ctx: UploadContextABC, search_folders: List[pathlib.Path]):
        with self.upload_lock:
            if not search_folders:
                search_folders = os.getcwd()
            self.upload_context = ctx
            self._pre_upload_hook()

            with self._construct_tqdm() as progress_bar:
                for file_batch in self._get_file_batches():
                    self._upload_batch(file_batch, search_folders, lambda: progress_bar.update(1))
                self._process_not_found_files(search_folders, progress_bar)

            self._post_upload_hook()

    def _upload_batch(self, files: List[Uploadable], search_folders: List[pathlib.Path],
                      progress_cb: Callable):
        assert len(files) > 0, "no files passed to _upload_batch method"

        # group files by whether the path exists
        def group_files_by_path_exists():
            exists: List[Uploadable] = []
            not_exists: List[Uploadable] = []
            for m in files:
                for folder in search_folders:
                    path = pathlib.Path(folder, m.search_name)
                    if path.exists():
                        m.path = path
                        exists.append(m)
                        break
                else:
                    not_exists.append(m)
            return exists, not_exists

        files_found, not_found = group_files_by_path_exists()
        self.batch_context = BatchContextABC(files_found, not_found)
        self._pre_batch_hook()

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._upload_file, man) for man in files_found]

        for f in as_completed(futures):
            if f.exception():
                exit_error()
            else:
                progress_cb()

        if not_found:
            self.not_found_list.extend(not_found)
        self._post_batch_hook()

    # generator for file batches
    @abstractmethod
    def _get_file_batches(self):
        ...

    @abstractmethod
    def _upload_file(self, file: Uploadable):
        ...

    def _process_not_found_files(self, search_folders: List[pathlib.Path], progress_bar):
        """Default method to handle missing files. Will print and exit. Can be overridden in subclasses"""
        msg = files_not_found_msg(self.not_found_list, search_folders)
        exit_error(msg)

    def _construct_tqdm(self):
        """Default method to construct progress bar. Can be overridden in subclasses"""
        return tqdm(disable=self.hide_progress)

    def _pre_batch_hook(self):
        pass

    def _post_batch_hook(self):
        pass

    def _pre_upload_hook(self):
        pass

    def _post_upload_hook(self):
        pass
