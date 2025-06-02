import logging
import os
import pathlib
from abc import ABC, abstractmethod
from concurrent.futures import as_completed, ThreadPoolExecutor
from os import PathLike
from threading import RLock
from typing import List, Callable, Union

from tqdm import tqdm

from NDATools import exit_error

logger = logging.getLogger(__name__)


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
    def path(self, new_value):
        self._path = new_value

    def calculate_size(self):
        if not self.path.exists():
            raise FileNotFoundError(f'Cannot calculate size because File {self.path} was not found')
        return self.path.stat().st_size


class UploadError(Exception):
    def __init__(self, file: Uploadable, unexpected_error: Exception = None):
        self.file = file
        self.error = unexpected_error


class BatchResults:
    def __init__(self, success: List[Uploadable], files_not_found: List[Uploadable],
                 search_folders: List[PathLike]):
        self.success = success
        self.files_not_found = files_not_found
        self.search_folders = search_folders


class UploadContext:
    ...


class BatchFileUploader(ABC):
    def __init__(self, max_threads, exit_on_error=False, hide_progress=False, batch_size=50):
        self.max_threads = max_threads
        self.hide_progress = hide_progress
        self.exit_on_error = exit_on_error
        self.batch_size = batch_size
        self.upload_context = None
        self.upload_lock = RLock()  # lock to prevent concurrent uploads

    def start_upload(self, search_folders: List[os.PathLike], ctx: UploadContext = None):
        with self.upload_lock:
            if not search_folders:
                search_folders = [pathlib.Path(os.getcwd())]
            self.upload_context = ctx

            with self._construct_tqdm() as progress_bar:
                for file_batch in self._get_file_batches():
                    self._upload_batch(file_batch, search_folders, lambda: progress_bar.update(1))
                self._post_upload_hook()

    def _upload_batch(self, files: List[Uploadable], search_folders: List[os.PathLike], progress_cb: Callable):
        assert len(files) > 0, "no files passed to _upload_batch method"
        self._pre_batch_hook(files, search_folders)

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

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._upload_file, man) for man in files_found]

        for f in as_completed(futures):
            if f.exception():
                exit_error()
            else:
                progress_cb()

        self._post_batch_hook(BatchResults(files_found, not_found, search_folders))

    # generator for file batches
    @abstractmethod
    def _get_file_batches(self):
        ...

    @abstractmethod
    def _upload_file(self, file: Uploadable):
        ...

    def _construct_tqdm(self):
        """Default method to construct progress bar. Can be overridden in subclasses"""
        return tqdm(disable=self.hide_progress)

    def _pre_batch_hook(self, found: List[Uploadable], search_folders: List[PathLike]):
        ...

    def _post_batch_hook(self, batch_results: BatchResults):
        _process_not_found_files(batch_results.files_not_found, batch_results.search_folders)

    def _post_upload_hook(self):
        ...


def _process_not_found_files(not_found_list: List[Uploadable], search_folders: List[PathLike]):
    """Default method to handle missing files. Will print and exit. Can be overridden in subclasses"""
    msg = files_not_found_msg(not_found_list, search_folders)
    exit_error(msg)


def files_not_found_msg(not_found: List[Uploadable], dirs, limit=20):
    files_not_found = [m.search_name for m in not_found]
    if not isinstance(dirs, list):
        dirs = [dirs]
    dir_sting = "\n".join([str(d) for d in dirs])
    msg = f'The following files could not be found in {dir_sting}:\n'
    msg += '\n'.join(files_not_found[:limit])
    if len(files_not_found) > limit:
        msg += f'\n... and {len(files_not_found) - limit} more'
    return msg
