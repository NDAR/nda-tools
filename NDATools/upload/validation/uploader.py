import logging
import multiprocessing
import os
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable

from tqdm import tqdm

from NDATools.Utils import exit_error
from NDATools.upload import ManifestFile, ManifestNotFoundError
from NDATools.upload.validation.api import ValidationApi

logger = logging.getLogger(__name__)


def _manifests_not_found_msg(errs: List[ManifestNotFoundError], manifests_dir: str):
    files_not_found = list(map(lambda x: x.manifest.local_file_name, errs))
    msg = f'The following manifests could not be found in {manifests_dir}:\n'
    msg += '\n'.join(files_not_found[:20])
    if len(files_not_found) > 20:
        msg += f'\n... and {len(files_not_found) - 20} more'
    return msg


class ManifestsUploader:
    def __init__(self, validation_api: ValidationApi, max_threads=None, interactive=True, hide_progress=False):
        self.validation_api = validation_api
        self.max_threads = max_threads or max([1, multiprocessing.cpu_count() - 1])
        self.hide_progress = hide_progress
        self.interactive = interactive

    def upload_manifests(self, manifests: List[ManifestFile], manifest_dir: pathlib.Path):
        manifest_count = len(manifests)
        validated_files = {m.validated_file for m in manifests}

        logger.info(f'\nUploading {manifest_count} manifests...')
        if not manifest_dir:
            manifest_dir = os.getcwd()
        with tqdm(total=manifest_count, disable=self.hide_progress) as progress_bar:
            # do this one validation_result at a time in order to avoid running multiple instances of S3Transfer simultaneously
            for v in validated_files:
                self._upload_manifests_helper(v.manifests, manifest_dir, lambda x: progress_bar.update(1))

        logger.debug(f'Finished uploading {manifest_count} manifests')

    def _upload_manifests_helper(self,
                                 manifests: List[ManifestFile],
                                 manifest_dir: pathlib.Path,
                                 progress_cb: Callable = None):

        assert len(manifests) > 0, "no manifests passed to _upload_manifests method"
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = list(
                map(lambda man: executor.submit(man.upload_manifest, manifest_dir), manifests))

        not_found_errors = []
        for f in as_completed(futures):
            err = f.exception()
            if err:
                if isinstance(err, ManifestNotFoundError):
                    not_found_errors.append(err)
                else:
                    logger.error(
                        f'Unexpected errors occurred while uploading {err.manifest.name}: {err.error}')
                    traceback.print_exc()
                    exit_error()
            else:
                # success
                if progress_cb:
                    progress_cb()

        if not_found_errors:
            self._handle_manifests_not_found(not_found_errors, manifest_dir, progress_cb)

    def _handle_manifests_not_found(self,
                                    errs: List[ManifestNotFoundError],
                                    manifest_dir: pathlib.Path,
                                    progress_cb: Callable = None):
        # ask the user if they want to continue
        msg = _manifests_not_found_msg(errs, str(manifest_dir))
        files_not_found = list(map(lambda x: x.manifest, errs))
        if not self.interactive:
            exit_error(msg)
        while True:
            retry_manifests_dir = pathlib.Path(input(
                'Press the "Enter" key to specify location for manifest files and try again:'))
            if not retry_manifests_dir.exists():
                logger.error(f'{retry_manifests_dir} does not exist. Please try again.')
            else:
                break
        self._upload_manifests_helper(files_not_found, retry_manifests_dir, progress_cb)
