import logging
import os
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Union

from tqdm import tqdm

from NDATools.Utils import exit_error
from NDATools.upload.validation.api import ValidationApi, ValidationV2Credentials

logger = logging.getLogger(__name__)


class ManifestFile:
    def __init__(self, name: str, s3_destination: str, uuid: str, record_number: int):
        self.name = name
        self.s3_destination = s3_destination
        self.uuid = uuid
        self.record_number = record_number

    def resolve_local_path(self, manifest_dir: pathlib.Path):
        return manifest_dir / self.name

    @staticmethod
    def manifests_from_credentials(creds):
        return [
            ManifestFile(m['localFileName'], m['s3Destination'], m['uuid'], m['recordNumber'], None)
            for m in creds.download_manifests()
        ]

    def __eq__(self, other):
        if isinstance(other, ManifestFile):
            return self.uuid == other.uuid
        return False

    def __hash__(self):
        return self.uuid


class ManifestUploadError(Exception):
    def __init__(self, manifest: ManifestFile, unexpected_error: Exception = None):
        self.manifest = manifest
        self.error = unexpected_error


def _manifests_not_found_msg(manifests: List[ManifestFile], manifests_dir: str):
    files_not_found = [m.local_file_name for m in manifests]
    msg = f'The following manifests could not be found in {manifests_dir}:\n'
    msg += '\n'.join(files_not_found[:20])
    if len(files_not_found) > 20:
        msg += f'\n... and {len(files_not_found) - 20} more'
    return msg


class ManifestsUploader:
    def __init__(self, validation_api: ValidationApi, max_threads, exit_on_error=False, hide_progress=False):
        self.validation_api = validation_api
        self.max_threads = max_threads
        self.hide_progress = hide_progress
        self.exit_on_error = exit_on_error

    def upload_manifests(self, creds: Union[List[ValidationV2Credentials], ValidationV2Credentials],
                         manifest_dir: pathlib.Path):
        # normalize parameter to list
        if isinstance(creds, ValidationV2Credentials):
            creds = [creds]

        # generator for manifest files
        def get_manifest_batches():
            for c in creds:
                yield ManifestFile.manifests_from_credentials(c), c

        logger.info(f'\nUploading manifests...')
        if not manifest_dir:
            manifest_dir = os.getcwd()

        count = 0
        with tqdm(disable=self.hide_progress) as progress_bar:
            for manifest_batch, creds in get_manifest_batches():
                self._upload_manifests_batch(manifest_batch, creds, manifest_dir, lambda _: progress_bar.update(1))
                count += len(manifest_batch)

        logger.debug(f'Finished uploading {count} manifests')

    def _upload_manifests_batch(self,
                                manifests_found: List[ManifestFile],
                                creds: ValidationV2Credentials,
                                manifest_dir: pathlib.Path,
                                progress_cb: Callable):

        assert len(manifests_found) > 0, "no manifests passed to _upload_manifests method"

        # group manifests by whether the path exists
        def group_manifests_by_path_exists():
            exists = []
            not_exists = []
            for m in manifests_found:
                if m.resolve_local_path(manifest_dir).exists():
                    exists.append(m)
                else:
                    not_exists.append(m)
            return exists, not_exists

        manifests_found, not_found = group_manifests_by_path_exists()

        def upload(m):
            try:
                creds.upload(str(m.resolve_local_path(manifest_dir)), m.s3_destination)
            except Exception as e:
                logger.error(f'Unexpected error occurred while uploading {m}: {e}')
                logger.error(traceback.format_exc())
                raise ManifestUploadError(m, e)

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(upload, man) for man in manifests_found]

        for f in as_completed(futures):
            if f.exception():
                exit_error()
            else:
                progress_cb()

        if not_found:
            self._handle_manifests_not_found(not_found, creds, manifest_dir, progress_cb)

    def _handle_manifests_not_found(self,
                                    errs: List[ManifestFile],
                                    creds: ValidationV2Credentials,
                                    manifest_dir: pathlib.Path,
                                    progress_cb: Callable = None):
        # ask the user if they want to continue
        msg = _manifests_not_found_msg(errs, str(manifest_dir))
        files_not_found = list(map(lambda x: x.manifest, errs))
        if not self.exit_on_error:
            exit_error(msg)
        else:
            logger.info(msg)
        while True:
            retry_manifests_dir = pathlib.Path(input(
                'Press the "Enter" key to specify location for manifest files and try again:'))
            if not retry_manifests_dir.exists():
                logger.error(f'{retry_manifests_dir} does not exist. Please try again.')
            else:
                break
        self._upload_manifests_batch(files_not_found, creds, retry_manifests_dir, progress_cb)
