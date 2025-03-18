import logging
import multiprocessing
import os
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from NDATools.Utils import exit_error
from NDATools.upload.validation.api import ValidationResponse, ValidationManifest, ValidationApi

logger = logging.getLogger(__name__)


class ManifestError(Exception):
    def __init__(self, manifest: ValidationManifest, unexpected_error: Exception = None):
        self.manifest = manifest
        self.error = unexpected_error


class ManifestNotFoundError(ManifestError):
    ...


def _manifests_not_found_msg(errs: [ManifestNotFoundError], manifests_dir: str):
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

    def _upload_manifest_for_validation(self, manifest: ValidationManifest, manifest_dir: pathlib.Path):
        creds = manifest.validation_response.rw_creds
        try:
            local_file = pathlib.Path(os.path.join(manifest_dir, manifest.local_file_name))
            if not local_file.exists():
                raise ManifestNotFoundError(manifest)
            creds.upload(str(local_file), manifest.s3_destination)
        except Exception as e:
            if not isinstance(e, ManifestNotFoundError):
                logger.error(f'Unexpected error occurred while uploading {manifest.local_file_name}: {e}')
                logger.error(traceback.format_exc())
                raise ManifestError(manifest, e)
            else:
                logger.debug(
                    f'Could not find manifest {manifest.local_file_name} from file {manifest.validation_response.file.name} in {manifest_dir}')
                raise e

    def _handle_manifests_not_found(self,
                                    errs: [ManifestNotFoundError],
                                    manifest_dir: pathlib.Path,
                                    progress_bar: tqdm):
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
        self._upload_manifests(files_not_found, retry_manifests_dir, progress_bar)

    def upload_manifests(self, manifests: [ValidationManifest], manifest_dir: pathlib.Path):
        logger.info(f'\nUploading {len(manifests)} manifests')
        if not manifest_dir:
            manifest_dir = os.getcwd()
        validation_results: {ValidationResponse} = {m.validation_response for m in manifests}
        with tqdm(total=len(manifests), disable=self.hide_progress) as progress_bar:
            # do this one validation_result at a time in order to avoid running multiple instances of S3Transfer simultaneously
            for v in validation_results:
                self._upload_manifests(v.manifests, manifest_dir, progress_bar)

        logger.debug(f'Finished uploading {len(manifests)} manifests')

    def _upload_manifests(self,
                          manifests: [ValidationManifest],
                          manifest_dir: pathlib.Path,
                          progress_bar: tqdm):
        assert len(manifests) > 0, "no manifests passed to _upload_manifests method"
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = list(
                map(lambda man: executor.submit(self._upload_manifest_for_validation, man, manifest_dir), manifests))

        not_found_errors = []
        for f in as_completed(futures):
            err = f.exception()
            if err:
                if isinstance(err, ManifestNotFoundError):
                    not_found_errors.append(err)
                else:
                    logger.error(
                        f'Unexpected errors occurred while uploading {err.manifest.local_file_name}: {err.error}')
                    traceback.print_exc()
                    exit_error()
            else:
                # success
                progress_bar.update(1)

        if not_found_errors:
            self._handle_manifests_not_found(not_found_errors, manifest_dir, progress_bar)
