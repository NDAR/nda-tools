import logging
import os
import traceback
from os import PathLike
from typing import List, Union

from tqdm import tqdm

from NDATools import exit_error
from NDATools.Utils import get_directory_input
from NDATools.upload.batch_file_uploader import BatchFileUploader, Uploadable, BatchResults, UploadError, \
    UploadContext, files_not_found_msg
from NDATools.upload.validation.api import ValidationV2Api, ValidationV2Credentials

logger = logging.getLogger(__name__)


class ManifestFile:
    def __init__(self, name: str, s3_destination: str, uuid: str, record_number: int, column: str):
        self.name = name
        self.s3_destination = s3_destination
        self.uuid = uuid
        self.record_number = record_number
        self.column = column

    @staticmethod
    def manifests_from_credentials(creds):
        return [
            ManifestFile(m['localFileName'], m['s3Destination'], m['uuid'], m['recordNumber'], m['header'])
            for m in creds.download_manifests()
        ]

    def __eq__(self, other):
        if isinstance(other, ManifestFile):
            return self.uuid == other.uuid
        return False

    def __hash__(self):
        return self.uuid


class MFUploadable(Uploadable):
    def __init__(self, manifest: ManifestFile, creds: ValidationV2Credentials):
        super().__init__()
        self.manifest = manifest
        self.creds = creds

    @property
    def search_name(self):
        return self.manifest.name

    def __hash__(self):
        return hash(self.manifest.uuid)

    def __eq__(self, other):
        if isinstance(other, MFUploadable):
            return other.manifest.uuid == self.manifest.uuid
        return False


class MFUploadContext(UploadContext):
    def __init__(self, credentials_list: List[ValidationV2Credentials]):
        self.credentials_list = credentials_list
        # initialize this to false, and toggle to true after we prompt user to enter manifest folder
        self.display_missing_files_message = False


class _ManifestFileBatchUploader(BatchFileUploader):

    def _get_file_batches(self):
        for c in self.upload_context.credentials_list:
            manifests = ManifestFile.manifests_from_credentials(c)
            yield [MFUploadable(m, c) for m in manifests]

    def _upload_file(self, file: MFUploadable):
        try:
            creds = file.creds
            creds.upload(str(file.path), file.manifest.s3_destination)
            logger.debug(f'Finished uploading {str(file.path)}')
        except Exception as e:
            logger.error(f'Unexpected error occurred while uploading {file}: {e}')
            logger.error(traceback.format_exc())
            raise UploadError(file, e)

    def _construct_tqdm(self):
        """Use the default tqdm but insert into the UploadContext to use inside _post_batch_hook"""
        progressbar = tqdm(disable=True)
        self.upload_context.progress_bar = progressbar
        return progressbar

    def _post_batch_hook(self, br: BatchResults):
        """Handle missing manifests at the end of each batch. Don't proceed until all manifests from the batch are processed"""
        if br.files_not_found:
            msg = files_not_found_msg(br.files_not_found, br.search_folders)
            if self.exit_on_error:
                exit_error(msg)
            else:
                if br.success and br.files_not_found:
                    # it makes sense to show this message if some files were found while others were not
                    self.upload_context.display_missing_files_message = True

                if self.upload_context.display_missing_files_message:
                    logger.info(msg)
                    new_dir = get_directory_input('Specify the folder containing the manifest files and try again:')
                else:
                    self.upload_context.display_missing_files_message = True
                    new_dir = get_directory_input(
                        'Your data contains manifest files. Specify the folder containing the manifest files:')

                self._upload_batch(br.files_not_found, [new_dir], lambda: self.upload_context.progress_bar.update(1))


class ManifestFileUploader:
    def __init__(self, validation_api: ValidationV2Api, max_threads, exit_on_error=False, hide_progress=False):
        self.api = validation_api
        self.uploader = _ManifestFileBatchUploader(max_threads, exit_on_error, hide_progress)

    def start_upload(self, creds: Union[List[ValidationV2Credentials], ValidationV2Credentials],
                     manifest_dirs: Union[List[PathLike], PathLike]):
        # normalize parameter to list
        if isinstance(creds, ValidationV2Credentials):
            creds = [creds]
        if not manifest_dirs:
            manifest_dirs = [os.getcwd()]
        elif not isinstance(manifest_dirs, list):
            manifest_dirs = [manifest_dirs]

        self.uploader.start_upload(manifest_dirs, MFUploadContext(creds))
