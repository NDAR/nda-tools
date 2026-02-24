import hashlib
import json
import logging
import os
import re
import traceback
from os import PathLike
from pathlib import Path
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

    def _construct_tqdm(self, total: str, description: str):
        """Use the default tqdm but insert into the UploadContext to use inside _post_batch_hook"""
        progressbar = tqdm(disable=True, total=total, desc=description)
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

                self._upload_batch(br.files_not_found, [new_dir])


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


class ManifestRecord:
    def __init__(self, path, name, md5sum=None, size=None):
        self.path = path
        self.name = name
        self.md5sum = md5sum
        self.size = size

    def to_dict(self):
        d = {
            "path": self.path,
            "name": self.name
        }
        if self.md5sum is not None:
            d["md5sum"] = self.md5sum
        if self.size is not None:
            d["size"] = self.size
        return d


def generate_manifests(subject_directory, output_directory, include_regex='.*', exclude_regex=None,
                       include_checksum=False, include_size=False):
    subject_path = Path(subject_directory).resolve()
    output_path = Path(output_directory).resolve()

    include_re = re.compile(include_regex) if include_regex else None
    exclude_re = re.compile(exclude_regex) if exclude_regex else None

    # Scan the subject-directory and create a manifest for each directory found.
    empty_dirs = []
    for entry in os.scandir(subject_path):
        if entry.is_dir(follow_symlinks=False):
            dir_path = Path(entry.path)
            # name of the folder in the subject-directory should be the name of the manifest-file that is created
            manifest_name = entry.name
            records = []

            for root, dirs, files in os.walk(dir_path):
                # Symbolic links should be excluded from the search.
                # os.walk by default does not follow symlinks.

                for file in files:
                    file_path = Path(root) / file
                    if file_path.is_symlink():
                        continue

                    # Relative path calculation
                    # if subject_directory is /a/b and we are looking at /a/b/c/d.txt,
                    # the path should be c/d.txt.
                    rel_path = file_path.relative_to(subject_path)

                    # Regex filtering
                    if exclude_re and exclude_re.search(str(rel_path)):
                        logger.debug(f"Excluding file {str(rel_path)} due to regex exclusion")
                        continue
                    if include_re and not include_re.search(str(rel_path)):
                        logger.debug(f"Excluding file {str(rel_path)} due to regex inclusion")
                        continue

                    md5sum = None
                    if include_checksum:
                        hash_md5 = hashlib.md5()
                        with open(file_path, "rb") as f:
                            for chunk in iter(lambda: f.read(4096), b""):
                                hash_md5.update(chunk)
                        md5sum = hash_md5.hexdigest()

                    size = None
                    if include_size:
                        size = file_path.stat().st_size

                    record = ManifestRecord(str(rel_path), file, md5sum, size)
                    records.append(record.to_dict())
            if not records:
                empty_dirs.append(dir_path)
            else:
                manifest_file = output_path / f"{manifest_name}.json"
                with open(manifest_file, 'w') as f:
                    json.dump({"files": records}, f, indent=2)
                logged_name = f"{str(Path(output_directory) / manifest_name)}.json"
                logger.info(
                    f"Generated {logged_name} for directory {dir_path} containing {len(records)} files")

    if empty_dirs:
        dir_str = '\n'.join([str(d) for d in empty_dirs])
        logger.warning(
            f"No manifests were created for the following directories: \n{dir_str}")
        if exclude_re or include_re:
            logger.warning(
                "If manifests were supposed to be created for these directories, please check your regex patterns."
                "\nHint: you can rerun the command with the '--verbose' option to see which files were included/excluded because of the regular-expression pattern")
