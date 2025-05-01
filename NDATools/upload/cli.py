import concurrent
import enum
import logging
import os
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List

from tabulate import tabulate
from tqdm import tqdm

from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import exit_error
from NDATools.upload.validation.v1 import Validation

logger = logging.Logger(__name__)


class NdaSubmission:
    pass


class ManifestFile:
    def __init__(self, name: str, s3_destination: str, validated_file):
        self.name = name
        self.s3_destination = s3_destination
        self.validated_file = validated_file

    def upload(self, manifest_dir):
        try:
            self.validated_file.upload_manifest(self, manifest_dir)
        except Exception as e:
            if not isinstance(e, ManifestNotFoundError):
                logger.error(f'Unexpected error occurred while uploading {self.name}: {e}')
                logger.error(traceback.format_exc())
                raise ManifestUploadError(self, e)
            else:
                logger.debug(
                    f'Could not find manifest {self.name} from file {self.validated_file.file.name} in {manifest_dir}')
                raise e


class ManifestUploadError(Exception):
    def __init__(self, manifest: ManifestFile, unexpected_error: Exception = None):
        self.manifest = manifest
        self.error = unexpected_error


class ManifestNotFoundError(ManifestUploadError):
    ...


class ValidationError:
    def __init__(self, record_number, column_name, message):
        self.record_number = record_number
        self.column_name = column_name
        self.message = message


class ValidationStatus(str, enum.Enum):
    PROCESSING = 'Processing'
    COMPLETE = 'Complete'
    COMPLETE_WITH_WARNINGS = 'CompleteWithWarnings'
    COMPLETE_WITH_ERRORS = 'CompleteWithErrors'
    SYSTEM_ERROR = 'SystemError'
    PENDING_MANIFESTS = 'PendingManifestFiles'


class ValidatedFile:
    def __init__(self, file: pathlib.Path, *, v1_resource=None, v2_resource=None, v2_creds=None):
        self.file = file
        assert v1_resource or v2_resource, "v1_resource or v2_resource must be specified"
        if v2_resource:
            self.status = ValidationStatus(v2_resource.status)
            self.uuid = v2_resource.uuid
            self._errors = None
            self._warnings = None
            self._manifests = None
            self._associated_files = None
            self._v2_creds = v2_creds
        elif v1_resource:
            self.status = ValidationStatus(v1_resource['status'])
            self.uuid = v1_resource['id']
            self._errors = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')) for err_type
                            in v1_resource['errors'].values() for i in err_type]
            self._warnings = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')) for err_type
                              in v1_resource['warnings'].values() for i in err_type]
            self._manifests = [ManifestFile(m['localFileName'], m['s3Destination'], self) for m in
                               v1_resource['manifests']]
            self._associated_files = v1_resource['associated_file_paths']

    def __hash__(self):
        return hash(self.file) + hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, ValidatedFile):
            return other.file == self.file and other.uuid == self.uuid
        return False

    @property
    def warnings(self) -> List[ValidationError]:
        if self._warnings is None:
            self._warnings = [ValidationError(m.get('recordNumber'), m.get('columnName'), m.get('message')) for err_type
                              in self._v2_creds.download_warnings().values() for m in err_type]
        return self._warnings

    @property
    def errors(self) -> List[ValidationError]:
        if self._errors is None:
            self._errors = [ValidationError(m.get('recordNumber'), m.get('columnName'), m.get('message')) for err_type
                            in self._v2_creds.download_errors().values() for m in err_type]
        return self._errors

    @property
    def manifests(self):
        if self._manifests is None:
            self._manifests = [ManifestFile(m['localFileName'], m['s3Destination'], self) for m in
                               self._v2_creds.download_manifests()['manifests']]
        return self._manifests

    @property
    def associated_file(self):
        if self._associated_files is None:
            self._associated_files = [m['clientFilePath'] for m in
                                      self._v2_creds.download_associated_files()['associatedFiles']]
        return self._associated_files

    def has_manifest_errors(self):
        pass

    def is_valid(self):
        return not self.has_errors()

    def is_invalid(self):
        return not self.is_valid()

    def system_error(self):
        return self.status == ValidationStatus.SYSTEM_ERROR

    def has_warnings(self):
        # return 'warnings' in str(self.status).lower()
        return self.status == ValidationStatus.COMPLETE_WITH_WARNINGS

    def has_errors(self):
        # return 'errors' in str(self.status).lower()
        return self.status == ValidationStatus.COMPLETE_WITH_ERRORS

    def waiting_manifest_upload(self):
        # return 'pending' in str(self.status).lower()
        return self.status == ValidationStatus.PENDING_MANIFESTS

    def preview_validation_errors(self, limit=10):
        table_list = []
        logger.info('\nErrors found in {}:'.format(self.file.name))
        errors = self.errors
        rows = [
            [
                error.record_number,
                error.column_name,
                error.message
            ] for error in errors[:limit]
        ]
        if rows:
            logger.info('')
            table = tabulate(rows, headers=['Row', 'Column', 'Message'])
            table_list.append(table)
            logger.info(table)
            logger.info('')
        if len(errors) > limit:
            logger.info('\n...and {} more errors'.format(len(errors) - limit))
        return table_list

    def show_manifest_errors(self):
        raise NotImplementedError()

    def upload_manifest(self, manifest: ManifestFile, manifest_dir: str):
        assert self._v2_creds
        local_file = pathlib.Path(os.path.join(manifest_dir, manifest.name))
        if not local_file.exists():
            raise ManifestNotFoundError(manifest)
        self._v2_creds.upload(str(local_file), manifest.s3_destination)


class NdaUploadCli:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config: ClientConfiguration):
        self.config = client_config
        self.validation_api = self.config.validation_api
        self.uploader = self.config.manifests_uploader
        ...

    def submit(self, collection_id) -> NdaSubmission:
        raise NotImplementedError()

    def resume(self, collection_id) -> NdaSubmission:
        raise NotImplementedError()

    def resubmit(self, submission_id: int, validated_files: List[ValidatedFile]) -> NdaSubmission:
        raise NotImplementedError()

    def validate_v1(self, file_list, threads) -> List[ValidatedFile]:
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hideProgress,
                                thread_num=threads,
                                allow_exit=True)
        validation.validate()
        return [ValidatedFile(v[1], v1_resource=v[0]) for v in validation.responses]

    def validate(self, files: List[str]) -> List[ValidatedFile]:
        try:
            # validate the files first, and then upload the manifests in order to match the behavior of prev versions of the client
            results: List[ValidatedFile] = []

            with tqdm(total=len(files), disable=self.config.hideProgress) as progress_bar, \
                    ThreadPoolExecutor(max_workers=self.config.workerThreads) as executor:
                # executor.map seems to block whereas executor.submit doesnt...
                futures = list(map(lambda x: executor.submit(self._validate_file, x, False), files))
                for result in concurrent.futures.as_completed(futures):
                    results.append(result.result())
                    progress_bar.update(1)

            manifest_csvs = [r for r in results if r.waiting_manifest_upload()]
            if manifest_csvs:
                self._upload_manifests(*manifest_csvs)
            return results

        except Exception as e:
            logger.error(f'An unexpected error occurred: {e}')
            logger.error(traceback.format_exc())
            exit_error()
            exit(1)

    def _validate_file(self, file_name: str, upload_manifests=True) -> ValidatedFile:
        file = pathlib.Path(file_name)
        creds = self.validation_api.request_upload_credentials(file.name, self.config.scope)
        creds.upload_csv(file)
        validation_v2 = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
        validated_file = ValidatedFile(file, v2_resource=validation_v2, v2_creds=creds)

        # upload manifests if the file has any...
        if upload_manifests and validated_file.waiting_manifest_upload():
            self._upload_manifests(validated_file)
        return validated_file

    def _upload_manifests(self, *files):
        # add warning if more than 1 manifest dir was detected. in later versions of the tool, we are only going to allow users to specify one manifest dir
        if isinstance(self.config.manifest_path, list):
            if len(self.config.manifest_path) > 1:
                logger.warning(
                    f'Found multiple manifest directories: {self.config.manifest_path}. Only the first one ({self.config.manifest_path[0]}) will be used.')
            manifest_dir = self.config.manifest_path[0]
        else:
            # should be NoneType
            manifest_dir = self.config.manifest_path
        manifests = [manifest for file in files for manifest in file.manifests]
        self.uploader.upload_manifests(manifests, manifest_dir)
        print(f'\nManifests uploaded. Waiting for validation of manifests to complete....')

        with tqdm(total=len(files), disable=self.config.hideProgress) as progress_bar, \
                ThreadPoolExecutor(max_workers=self.config.workerThreads) as executor:
            # executor.map seems to block whereas executor.submit doesn't...
            futures = list(
                map(lambda x: executor.submit(
                    self.validation_api.wait_validation_complete, x.uuid, self.config.validation_timeout, True),
                    files))
            for f in concurrent.futures.as_completed(futures):
                r = f.result()
                logger.debug(f'Validation status for {r.uuid} updated to {r.status}')
                progress_bar.update(1)
