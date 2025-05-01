import enum
import logging
import os
import pathlib
import traceback
from typing import List, Callable, Tuple

from tabulate import tabulate
from tqdm.contrib.concurrent import thread_map

from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import exit_error
from NDATools.upload.validation.v1 import Validation

logger = logging.Logger(__name__)


class NdaSubmission:
    pass


class ManifestFile:
    def __init__(self, name: str, s3_destination: str, uuid: str, record_number: int, validated_file):
        self.name = name
        self.s3_destination = s3_destination
        self.uuid = uuid
        self.record_number = record_number
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

    @staticmethod
    def from_credentials(creds):
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


class ManifestValidationError():
    def __init__(self, manifest: ManifestFile, messages: List[str]):
        self.manifest = manifest
        self.messages = messages


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
    def __init__(self, file: pathlib.Path, *, v1_resource=None, v2_resource=None, v2_creds=None, manifests=None,
                 manifest_errors=None):
        self.file = file
        assert v1_resource or v2_resource, "v1_resource or v2_resource must be specified"
        if v2_resource:
            self.status = ValidationStatus(v2_resource.status)
            self.uuid = v2_resource.uuid
            self._errors = None
            self._warnings = None
            self._manifests = manifests
            self._associated_files = None
            self._manifest_errors = manifest_errors
            self._v2_creds = v2_creds
        elif v1_resource:
            self.status = ValidationStatus(v1_resource['status'])
            self.uuid = v1_resource['id']
            self._errors = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')) for err_type
                            in v1_resource['errors'].values() for i in err_type]
            self._warnings = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')) for err_type
                              in v1_resource['warnings'].values() for i in err_type]
            self._manifests = [ManifestFile(m['localFileName'], m['s3Destination'], m['uuid'], m['recordNumber'], self)
                               for m in v1_resource['manifests']]
            self._associated_files = v1_resource['associated_file_paths']
            self._manifest_errors = None

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
    def manifests(self) -> List[ManifestFile]:
        if self._manifests is None:
            self._manifests = [ManifestFile(m['localFileName'], m['s3Destination'], m['uuid'], m['recordNumber'], self)
                               for m in self._v2_creds.download_manifests()['manifests']]
        return self._manifests

    @property
    def associated_file(self) -> List[str]:
        if self._associated_files is None:
            self._associated_files = [m['clientFilePath'] for m in
                                      self._v2_creds.download_associated_files()['associatedFiles']]
        return self._associated_files

    @property
    def manifest_errors(self) -> List[ManifestValidationError]:
        return self._manifest_errors

    def has_manifest_errors(self):
        return self.status == ValidationStatus.COMPLETE_WITH_ERRORS and not self.errors

    def is_valid(self):
        return not self.has_errors()

    def is_invalid(self):
        return not self.is_valid()

    def system_error(self):
        return self.status == ValidationStatus.SYSTEM_ERROR

    def has_warnings(self):
        return self.status == ValidationStatus.COMPLETE_WITH_WARNINGS

    def has_errors(self):
        return self.status == ValidationStatus.COMPLETE_WITH_ERRORS

    def waiting_manifest_upload(self):
        return self.status == ValidationStatus.PENDING_MANIFESTS

    def preview_validation_errors(self, limit=10):
        table_list = []
        logger.info('\nErrors found in {}:'.format(self.file.name))
        rows = [
            [
                error.record_number,
                error.column_name,
                error.message
            ] for error in self.errors[:limit]
        ]
        if rows:
            logger.info('')
            table = tabulate(rows, headers=['Row', 'Column', 'Message'])
            table_list.append(table)
            logger.info(table)
            logger.info('')
        if len(self.errors) > limit:
            logger.info('\n...and {} more errors'.format(len(self.errors) - limit))
        return table_list

    def preview_manifest_errors(self, limit=10):
        table_list = []
        logger.info('\nManifest Errors found in {}:'.format(self.file.name))
        rows = [
            [
                error.record_number,
                error.column_name,
                error.message
            ] for error in self.get_manifest_errors(limit)
        ]

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
        # add warning if more than 1 manifest dir was detected. in later versions of the tool, we are only going to allow users to specify one manifest dir
        if isinstance(self.config.manifest_path, list):
            if len(self.config.manifest_path) > 1:
                logger.warning(
                    f'Found multiple manifest directories: {self.config.manifest_path}. Only the first one ({self.config.manifest_path[0]}) will be used.')
            self.manifest_dir = self.config.manifest_path[0]
        else:
            # should be NoneType
            self.manifest_dir = self.config.manifest_path
        ...

    def submit(self, collection_id: int, title: str, description: str, ) -> NdaSubmission:
        raise NotImplementedError()

    def resume(self, submission_id: int, associated_file_dir: pathlib.Path) -> NdaSubmission:
        raise NotImplementedError()

    def resubmit(self, submission_id: int, validated_files: List[ValidatedFile]) -> NdaSubmission:
        raise NotImplementedError()

    def validate_v1(self, file_list, threads) -> List[ValidatedFile]:
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hideProgress,
                                thread_num=threads,
                                allow_exit=True)
        validation.validate()
        return [ValidatedFile(v[1], v1_resource=v[0]) for v in validation.responses]

    def validate(self, file_name: str) -> ValidatedFile:
        """
        Validates a single file

        Args:
            file_name (str): The path to the file to be validated.

        Returns:
            ValidatedFile: A `ValidatedFile` object representing the validation results for the input file.

        Raises:
            Exception: If there are unexpected issues during the validation or upload process.
        """
        assert isinstance(file_name, str), "file_name must be a string"
        file = pathlib.Path(file_name)
        creds = self.validation_api.request_upload_credentials(file.name, self.config.scope)
        creds.upload_csv(file)
        resource = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
        if resource['status'] == ValidationStatus.PENDING_MANIFESTS:
            manifests = creds.download_manifests()
            self.uploader.upload_manifests(manifests, creds.uuid)

        return ValidatedFile(file, v2_resource=resource, v2_creds=creds)

    def validate_multiple(self, files: List[str]) -> List[ValidatedFile]:
        """
        Validates the provided data-structure files

        Args:
            files (List[str]): A single file path or a list of file paths to validate.

        Returns:
            List[ValidatedFile]: A list of `ValidatedFile` objects, each representing validation results
            for the corresponding input file.

        Raises:
            Exception: If there are unexpected issues during the validation or upload process.
        """
        assert isinstance(files, list), "files must be a list of strings"

        class InitiatedV2Request:
            ''' Private class used to keep track of the state of requests. Some requests complete as soon as they are
            uploaded while others require manifests to be validated after the csv is validated.'''

            def __init__(self, file: pathlib.Path, v2_resource=None, v2_creds=None):
                self.file = file
                self.v2_resource = v2_resource
                self.v2_creds = v2_creds
                self.waiting_manifest_upload = v2_resource['status'] == ValidationStatus.PENDING_MANIFESTS
                self.manifests = [
                    ManifestFile(m['localFileName'], m['s3Destination'], m['uuid'], m['recordNumber'], self)
                    for m in self.v2_creds.download_manifests()['manifests']]
                self.uuid = v2_resource['uuid']
                self.manifest_errors = None

        def initiate_v2_request(file: pathlib.Path) -> InitiatedV2Request:
            """ Uploads CSV file and waits until validation is complete. Does not upload manifest files if manifests are present"""
            creds = self.validation_api.request_upload_credentials(file.name, self.config.scope)
            creds.upload_csv(file)
            resource = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
            return InitiatedV2Request(file, v2_resource=resource, v2_creds=creds)

        """ 
        Main validation block
        validates all files first, then uploads manifests in order to match the behavior of prev versions of the client. 
        """
        try:
            results: List[InitiatedV2Request] = []
            self._execute_in_threadpool(initiate_v2_request, [(pathlib.Path(x),) for x in files])

            manifest_csvs = [r for r in results if r.waiting_manifest_upload]
            if manifest_csvs:
                manifests = [manifest for file in manifest_csvs for manifest in file.manifests]
                self.uploader.upload_manifests(manifests, self.manifest_dir)
                print(f'\nManifests uploaded. Waiting for validation of manifests to complete....')

            def wait_and_update(request: InitiatedV2Request):
                resource = self.validation_api.wait_validation_complete(request.uuid,
                                                                        self.config.validation_timeout, True)
                # update the resource on the owning request object
                request.v2_resource = resource
                if resource['status'] == ValidationStatus.COMPLETE_WITH_ERRORS:
                    # get manifest errors
                    request.manifest_errors = request.v2_creds.get_manifest_errors()

                self._execute_in_threadpool(wait_and_update, [(r,) for r in requests])

            # convert initiated v2 requests to validated files
            return [ValidatedFile(x.file, v2_resource=x.v2_resource, v2_creds=x.v2_creds,
                                  manifests=x.manifests, manifest_errors=x.manifest_errors) for x in results]
        except Exception as e:
            logger.error(f'An unexpected error occurred: {e}')
            logger.error(traceback.format_exc())
            exit_error()
            exit(1)

    def _execute_in_threadpool(self, func: Callable, args: List[Tuple], disable_tqdm: bool = False):
        return thread_map(func, args,
                          max_workers=self.config.workerThreads,
                          total=len(args),
                          disable=self.config.hideProgress or disable_tqdm)
        # manual implementation - keeping for now until decidedly not needed
        # results = []
        # with tqdm(total=len(args), disable=self.config.hideProgress or disable_tqdm) as progress_bar, \
        #         ThreadPoolExecutor(max_workers=self.config.workerThreads) as executor:
        #     # executor.map seems to block whereas executor.submit doesnt...
        #     futures = list(map(lambda arg: executor.submit(func, *arg), args))
        #     for result in concurrent.futures.as_completed(futures):
        #         r = result.result()
        #         results.append(r)
        #         if cb:
        #             cb(r)
        #         progress_bar.update(1)
        # return results
