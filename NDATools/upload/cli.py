import enum
import logging
import os
import pathlib
from collections import namedtuple
from os import PathLike
from typing import List, Callable, Tuple, Union

from tabulate import tabulate

from NDATools.Utils import tqdm_thread_map
from NDATools.upload.submission.api import SubmissionPackage, Submission, SubmissionDetails, PackagingStatus, \
    SubmissionStatus
from NDATools.upload.submission.resubmission import build_replacement_package_info
from NDATools.upload.validation.api import ValidationV2
from NDATools.upload.validation.manifests import ManifestFile
from NDATools.upload.validation.v1 import Validation

logger = logging.getLogger(__name__)


class ManifestValidationError:
    def __init__(self, manifest: ManifestFile, message: str):
        self.manifest = manifest
        self.message = message


class ValidationError:
    def __init__(self, record_number, column_name, message, err_code):
        self.record_number = record_number
        self.column_name = column_name
        self.message = message
        self.err_code = err_code


class ValidationStatus(str, enum.Enum):
    PROCESSING = 'Processing'
    COMPLETE = 'Complete'
    COMPLETE_WITH_WARNINGS = 'CompleteWithWarnings'
    COMPLETE_WITH_ERRORS = 'CompleteWithErrors'
    SYSTEM_ERROR = 'SystemError'
    PENDING_MANIFESTS = 'PendingManifestFiles'


class ValidatedFile:
    def __init__(self, file: PathLike, *, v1_resource=None, v2_resource: ValidationV2 = None, v2_creds=None,
                 manifest_errors=None):
        if not manifest_errors:
            manifest_errors = []

        self.file = pathlib.Path(file)
        assert v1_resource or v2_resource, "v1_resource or v2_resource must be specified"
        if v2_resource:
            self.status = ValidationStatus(v2_resource.status)
            self.uuid = v2_resource.uuid
            self.short_name = v2_resource.short_name
            self.row_count = v2_resource.rows
            self._errors = None
            self._warnings = None
            self._associated_files = None
            self._manifest_errors = manifest_errors
            self._v2_creds = v2_creds
        elif v1_resource:
            self.status = ValidationStatus(v1_resource['status'])
            self.uuid = v1_resource['id']
            self.short_name = v1_resource['short_name']
            self.row_count = v1_resource['rows']
            self._errors = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message'), err_type) for
                            err_type, errors
                            in v1_resource['errors'].items() for i in errors]
            self._warnings = [ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message'), err_type)
                              for err_type, errors
                              in v1_resource['warnings'].items() for i in errors]
            self._associated_files = v1_resource['associated_file_paths']
            self._manifest_errors = []

    def __hash__(self):
        return hash(self.file) + hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, ValidatedFile):
            return other.file == self.file and other.uuid == self.uuid
        return False

    @property
    def warnings(self) -> List[ValidationError]:
        if self._warnings is None:
            self._warnings = [ValidationError(m.get('recordNumber'), m.get('columnName'), m.get('message'), err_type)
                              for err_type, errors in self._v2_creds.download_warnings().items() for m in errors]
        return self._warnings

    @property
    def errors(self) -> List[ValidationError]:
        if self._errors is None:
            self._errors = [ValidationError(m.get('recordNumber'), m.get('columnName'), m.get('message'), err_type)
                            for err_type, errors in self._v2_creds.download_errors().items() for m in errors]
        return self._errors

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

    def _preview(self, rows, headers):
        if not rows:
            return None
        logger.info('')
        table = tabulate(rows, headers=headers)
        logger.info(table)
        logger.info('')
        return table

    def preview_validation_errors(self, limit=10):
        logger.info('\nErrors found in {}:'.format(self.file.name))
        rows = [
            [
                error.record_number,
                error.column_name,
                error.message
            ] for error in self.errors[:limit]
        ]
        self._preview(rows, ['Row', 'Column', 'Message'])
        if len(self.errors) > limit:
            logger.info('\n...and {} more errors'.format(len(self.errors) - limit))

    def preview_manifest_errors(self, limit=10):
        logger.info('\nManifest Errors found in {}:'.format(self.file.name))
        rows = [
            [
                error.manifest.record_number,
                error.manifest.name,
                error.message
            ] for error in self._manifest_errors[:limit]
        ]
        self._preview(rows, ['Row', 'FileName', 'Message'])
        if len(self.manifest_errors) > limit:
            logger.info('\n...and {} more errors'.format(len(self.manifest_errors) - limit))


class NdaSubmission:
    def __init__(self, id: int, collection_id: int, title: str, description: str, validated_files: List[ValidatedFile],
                 status: SubmissionStatus):
        self.id = id
        self.collection_id = collection_id
        self.title = title
        self.description = description
        self.validated_files = validated_files
        self.status = status


class NdaUploadCli:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config):
        self.config = client_config
        ...

    @property
    def submission_package_api(self):
        return self.config.submission_package_api

    @property
    def submission_api(self):
        return self.config.submission_api

    @property
    def validation_api(self):
        return self.config.validation_api

    @property
    def collection_api(self):
        return self.config.collection_api

    @property
    def manifests_uploader(self):
        return self.config.manifests_uploader

    @property
    def associated_files_uploader(self):
        return self.config.associated_files_uploader

    def submit(self, validated_files: List[ValidatedFile], collection_id: int, title: str, description: str,
               associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Submits data from validated files. A new submission will be created in NDA after this operation succeeds"""
        package = self._build_package(collection_id, title, description, [v.uuid for v in validated_files])
        logger.info('Requesting submission for package: {}'.format(package.submission_package_uuid))
        submission = self.submission_api.create_submission(package.submission_package_uuid)
        # print package info to console
        logger.info('')
        logger.info('Submission ID: {}'.format(str(submission.submission_id)))
        logger.info('Files: {}'.format(' '.join([v.file.name for v in validated_files])))
        logger.info('Collection ID: {}'.format(collection_id))
        logger.info('Name: {}'.format(title))
        logger.info('Description: {}'.format(description))
        logger.info('')

        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission.submission_id)
        return NdaSubmission(submission.submission_id, submission.collection.id, submission.dataset_title,
                             submission.dataset_description, validated_files, submission.status)

    def resume(self, submission_id: int, associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Resumes an in-progress submission by uploading any remaining Associated Files."""
        submission = self.submission_api.get_submission(submission_id)
        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission_id)
        return NdaSubmission(submission.submission_id, submission.collection.id, submission.dataset_title,
                             submission.dataset_description, [], submission.status)

    def replace_submission(self, submission_id: int, validated_files: List[ValidatedFile],
                           associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Replaces the data in a submission with the passed in set of validated_files. Used to correct QA errors"""
        package = self._build_replacement_package(submission_id, validated_files)
        logger.info('Requesting submission for package: {}'.format(package.submission_package_uuid))
        submission = self.submission_api.replace_submission(submission_id, package.submission_package_uuid)
        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission.submission_id)
        return NdaSubmission(submission.submission_id, submission.collection.id, submission.dataset_title,
                             submission.dataset_description, validated_files, submission.status)

    def validate_v1(self, file_list, threads) -> List[ValidatedFile]:
        """Validates files using the old validation API. Deprecated and will be removed in a future release"""
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hide_progress,
                                thread_num=threads,
                                allow_exit=True)
        validation.validate()
        return [ValidatedFile(v[1], v1_resource=v[0]) for v in validation.responses]

    def validate(self, file_names: Union[List[PathLike], PathLike], manifests_dir: List[PathLike] = None) -> List[
        ValidatedFile]:
        """
        Validates one or multiple files by interacting with a validation API and handling csv uploads,
        and manifest uploads (if necessary), and validation statuses. If a list of files is provided, csv and manifest
        files uploads are performed concurrently using the max-threads setting in the nda configuration.

        Parameters
        ----------
        file_names : Union[List[PathLike], PathLike]
            The path to file(s) to validate

        manifests_dir: PathLike
            The directory to initially search for manifests. If the --force flag is not provided, the program
            will prompt the user for another directory to search if one or more manifests are not found

        Returns
        -------
        List[ValidatedFile]
            A list of ValidatedFile objects, which contains the information generated by the validation process, including
            errors and warnings
        """
        if not isinstance(file_names, list):
            file_names = [file_names]

        # named tuple validation_v2_request is used to keep track of the state of each request
        validation_v2_request = namedtuple('ValidationV2Request',
                                           ['file', 'creds', 'resource'])

        def initiate_request(f_name):
            creds = self.validation_api.request_upload_credentials(f_name, self.config.scope)
            creds.upload_csv(f_name)
            resource = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
            return validation_v2_request(f_name, creds, resource)

        requests: List[validation_v2_request] = list(self._tqdm_thread_map(initiate_request, file_names))

        validated_files = [ValidatedFile(req.file, v2_resource=req.resource, v2_creds=req.creds) for req in
                           requests if req.resource.status != ValidationStatus.PENDING_MANIFESTS]

        # return early if all files completed validation
        if len(validated_files) == len(file_names):
            return validated_files

        # Process requests with a status of 'PendingManifests' by uploading manifest files and waiting for status to change
        manifest_requests = [m for m in requests if m.resource.status == ValidationStatus.PENDING_MANIFESTS]
        logger.info(f'Uploading manifests from {len(manifest_requests)} files')
        self.manifests_uploader.start_upload([m.creds for m in manifest_requests], manifests_dir)
        logger.info(f'Waiting for {len(manifest_requests)} files to finish validation')

        def wait_manifest_validation_complete(req: validation_v2_request):
            resource = self.validation_api.wait_validation_complete(req.creds.uuid,
                                                                    self.config.validation_timeout,
                                                                    True)
            # there must be manifest errors if the status changes from 'PendingManifests' to 'CompleteWithErrors'
            if resource.status == ValidationStatus.COMPLETE_WITH_ERRORS:
                manifests = ManifestFile.manifests_from_credentials(req.creds)
                # hash manifests by uuid for efficient lookup when creating manifest_errors
                mdict = {m.uuid: m for m in manifests}
                manifest_errors = [ManifestValidationError(mdict[e.uuid], err) for e in
                                   self.validation_api.get_manifest_errors(req.creds.uuid) for err in e.errors]
                return ValidatedFile(req.file, v2_resource=resource, v2_creds=req.creds,
                                     manifest_errors=manifest_errors)
            else:
                return ValidatedFile(req.file, v2_resource=resource, v2_creds=req.creds)

        validated_files.extend(list(
            self._tqdm_thread_map(wait_manifest_validation_complete, manifest_requests)))
        return validated_files

    def _tqdm_thread_map(self, func: Callable, args: List[Tuple]):
        """Returns an iterator. See https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.map for more information """
        return tqdm_thread_map(func, args, max_workers=self.config.worker_threads,
                               disable_tqdm=self.config.hide_progress)

    def _upload_associated_files(self, submission: Submission, associated_file_dirs: List[PathLike],
                                 resuming_upload=False) -> None:
        if not associated_file_dirs:
            associated_file_dirs = [os.getcwd()]
        logger.info('Preparing to upload associated files.')
        self.associated_files_uploader.start_upload(submission, associated_file_dirs, resuming_upload)

    def _build_replacement_package(self, submission_id: int, validated_files: List[ValidatedFile]) -> SubmissionPackage:
        """Builds a submissionPackage for a replacement submission"""
        submission: Submission = self.submission_api.get_submission(submission_id)
        submission_details: SubmissionDetails = self.submission_api.get_submission_details(submission.submission_id)
        pkg = build_replacement_package_info(validated_files, submission, submission_details)
        return self._build_package(pkg.collection_id, pkg.title, pkg.description, pkg.validation_uuids,
                                   pkg.submission_id)

    def _build_package(self, collection_id: int, title: str, description: str,
                       validation_uuids: List[str], replacement_submission: int = None) -> SubmissionPackage:
        """Builds a submissionPackage using the passed in parameters as the values for the payload"""
        package = self.submission_package_api.build_package(collection_id, title, description, validation_uuids,
                                                            replacement_submission)
        if package.status == PackagingStatus.PROCESSING:
            package = self.submission_package_api.wait_package_complete(package.submission_package_uuid)

        return package
