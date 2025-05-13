import enum
import logging
import os
import pathlib
import traceback
from os import PathLike
from typing import List, Callable, Tuple, Union

from tabulate import tabulate

from NDATools.Utils import exit_error, execute_in_threadpool
from NDATools.upload.submission.api import SubmissionPackage, Submission, SubmissionDetails, PackagingStatus, \
    SubmissionStatus
from NDATools.upload.submission.resubmission import build_replacement_package_info
from NDATools.upload.validation.api import ManifestError, ValidationV2
from NDATools.upload.validation.manifests import ManifestFile
from NDATools.upload.validation.v1 import Validation

logger = logging.Logger(__name__)


class ManifestValidationError:
    def __init__(self, manifest: ManifestFile, messages: List[str]):
        self.manifest = manifest
        self.messages = messages


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
    PENDING_MANIFESTS = 'PendingManifests'


class ValidatedFile:
    def __init__(self, file: pathlib.Path, *, v1_resource=None, v2_resource: ValidationV2 = None, v2_creds=None,
                 manifest_errors=None):
        if not manifest_errors:
            manifest_errors = []

        self.file = file
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
                error.messages
            ] for error in self._manifest_errors[:limit]
        ]
        self._preview(rows, ['Row', 'FileName', 'Message'])
        if len(self.manifest_errors) > limit:
            logger.info('\n...and {} more errors'.format(len(self.manifest_errors) - limit))


class NdaSubmission:
    def __init__(self, collection_id: int, title: str, description: str, validated_files: List[ValidatedFile],
                 status: str):
        self.collection_id = collection_id
        self.title = title
        self.description = description
        self.validated_files = validated_files
        self.status = status


class NdaUploadCli:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config):
        self.config = client_config
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

    def submit(self, collection_id: int, title: str, description: str,
               validated_files: List[ValidatedFile], associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Submits data from validated files. A new submission will be created in NDA after this operation succeeds"""
        sub_package = self._build_package(collection_id, title, description, validated_files)
        logger.info('Requesting submission for package: {}'.format(sub_package.submission_package_uuid))
        submission = self.submission_api.create_submission(sub_package.submission_package_uuid)
        logger.info('Submission ID: {}'.format(str(submission.submission_id)))
        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission.submission_id)
        return NdaSubmission(submission.collection.id, submission.dataset_title, submission.dataset_description,
                             validated_files, submission.status)

    def resume(self, submission_id: int, associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Resumes an in-progress submission by uploading any remaining Associated Files."""
        submission = self.submission_api.get_submission(submission_id)
        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission_id)
        return NdaSubmission(submission.collection.id, submission.dataset_title, submission.dataset_description,
                             [], submission.status)

    def replace_submission(self, submission_id: int, validated_files: List[ValidatedFile],
                           associated_file_dirs: List[PathLike] = None) -> NdaSubmission:
        """Replaces the data in a submission with the passed in set of validated_files. Used to correct QA errors"""
        package_id = self._build_replacement_package(submission_id, validated_files)
        logger.info('Requesting submission for package: {}'.format(package_id))
        submission = self.submission_api.replace_submission(submission_id, package_id)
        logger.info("Submission replaced successfully.")
        if submission.status == SubmissionStatus.UPLOADING:
            self._upload_associated_files(submission, associated_file_dirs, resuming_upload=True)
            submission = self.submission_api.get_submission(submission.submission_id)
        return NdaSubmission(submission.collection.id, submission.dataset_title, submission.dataset_description,
                             validated_files, submission.status)

    def validate_v1(self, file_list, threads) -> List[ValidatedFile]:
        """Validates files using the old validation API. Deprecated and will be removed in a future release"""
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hide_progress,
                                thread_num=threads,
                                allow_exit=True)
        validation.validate()
        return [ValidatedFile(v[1], v1_resource=v[0]) for v in validation.responses]

    def validate(self, file_name: Union[List[str], str]) -> List[ValidatedFile]:
        """
        Validates one or multiple files by interacting with a validation API and handling csv uploads,
        and manifest uploads (if necessary), and validation statuses. If a list of files is provided, csv and manifest
        files uploads are performed concurrently using the max-threads setting in the nda configuration.

        Parameters
        ----------
        file_name : Union[List[str], str]
            The name of the file to validate as a string or a list of file names.

        Returns
        -------
        List[ValidatedFile]
            A list of ValidatedFile objects, which include information about the uploaded and validated
            files, associated resources, and any manifest errors encountered during the validation
            process.
        """
        if isinstance(file_name, list):
            if len(file_name) > 1:
                # this method is optimized for multiple files
                return self._validate_multiple(file_name)
            else:
                file_name = file_name[0]

        file = pathlib.Path(file_name)
        creds = self.validation_api.request_upload_credentials(file.name, self.config.scope)
        creds.upload_csv(file)
        resource = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
        if resource.status == ValidationStatus.PENDING_MANIFESTS:
            self.manifests_uploader.upload_manifests(creds, self.config.manifest_path)
            resource = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, True)
            # there must be manifest errors if the status changes from 'PendingManifests' to 'CompleteWithErrors'
            # TODO refactor this and set_manifest_errors into common helper method
            if resource.status == ValidationStatus.COMPLETE_WITH_ERRORS:
                manifests = ManifestFile.manifests_from_credentials(creds)
                # hash manifests by uuid for efficient lookup when creating manifest_errors
                mdict = {m.uuid: m for m in manifests}
                manifest_errors = [ManifestValidationError(mdict[e.uuid], e.errors) for e in
                                   self.validation_api.get_manifest_errors(creds.uuid)]

                return [ValidatedFile(file, v2_resource=resource, v2_creds=creds, manifest_errors=manifest_errors)]

        return [ValidatedFile(file, v2_resource=resource, v2_creds=creds)]

    def _validate_multiple(self, files: List[str]) -> List[ValidatedFile]:
        """
        Validates multiple data-structure files in a way that minimizes the amount of time user needs to wait for
        validation of all files to complete. Workflow is:
            1. validate data-structure files - after this step all statuses of files are Complete or PendingManifests
            2. upload manifests - manifests are uploaded for all files from the previous step with status 'PendingManifests'
            3. wait validation complete - blocking step until all files from step 1 with status 'PendingManifests' transition to complete

        Workflow is structured this way to minimize the amount of time to perform validation on a large number of files, with potentially multiple
        files containing manifest elements.

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
            """ Private class used to keep track of the state of requests. Some requests complete as soon as they are
             uploaded, while others require manifests to be validated after the csv is validated."""

            def __init__(self, file: pathlib.Path, v2_resource=None, v2_creds=None):
                self.file = file
                self.v2_resource = v2_resource
                self.v2_creds = v2_creds
                self.waiting_manifest_upload = v2_resource['status'] == ValidationStatus.PENDING_MANIFESTS
                self.uuid = v2_resource['uuid']
                self.manifest_errors = None

            def set_manifest_errors(self, errors: List[ManifestError]):
                manifests = ManifestFile.manifests_from_credentials(self.v2_creds)
                mdict = {m.uuid: m for m in manifests}
                self.manifest_errors = [ManifestValidationError(mdict[e.uuid], e.errors) for e in errors]

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
            results: List[InitiatedV2Request] = \
                self._execute_in_threadpool(initiate_v2_request, [(pathlib.Path(x),) for x in files])

            manifest_requests = [r for r in results if r.waiting_manifest_upload]
            if manifest_requests:
                self.manifests_uploader.upload_manifests([m.v2_creds for m in manifest_requests], self.manifest_dir)
                logger.info(f'Waiting for validation of {len(manifest_requests)} files to complete....')

            def wait_and_update(request: InitiatedV2Request):
                resource = \
                    self.validation_api.wait_validation_complete(request.uuid, self.config.validation_timeout, True)
                # update the resource on the owning request object
                request.v2_resource = resource
                if resource['status'] == ValidationStatus.COMPLETE_WITH_ERRORS:
                    # update the manifest errors
                    request.set_manifest_errors(self.validation_api.get_manifest_errors(request.uuid))

            self._execute_in_threadpool(wait_and_update, [(r,) for r in manifest_requests])

            # convert initiated v2 requests to validated files
            return [
                ValidatedFile(x.file, v2_resource=x.v2_resource, v2_creds=x.v2_creds, manifest_errors=x.manifest_errors)
                for x in results
            ]
        except Exception as e:
            logger.error(f'An unexpected error occurred: {e}')
            logger.error(traceback.format_exc())
            exit_error()
            exit(1)

    def _execute_in_threadpool(self, func: Callable, args: List[Tuple]):
        return execute_in_threadpool(func, args, max_workers=self.config.worker_threads,
                                     disable_tqdm=self.config.hide_progress)

    def _upload_associated_files(self, submission, associated_file_dirs: List[PathLike],
                                 resuming_upload=False) -> Submission:
        if not associated_file_dirs:
            associated_file_dirs = [os.getcwd()]
        self.associated_files_uploader.start_upload(submission, associated_file_dirs, resuming_upload)
        return self.submission_api.get_submission(submission.submission_id)

    def _build_replacement_package(self, submission_id: int, validated_files: List[ValidatedFile]) -> SubmissionPackage:
        """Builds a submissionPackage for a replacement submission"""
        submission: Submission = self.submission_api.get_submission(submission_id)
        submission_details: SubmissionDetails = self.submission_api.get_submission_details(submission.submission_id)
        pkg = build_replacement_package_info(validated_files, submission, submission_details)
        return self._build_package(pkg.collection_id, pkg.title, pkg.description, pkg.validation_uuids,
                                   pkg.submission_id)

    def _build_package(self, collection_id: int, title: str, description: str,
                       validated_files: List[ValidatedFile], replacement_submission: int = None) -> SubmissionPackage:
        """Builds a submissionPackage using the passed in parameters as the values for the payload"""
        validation_uuids = [v.uuid for v in validated_files]
        package = self.submission_package_api.build_package(collection_id, title, description, validation_uuids,
                                                            replacement_submission)
        if package.status == PackagingStatus.PROCESSING:
            package = self.submission_package_api.wait_package_complete(package.submission_package_uuid)

        # print package info to console
        logger.info('\n\nPackage Information:')
        logger.info('validation results: {}'.format(validation_uuids))
        logger.info('submission_package_uuid: {}'.format(package.submission_package_uuid))
        logger.info('created date: {}'.format(package.created_date))
        logger.info('expiration date: {}'.format(package.expiration_date))

        return package
