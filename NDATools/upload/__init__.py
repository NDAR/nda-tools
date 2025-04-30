import concurrent
import enum
import logging
import os
import pathlib
import random
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List

from tabulate import tabulate
from tqdm import tqdm

import NDATools
from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import get_request, exit_error
from NDATools.upload.submission.resubmission import check_replacement_authorized
from NDATools.upload.validation.results_writer import ResultsWriterFactory
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
            self._errors = v1_resource['errors']
            self._warnings = v1_resource['warnings']
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
    def errors(self) -> List[ValidationError]:
        if self._errors:
            return list(map(lambda i: ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')),
                            self._errors.items()))
        else:
            raise NotImplementedError()

    @property
    def manifests(self):
        if not self._manifests:
            self._manifests = [ManifestFile(m['local_file_name'], m['s3_destination']) for m in
                               self._v2_creds.download_manifests()]
        return self._manifests

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


class NdaUpload:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config: ClientConfiguration):
        self.config = client_config
        self.validation_api = self.config.validation_api
        self.uploader = self.config.manifests_uploader
        ...

    def submit(self, collection_id) -> NdaSubmission:
        pass

    def resubmit(self, submission_id: int, validated_files: List[ValidatedFile]) -> NdaSubmission:
        pass

    def upload_associated_files(self, submission: NdaSubmission):
        pass

    def validate_files_v1(self, file_list, threads) -> List[ValidatedFile]:
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hideProgress,
                                thread_num=threads,
                                allow_exit=True)
        logger.info('\nValidating files...')
        validation.validate()
        return [ValidatedFile(v[1], v1_resource=v[0]) for v in validation.responses]

    def validate_files(self, files: List[str]) -> List[ValidatedFile]:
        logger.info(f'\nValidating {len(files)} files...')
        try:
            # validate the files first, and then upload the manifests in order to match the behavior of prev versions of the client
            results: List[ValidatedFile] = []

            with tqdm(total=len(files), disable=self.config.hideProgress) as progress_bar, \
                    ThreadPoolExecutor(max_workers=self.config.workerThreads) as executor:
                # executor.map seems to block whereas executor.submit doesnt...
                futures = list(map(lambda x: executor.submit(self.validate_file, x, False), files))
                for result in concurrent.futures.as_completed(futures):
                    results.append(result.result())
                    progress_bar.update(1)

            manifest_csvs = [r for r in results if r.waiting_manifest_upload()]
            if manifest_csvs:
                self.upload_manifests(*manifest_csvs)
            return results

        except Exception as e:
            logger.error(f'An unexpected error occurred: {e}')
            logger.error(traceback.format_exc())
            exit_error()
            exit(1)

    def validate_file(self, file_name: str, upload_manifests=True) -> ValidatedFile:
        file = pathlib.Path(file_name)
        creds = self.validation_api.request_upload_credentials(file.name, self.config.scope)
        creds.upload_csv(file)
        validation_v2 = self.validation_api.wait_validation_complete(creds.uuid, self.config.validation_timeout, False)
        validated_file = ValidatedFile(file, v2_resource=validation_v2, v2_creds=creds)

        # upload manifests if the file has any...
        if upload_manifests and validated_file.waiting_manifest_upload():
            self.upload_manifests(validated_file)
        return validated_file

    def upload_manifests(self, *files):
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


def check_args(args, config):
    if args.replace_submission:
        if args.title or args.description or args.collectionID:
            message = 'Title, description, and collection ID are not allowed when replacing a submission' \
                      ' using -rs flag. Please remove -t, -d and -c when using -rs. Exiting...'
            logger.error(message)
            exit(1)
        check_replacement_authorized(config, args.replace_submission)


def validate(args, config=None) -> List[ValidatedFile]:
    if not config:
        auth_req = True if args.buildPackage or args.resume or args.replace_submission or args.username else False
        config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, auth_req=auth_req)
        check_args(args, config)

    api_config = get_request(f'{config.validation_api_endpoint}/config')
    percent = api_config['v2Routing']['percent']
    logger.debug('v2_routing percent: {}'.format(percent))
    # route X% of traffic to the new validation API
    v2_api = random.randint(1, 100) <= (percent * 100)

    nda = NdaUpload(config)  # only object to contain urls
    # Perform the validation using v1 or v2 endpoints. Errors and warnings are streamed or saved in memory for v2 and v1 respectively
    if v2_api:
        logger.debug('Using the new validation API.')
        if not config.is_authenticated():
            config.read_user_credentials()
        validated_files = nda.validate_files(args.files)
    else:
        logger.debug('Using the old validation API.')
        validated_files = nda.validate_files_v1(args.files, args.workerThreads)

    # Save errors to errors file
    writer = ResultsWriterFactory.get_writer(file_format='json' if args.JSON else 'csv')

    errors_file = writer.write_errors(validated_files)
    logger.info(
        '\nAll files have finished validating. Validation report output to: {}'.format(
            errors_file))
    if any(map(lambda x: x.system_error(), validated_files)):
        msg = 'Unexpected error occurred while validating one or more of the csv files.'
        msg += '\nPlease email NDAHelp@mail.nih.gov for help in resolving this error and include {} as an attachment to help us resolve the issue'
        exit_error(msg)

    # Save warnings to warnings file (if requested)
    if args.warning:
        warnings_file = writer.write_warnings(validated_files)
        logger.info('Warnings output to: {}'.format(warnings_file))
    elif any(map(lambda x: x.has_warnings(), validated_files)):
        logger.info('Note: Your data has warnings. To save warnings, run again with -w argument.')

    # Preview errors for each file
    for file in validated_files:
        if file.has_errors():
            if file.has_manifest_errors():
                file.show_manifest_errors()
            else:
                file.preview_validation_errors(10)

    # Exit if user intended to submit and there are any errors
    will_submit = args.buildPackage
    replace_submission = args.replace_submission
    if will_submit:
        if replace_submission:
            logger.error('ERROR - At least some of the files failed validation. '
                         'All files must pass validation in order to edit submission {}. Please fix these errors and try again.'.format(
                replace_submission))
        else:
            logger.info('You must correct the above errors before you can submit to NDA')
        sys.exit(1)

    return validated_files


def submit(args):
    exit_error('This command is not yet implemented')
