import concurrent
import logging
import pathlib
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List

from tqdm import tqdm

from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import exit_error
from NDATools.upload import ValidatedFile
from NDATools.upload.validation.api import ValidationManifest
from NDATools.upload.validation.io import UserIO
from NDATools.upload.validation.v1 import Validation

logger = logging.getLogger(__name__)


def display_validation_results(validated_files: List[ValidatedFile], will_submit: bool,
                               replace_submission: int = None,
                               force: bool = False):
    valid_files = list(filter(lambda x: x.is_valid(), validated_files))
    invalid_files = list(filter(lambda x: x.is_invalid(), validated_files))

    # If some files passed validation, show files with and without errors
    if valid_files:
        logger.info('\nThe following files passed validation:')
        for file in valid_files:
            logger.info('UUID {}: {}'.format(file.uuid, file.file))

    if invalid_files:
        logger.info('\nThese files contain errors:')
        for file in invalid_files:
            logger.info('UUID {}: {}'.format(file.uuid, file.file))

        # pretty print summary of errors in a table
        validation.output_validation_error_messages()

        if will_submit:
            if replace_submission:
                logger.error('ERROR - At least some of the files failed validation. '
                             'All files must pass validation in order to edit submission {}. Please fix these errors and try again.'.format(
                    replace_submission))
            else:
                logger.info('You must correct the above errors before you can submit to NDA')
            sys.exit(1)

    # For resubmission workflow: alert user if data loss was detected in one of their data-structures
    if config.replace_submission:
        if will_submit and validation.data_structures_with_missing_rows and not config.force:
            logger.warning('\nWARNING - Detected missing information in the following files: ')

            for tuple_expected_actual in validation.data_structures_with_missing_rows:
                logger.warning(
                    '\n{} - expected {} rows but found {}  '.format(tuple_expected_actual[0],
                                                                    tuple_expected_actual[1],
                                                                    tuple_expected_actual[2]))
            prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
            prompt += '\nAre you sure you want to continue? <Yes/No>: '
            proceed = evaluate_yes_no_input(prompt, 'n')
            if str(proceed).lower() == 'n':
                exit_error(message='')


class NDA:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config: ClientConfiguration):
        self.config = client_config
        self.validation_api = self.config.validation_api
        self.uploader = self.config.manifests_uploader
        ...

    def validate_files_v1(self, file_list, save_warnings: bool, will_submit, threads, config=None, pending_changes=None,
                          original_uuids=None) -> List[ValidatedFile]:
        validation = Validation(file_list, config=self.config, hide_progress=self.config.hideProgress,
                                thread_num=threads,
                                allow_exit=True, pending_changes=pending_changes, original_uuids=original_uuids)
        logger.info('\nValidating files...')
        validation.validate()

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
                self.upload_manifests([m for csv in manifest_csvs for m in csv.manifests])
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
        validated_file = ValidatedFile(file, v2_resource=validation_v2)

        # upload manifests if the file has any...
        if upload_manifests and validated_file.waiting_manifest_upload():
            return self.upload_manifests(validated_file.manifests)[0]
        return validated_file

    def upload_manifests(self, manifests: List[ValidationManifest]) -> List[ValidatedFile]:
        # add warning if more than 1 manifest dir was detected. in later versions of the tool, we are only going to allow users to specify one manifest dir
        if isinstance(self.config.manifest_path, list):
            if len(self.config.manifest_path) > 1:
                logger.warning(
                    f'Found multiple manifest directories: {self.config.manifest_path}. Only the first one ({self.config.manifest_path[0]}) will be used.')
            manifest_dir = self.config.manifest_path[0]
        else:
            # should be NoneType
            manifest_dir = self.config.manifest_path

        self.uploader.upload_manifests(manifests, manifest_dir)
        print(f'\nManifests uploaded. Waiting for validation of manifests to complete....')
        validation_results: List[ValidatedFile] = [m.validation_response for m in manifests]

        with tqdm(total=len(validation_results), disable=self.config.hideProgress) as progress_bar, \
                ThreadPoolExecutor(max_workers=self.config.workerThreads) as executor:
            # executor.map seems to block whereas executor.submit doesn't...
            futures = list(
                map(lambda x: executor.submit(
                    self.validation_api.wait_validation_complete, x.uuid, self.config.validation_timeout, True),
                    validation_results))
            for f in concurrent.futures.as_completed(futures):
                r = f.result()
                logger.debug(f'Validation status for {r.uuid} updated to {r.status}')
                progress_bar.update(1)

        return validation_results


def validate(args):
    client_config = ClientConfiguration(args)
    nda = NDA(client_config)  # only object to contain urls
    io = UserIO(is_json=args.json, skip_prompt=args.force)

    results = nda.validate_files(args.files)
    io.save_validation_errors(results)
    if args.warnings:
        io.save_validation_warnings(results)


def submit(args):
    exit_error('This command is not yet implemented')


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparser = parser.add_subparsers()
    # TODO add 'download' subcommand
    parser_validate = subparser.add_parser('validate',
                                           help='validate data against NDA data-dictionary. Does not submit data')

    parser_validate.add_argument('files', type=pathlib.Path)
    parser_validate.set_defaults(func=validate)

    parser_submit = subparser.add_parser('submit',
                                         help='Submit data to an NDA collection. Data is validated before being submitted')

    parser_submit.add_argument('files', type=pathlib.Path)
    parser_submit.add_argument('-a', '--associated-files-dir', type=pathlib.Path)
    parser_submit.add_argument('-m', '--manifests-dir', type=pathlib.Path)
    # parser_submit.add_argument('-r', '--resume', type=pathlib.Path)
    parser_submit.set_defaults(func=submit)
