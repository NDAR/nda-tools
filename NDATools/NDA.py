import concurrent
import logging
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List

from tqdm import tqdm

from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import exit_error
from NDATools.upload import ValidatedFile
from NDATools.upload.submission import NdaSubmission
from NDATools.upload.validation.io import ValidationResultsWriter
from NDATools.upload.validation.v1 import Validation

logger = logging.getLogger(__name__)


class NDA:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config: ClientConfiguration):
        self.config = client_config
        self.validation_api = self.config.validation_api
        self.uploader = self.config.manifests_uploader
        self.validation_results_writer = ValidationResultsWriter(is_json=self.config.JSON)
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

    def save_validation_warnings(self, validated_files):
        self.validation_results_writer.save_validation_warnings(validated_files)
        return self.validation_results_writer.warnings_file

    def save_validation_errors(self, validated_files):
        self.validation_results_writer.save_validation_errors(validated_files)
        return self.validation_results_writer.errors_file


def validate(args):
    client_config = ClientConfiguration(args)
    nda = NDA(client_config)  # only object to contain urls

    results = nda.validate_files(args.files)
    nda.save_validation_errors(results)
    if args.warnings:
        nda.save_validation_warnings(results)


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
