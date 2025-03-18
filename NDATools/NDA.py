import concurrent
import logging
import pathlib
import traceback
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import exit_error
from NDATools.upload.validation.api import ValidationResponse, ValidationManifest, ValidationApi
from NDATools.upload.validation.io import UserIO
from NDATools.upload.validation.uploader import ManifestsUploader

logger = logging.getLogger(__name__)


class NDA:
    """ Higher level API for nda, meant to eventually replace existing vtmcd/downloadcmd code"""

    def __init__(self, client_config: ClientConfiguration):
        self.config = client_config
        self.validation_api = ValidationApi(self.config)
        self.uploader = ManifestsUploader(self.validation_api,
                                          self.config.workerThreads,
                                          not self.config.force,
                                          self.config.hideProgress)
        ...

    def validate_files(self, files: [str]) -> [ValidationResponse]:
        logger.info(f'\nValidating {len(files)} files...')
        try:
            # validate the files first, and then upload the manifests in order to match the behavior of prev versions of the client
            results: [ValidationResponse] = []

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

    def validate_file(self, file_name: str, upload_manifests=True) -> ValidationResponse:
        val_resp: ValidationResponse = self.validation_api.validate_file(pathlib.Path(file_name), self.config.scope,
                                                                         self.config.validation_timeout)
        # upload manifests if the file has any...
        if upload_manifests and val_resp.waiting_manifest_upload():
            return self.upload_manifests(val_resp.manifests)[0]
        return val_resp

    def upload_manifests(self, manifests: [ValidationManifest]) -> [ValidationResponse]:
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
        validation_results: {ValidationResponse} = {m.validation_response for m in manifests}

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
    io.run_validation_step_io(results, args.warnings)


def submit(args):
    exit_error('This command is not yet implemented')


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparser = parser.add_subparsers()
    # TODO add 'submit' and 'download' subcommands
    parser_validate = subparser.add_parser('validate',
                                           help='validate data against NDA data-dictionary. Does not submit data')

    parser_validate.add_argument('files', type=pathlib.Path)
    parser_validate.set_defaults(func=validate)

    parser_submit = subparser.add_parser('submit',
                                         help='Submit data to an NDA collection. Data is validated before being submitted')

    parser_submit.add_argument('files', type=pathlib.Path)
    parser_submit.add_argument('-a', '--assoicated-files-dir', type=pathlib.Path)
    parser_submit.add_argument('-m', '--manifests-dir', type=pathlib.Path)
    # parser_submit.add_argument('-r', '--resume', type=pathlib.Path)
    parser_submit.set_defaults(func=submit)
