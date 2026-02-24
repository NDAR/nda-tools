import argparse
import logging
import pathlib

import NDATools
from NDATools import exit_error
from NDATools.upload.validation import manifests

logger = logging.getLogger(__name__)


def validate(args):
    '''TODO before we move vtcmd code into here, we need to add an adapter for the command line options'''
    exit_error('This command is not yet implemented')


def submit(args):
    exit_error('This command is not yet implemented')


def generate_manifests(args):
    NDATools.init(args, NDATools.NDA_TOOLS_NDA_LOGS_FOLDER)
    manifests.generate_manifests(
        args.subject_directory,
        args.output_directory,
        include_regex=args.include_regex,
        exclude_regex=args.exclude_regex,
        include_checksum=args.include_checksum_calculation,
        include_size=args.include_file_size
    )


def existing_dir(path):
    p = pathlib.Path(path)
    if not p.is_dir():
        raise argparse.ArgumentTypeError(f"{path} is not a valid directory")
    return p


def create_dir_if_not_exists(path):
    p = pathlib.Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparser = parser.add_subparsers()

    parser.add_argument('--log-dir', type=pathlib.Path, help='Customize the file directory of logs. '
                                                             'If this value is not provided or the provided directory does not exist, logs will be saved to NDA/nda-tools/nda/logs inside your home folder.')

    parser_manifests = subparser.add_parser('generate-manifests', help='Generate manifest files for a submission')
    parser_manifests.add_argument('-i', '--subject-directory', type=existing_dir, default='.')
    parser_manifests.add_argument('-o', '--output-directory', type=existing_dir, default='.')
    parser_manifests.add_argument('-ir', '--include-regex', type=str, default=None)
    parser_manifests.add_argument('-er', '--exclude-regex', type=str, default=None)
    parser_manifests.add_argument('-c', '--include-checksum-calculation', action='store_true')
    parser_manifests.add_argument('-s', '--include-file-size', action='store_true')
    parser_manifests.add_argument('-v', '--verbose', action='store_true')
    parser_manifests.set_defaults(func=generate_manifests)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
