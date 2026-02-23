import argparse
import logging
import pathlib

from NDATools import exit_error
from NDATools.upload.validation import manifests

logger = logging.getLogger(__name__)


def validate(args):
    '''TODO before we move vtcmd code into here, we need to add an adapter for the command line options'''
    exit_error('This command is not yet implemented')


def submit(args):
    exit_error('This command is not yet implemented')


def generate_manifests(args):
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

    parser_manifests = subparser.add_parser('generate-manifests', help='Generate manifest files for a collection')
    parser_manifests.add_argument('-i', '--subject-directory', type=existing_dir, default='.')
    parser_manifests.add_argument('-o', '--output-directory', type=existing_dir, default='.')
    parser_manifests.add_argument('-ir', '--include-regex', type=str, default='.*')
    parser_manifests.add_argument('-er', '--exclude-regex', type=str, default=None)
    parser_manifests.add_argument('-c', '--include-checksum-calculation', action='store_false')
    parser_manifests.add_argument('-s', '--include-file-size', action='store_false')
    parser_manifests.set_defaults(func=generate_manifests)

    args = parser.parse_args()
    args.func(args)
