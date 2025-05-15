import logging
import pathlib

from NDATools import exit_error

logger = logging.getLogger(__name__)


def validate(args):
    '''TODO before we move vtcmd code into here, we need to add an adapter for the command line options'''
    exit_error('This command is not yet implemented')


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
