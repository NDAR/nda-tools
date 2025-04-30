import logging
import pathlib

from NDATools.upload import validate, submit

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparser = parser.add_subparsers()
    # TODO add 'download' subcommand
    parser_validate = subparser.add_parser('validate',
                                           help='validate data against NDA data-dictionary. Does not submit data')

    parser_validate.add_argument('files', type=pathlib.Path)
    parser_validate.set_defaults(func=validate, config=None)

    parser_submit = subparser.add_parser('submit',
                                         help='Submit data to an NDA collection. Data is validated before being submitted')

    parser_submit.add_argument('files', type=pathlib.Path)
    parser_submit.add_argument('-a', '--associated-files-dir', type=pathlib.Path)
    parser_submit.add_argument('-m', '--manifests-dir', type=pathlib.Path)
    # parser_submit.add_argument('-r', '--resume', type=pathlib.Path)
    parser_submit.set_defaults(func=submit)
