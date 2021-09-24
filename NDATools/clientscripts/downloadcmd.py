from __future__ import with_statement
from __future__ import absolute_import
import sys
if sys.version_info[0] < 3:
    input = raw_input
import argparse
from NDATools.Download import Download
from NDATools.Configuration import *
import NDATools

import logging


def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to enter a list of aws S3 paths and will download the files to your '
                    'drive in your home folder. Alternatively, you may enter a packageID, an NDA data structure file or'
                    ' a text file with s3 links, and the client will download all files from the S3 links listed. '
                    'Please note, the maximum transfer limit of data is 5TB at one time.',
        usage='%(prog)s <S3_path_list>')

    # parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='+', action='store',
    parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='*', action='store',
                        help='Will download all S3 files to your local drive')

    # parser.add_argument('-dp', '--package', action='store_true', required=True,
    parser.add_argument('-dp', '--package', metavar='<arg>', type=str, action='store',
                        help='Flags to download all S3 files in package. Required.')

    parser.add_argument('-t', '--txt', metavar='<arg>', type=str, action='store',
                        help='Flags that a text file has been entered from where to download S3 files.')

    parser.add_argument('-ds', '--datastructure', metavar='<arg>', type=str, action='store',
                        help='Flags that a  data structure text file has been entered from where to download S3 files.')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, action='store',
                        help='NDA username')

    parser.add_argument('-p', '--password', metavar='<arg>', type=str, action='store',
                        help='NDA password')

    parser.add_argument('-r', '--resume', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Flags to restart a download process. If you already have some files downloaded, you must '
                             'enter the directory where they are saved.')

    parser.add_argument('-d', '--directory', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Enter an alternate full directory path where you would like your files to be saved.')

    parser.add_argument('-wt', '--workerThreads', metavar='<arg>', type=int, action='store',
                        help='Number of worker threads')

    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Option to suppress output of detailed messages as the program runs.')

    parser.add_argument('--file-regex',
                        help='Option can be used to download only a subset of the files in a package. '
    ' This command line arg can be used with the -ds, -dp or -t flags. For example, to download all files with a ".txt" extension, you can specify --file-regex ".*.txt". '
     ' To download all files that contain "NDARINVZLHFUAF0" in the name, use --file-regex "NDARINVZLHFUAF0". '
     ' Finally to download all files underneath a folder called "T1w", use --file-regex ".*/T1w/.*"')

    args = parser.parse_args()

    return args


def configure(args):

    NDATools.Utils.logging.getLogger().setLevel(logging.INFO)
    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username, args.password)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.password)
        config.read_user_credentials()
        config.make_config()

    return config


def main():
    args = parse_args()
    config = configure(args)

    # directory where files will be downloaded
    if args.directory:
        dir = args.directory[0]
    elif args.resume:
        dir = args.resume[0]
    else:
        dir = os.path.join(os.path.expanduser('~'), 'AWS_downloads')

    # determine which method to use to collect s3 file locations
    # if args.package:  # todo: change how this is done. package is always provided
    #     links = 'package'
        # this option only downloads data structure files from the package.
        # after data structure files have been downloaded, it will begin downloading associated files listed in each
        # structure
    paths = None

    if args.txt:
        links = 'text'
        paths = args.txt
    elif args.datastructure:
        links = 'datastructure'
        paths = args.datastructure
    elif args.paths:
        links = 'paths'
        paths = args.paths
    else:  # only package provided
        links = 'package'

    s3Download = Download(dir, config, quiet=args.quiet, thread_num=args.workerThreads, regex_file_filter=args.file_regex)
    s3Download.get_links(links, paths, args.package, filters=None)
    s3Download.start_workers(args.resume, dir)


if __name__ == "__main__":
    main()
