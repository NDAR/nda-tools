from __future__ import with_statement
from __future__ import absolute_import
import sys
if sys.version_info[0] < 3:
    input = raw_input
import argparse
import shutil
import fileinput
from NDATools.Download import Download
from NDATools.Configuration import *


def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to enter a list of aws S3 paths and will download the files to your local drive '
                    'in your home folder. Alternatively, you may enter a packageID,an NDA data structure file or a text file with s3 links, '
                    'and the client will download all associated files from S3 listed.',
        usage='%(prog)s <S3_path_list>')

    parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='+', action='store',
                        help='Will download all S3 files to your local drive')

    parser.add_argument('-dp', '--package', action='store_true',
                        help='Flags to download all S3 files in package.')

    parser.add_argument('-t', '--txt', action='store_true',
                        help='Flags that a text file has been entered from where to download S3 files.')

    parser.add_argument('-ds', '--datastructure', action='store_true',
                        help='Flags that a  data structure text file has been entered from where to download S3 files.')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, action='store',
                        help='NDA username')

    parser.add_argument('-p', '--password', metavar='<arg>', type=str, action='store',
                        help='NDA password')

    parser.add_argument('-r', '--resume', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Flags to restart a download process. If you already have some files downloaded, you must enter the directory where they are saved.')


    parser.add_argument('-d', '--directory', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Enter an alternate full directory path where you would like your files to be saved.')

    args = parser.parse_args()

    return args

def configure(args):
    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg')
        if args.username:
            config.username = args.username
        if args.password:
            config.password = args.password
        config.nda_login()
        file_path = os.path.join(os.path.expanduser('~'), '.NDATools')
        os.makedirs(file_path, exist_ok=True)
        file_copy = os.path.join(file_path, 'settings.cfg')

        config_location = resource_filename(__name__, '/config/settings.cfg')
        shutil.copy(config_location, file_copy) # make sure you can find this file
        with fileinput.FileInput(file_copy, inplace=True) as file:
            for line in file:
                if line.startswith('username'):
                    print(line.replace('=', '= {}'.format(config.username)))
                elif line.startswith('password'):
                    print(line.replace('=', '= {}'.format(config.password)))
                else:
                    print(line)
    if args.username:
        config.username = args.username
    if args.password:
        config.password = args.password
    return config

def main():
    args = parse_args()
    config = configure(args)

    # directory where files will be downloaded
    if args.directory:
        dir = args.directory[0]
    else:
        dir = os.path.join(os.path.expanduser('~'), 'AWS_downloads')

    # determine which method to use to collect s3 file locations
    if args.package:
        links = 'package'
        # this option only downloads data structure files from the package.
        # after data structure files have been downloaded, it will begin downloading associated files listed in each
        # structure
    elif args.txt:
        links = 'text'
    elif args.datastructure:
        links = 'datastructure'
    else:
        links = 'paths'

    # if some files were already downloaded, resume option will only download new files
    resume = False
    prev_directory = None
    if args.resume:
        resume = True
        prev_directory = args.resume[0]

    s3Download = Download(dir, config)
    s3Download.get_links(links, args.paths, filters=None)
    s3Download.get_tokens()
    s3Download.start_workers(resume, prev_directory)

    # download associated files from package
    #if args.package:
    #    s3Download.searchForDataStructure(resume, prev_directory)


    print('Finished downloading all files.')

if __name__ == "__main__":
    main()