from __future__ import absolute_import, print_function, with_statement

import sys

import NDATools

if sys.version_info[0] < 3:
    input = raw_input
import argparse
from NDATools.Download import Download
from NDATools.Configuration import *

logger = logging.getLogger(__name__)
def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to enter a list of aws S3 paths and will download the files to your '
                    'drive in your home folder. Alternatively, you may enter a packageID, an NDA data structure file or'
                    ' a text file with s3 links, and the client will download all files from the S3 links listed. '
                    'Please note, the maximum transfer limit of data is 5TB at one time.',
        usage='%(prog)s <S3_path_list>',
        formatter_class=argparse.RawTextHelpFormatter)

    # parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='+', action='store',
    parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='*', action='store',
                        help='Will download all S3 files to your local drive')

    # parser.add_argument('-dp', '--package', action='store_true', required=True,
    parser.add_argument('-dp', '--package', required=True, metavar='<package-id>', type=str, action='store',
                        help='Flags to download all S3 files in package. Required.')

    parser.add_argument('-t', '--txt', metavar='<s3-links-file>', type=str, action='store',
                        help='Flags that a text file has been entered from where to download S3 files.')

    parser.add_argument('-ds', '--datastructure', metavar='<structure short-name>', type=str, action='store',
                        help='''Downloads all the files in a package from the specified data-structure. 
For example, to download all the image03 files from your package 12345, you should enter:
    downloadcmd -dp 12345 -ds image03
Note - the program only recognizes the short-names of the data-structures. The short-name is listed on the data-structures page 
and always ends in a 2 digit number. (For example, see the data-structure page for image03 at https://nda.nih.gov/data_structure.html?short_name=image03)''')

    parser.add_argument('-u', '--username', metavar='<username>', type=str, action='store',
                        help='NDA username')

    parser.add_argument('-p', '--password', help='Warning: Detected non-empty value for the -p/--password argument. '
                                                 'Support for this setting has been deprecated and will no longer be '
                                                 'used by this tool. Password storage is not recommended for security'
                                                 ' considerations')

    parser.add_argument('-d', '--directory', metavar='<download_directory>', type=str, nargs=1, action='store',
                        help='Enter an alternate full directory path where you would like your files to be saved. The default is ~/NDA/nda-tools/<package-id>')

    parser.add_argument('-wt', '--workerThreads', metavar='<thread-count>', type=int, action='store',
                        help='''Specifies the number of downloads to attempt in parallel. For example, running 'downloadcmd -dp 12345 -wt 10' will 
cause the program to download a maximum of 10 files simultaneously until all of the files from package 12345 have been downloaded. 
A default value is calculated based on the number of cpus found on the machine, however a higher value can be chosen to decrease download times. 
If this value is set too high the download will slow. With 32 GB of RAM, a value of '10' is probably close to the maximum number of 
parallel downloads that the computer can handle''')

    parser.add_argument('--file-regex', metavar='<regular expression>',
                        help='''Option can be used to download only a subset of the files in a package.  This command line arg can be used with
the -ds, -dp or -t flags. 

Examples - 
1) To download all files with a ".txt" extension, you can use the regular expression .*.txt
    downloadcmd -dp 12345 --file-regex .*.txt
2) To download all files that contain "NDARINVZLHFUAF0" in the name, you can use the regular expression NDARINVZLHFUAF0 
    downloadcmd -dp 12345 -ds image03 --file-regex NDARINVZLHFUAF0  
3) Finally to download all files underneath a folder called "T1w" you can use the regular expression .*/T1w/.* 
    downloadcmd -dp 12345 -t s3-links.txt --file-regex .*/T1w/.*''')

    parser.add_argument('--verify', action='store_true',
                        help='''When this option is provided a download is not initiated. Instead, a csv file is produced that contains a record of 
the files in the download, along with information about the file-size if the file could be found on the computer. For large packages containing millions of files, 
this verification step can take hours (this can be even longer if files are stored on a network drive). When the program finishes, a few new files/folders 
will be created (if they don't already exist):
1) verification_report folder in the NDA/nda-tools/downloadcmd/packages/<package-id> directory
2) .download_progress folder (hidden) in the NDA/nda-tools/downloadcmd/packages/<package-id> directory, which is used to values between command invocations.
    a. .download_progress/download-job-manifest.csv file - contains entries mapping 
    b. UUID folders inside .download_progress (with names like '6a056ac4-2dd9-48f2-b921-44b29c883578')
3) download-verification-report.csv in the NDA/nda-tools/downloadcmd/packages/<package-id> directory
4) download-verification-retry-s3-links.csv in the NDA/nda-tools/downloadcmd/packages/<package-id> directory

The hidden folder listed in 2 contains special files used by the program to avoid re-running expensive, time-consuming processes. This folder should not be deleted.

The download-verification-report.csv file will contain a record for each file in the download and contain 6 columns :
1) 'package_file_id'
2) 'package_file_expected_location' - base path is the value provided for the -d/--directory arg
3) 'nda_s3_url'
4) 'exists' - value for column will be ('Y'/'N')
5) 'expected_size'
6) 'actual_file_size' - value for columnw will be '0' if file doesn't exist

In addition, the file will contain 1 header line which will provide the parameters used for the download (more information below). 

If this file is opened in Excel or Google Docs, the user can easily find information on specific files that they are interested in. 

This file can be useful but may contain more information than is needed. The download-verification-retry-s3-links.csv file contains the s3 links for all of the files 
in the download-verification-report.csv where EXISTS = 'N' or EXPECTED-SIZE does not equal ACTUAL-FILE-SIZE. If the user is only interested in re-initiating the download 
for the files that failed they can do so by using the  download-verification-retry-s3-links.csv as the value for the -t argument. i.e.

downloadcmd -dp <package-id> -t NDA/nda-tools/downloadcmd/packages/<package-id>/download-verification-retry-s3-links.csv 

When the --verify option is provided, the rest of the arguments provided to the command are used to determine what files are supposed to be included in the download. 

For example, if the user runs:  
   downloadcmd -dp 12345 --verify
The download-verification-report.csv file will contain a record for each file in the package 12345. Since no -d/--directory argument is provided, the program 
will check for the existance of the files in the default download location. 

If the user runs:
   downloadcmd -dp 12345 -d /home/myuser/customdirectory --verify
The download-verification-report.csv file will contain a record for each file in the package 12345 and will check for the existance of files in the /foo/bar

If the user runs:
   downloadcmd -dp 12345 -d /home/myuser/customdirectory -t file-with-s3-links.csv --verify
The download-verification-report.csv file will contain a record for each file listed in the file-with-s3-links.csv and will check for the existance of files in /foo/bar

If the user runs:
   downloadcmd -dp 12345 -d /home/myuser/customdirectory -ds image03 --verify
The download-verification-report.csv file will contain a record for each file in the package's image03 data-structure and will check for the existance of files in /foo/bar

If the user runs:
   downloadcmd -dp 12345 -d /home/myuser/customdirectory -ds image03 --file-regex --verify
The download-verification-report.csv file will contain a record for each file in the package's image03 data-structure which also matches the file-regex and will check 
for the existance of files in /foo/bar

NOTE - at the moment, this option cannot be used to verify downloads to s3 locations (see -s3 option below). That will be implemented in the near
future.''')

    parser.add_argument('-s3', '--s3-destination', metavar='<s3 bucket>',
                        help='''Specify s3 location which you would like to download your files to. When this option is specified, an attempt will be made
to copy the files from your package, which are stored in NDA's own S3 repository, to the S3 bucket provided. 


This is the preferred way to download data from NDA for two reasons:

1)  FASTER !!! - Downloads to another s3 bucket are orders of magnitude faster because data doesn't leave AWS
2)  CHEAPER (for us) - We do not limit the amount of data transferred to another bucket, but we do when its downloaded out of AWS. 

For s3-to-s3 copy operations to be successfull, the s3 bucket supplied as the program arugment must be configured to allow PUT object 
operations  for 'arn:aws:sts::618523879050:federated-user/<username>' where <username> is your nda username. For non-public buckets, this 
will require an update to your bucket's policy. The following statement should be sufficient to grant the uploading privileges necessary 
to run this program using the s3 argument (after replacing <your-s3-bucket> with the appropriate value):
         {
            "Sid": "AllowNDAUpload",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::618523879050:federated-user/<username>"		
            },
            "Action": "s3:PutObject*",
            "Resource": "arn:aws:s3:::<your-s3-bucket>/*"
        }
        
You may need to email your company/institution IT department to have this added for you.''')

    args = parser.parse_args()

    if args.password:
        print('Warning: Support for the password flag (-p, --password) has been removed from nda-tools due to security '
              'concerns and has been replaced with keyring.')
        args.__dict__.pop('password')

    return args


def configure(args):
    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username)
        config.read_user_credentials()
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username)
        config.read_user_credentials()
        config.make_config()

    LoggingConfiguration.load_config(NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER)

    return config


def main():
    args = parse_args()
    config = configure(args)

    if args.s3_destination and not args.s3_destination.startswith('s3://'):
        raise Exception(
            'Invalid argument for -s3 option :{}. Argument must start with "s3://"'.format(args.s3_destination))
    if sys.version_info < (3, 5):
        logger.error('ERROR: "--verify" only works with python 3.5 or later. Please upgrade Python in order to continue')
        exit_client()

    s3Download = Download(config, args)
    if args.verify:
        s3Download.verify_download()
    else:
        s3Download.start()


if __name__ == "__main__":
    main()