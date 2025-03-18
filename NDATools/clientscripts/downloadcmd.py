import argparse
import sys

from NDATools.Configuration import *
from NDATools.Download import Download

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to download files from an NDA package. Tutorials for creating packages'
                    ' can be found on the website (links provided below). Information for packages, including package-ids,'
                    ' are displayed on the packages dashboard page (https://nda.nih.gov/user/dashboard/packages.html). Users can'
                    ' only download data from "personal" type packages. To download files from a'
                    ' "shared" package you need to convert it to a "personal" package first, which can be done by clicking the "Add to my data'
                    ' packages" button in the actions dropdown. '
                    ''
                    '\nLinks:'
                    '\n\tvideo tutorial - https://nda.nih.gov/tutorials/nda/accessing_files_in_the_cloud.html?chapter=creating-a-package '
                    '\n\tpdf - https://ndar.nih.gov/ndarpublicweb/Documents/Accessing+Shared+Data+Sept_2021-1.pdf',
        formatter_class=argparse.RawTextHelpFormatter)

    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    required.add_argument('-dp', '--package', required=True, metavar='<package-id>', type=int, action='store',
                          help='The package-id containing the files you wish to download. If no other command-line '
                               'options are provided, the program will download all files from the specified package.')

    parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='*', action='store',
                        help='Opional. When provided, the program will download only the specified files from the package.'
                             ' The specified files must exist in the package indicated by the -dp argument and the paths must be valid'
                             ' s3 urls.')

    parser.add_argument('-t', '--txt', metavar='<s3-links-file>', type=str, action='store',
                        help='Flags that a text file has been entered from where to download S3 files. '
                             'For more details, check the information on the README page.')

    parser.add_argument('-ds', '--datastructure', metavar='<structure short-name>', type=str, action='store',
                        help='''Downloads all the files in a package from the specified data-structure. 
For example, to download all the image03 files from your package 12345, you should enter:
    downloadcmd -dp 12345 -ds image03
Note - the program only recognizes the short-names of the data-structures. The short-name is listed on the data-structures page 
and always ends in a 2 digit number. (For example, see the data-structure page for image03 at https://nda.nih.gov/data_structure.html?short_name=image03)''')

    parser.add_argument('-u', '--username', metavar='<username>', type=str.lower, action='store',
                        help='NDA username')

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

For s3-to-s3 copy operations to be successful, the s3 bucket supplied as the program argument must be configured to allow PUT object 
operations  for 'arn:aws:sts::618523879050:federated-user/<username>' where <username> is your nda username. 
For non-public buckets, this will require an update to your bucket's policy. The following statement should be sufficient to grant the uploading privileges necessary 
to run this program using the s3 argument after replacing <your-s3-bucket> with the bucket name:
         {
            "Sid": "AllowNDAUpload",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::618523879050:federated-user/<username>"		
            },
            "Action": "s3:PutObject*",
            "Resource": "arn:aws:s3:::<your-s3-bucket>/*"
        }
        
You may need to email your company/institution IT department to have this added for you.
Note: If your bucket is encrypted with a customer-managed KMS key, then additional configuration is needed. 
For more details, check the information on the README page.
''')
    parser.add_argument('--verbose', action='store_true',
                        help='Enables debug logging.')

    parser.add_argument('--log-dir', type=str, action='store', help='Customize the file directory of logs. '
                                                                    'If this value is not provided or the provided directory does not exist, logs will be saved to NDA/nda-tools/downloadcmd/logs inside your root folder.')

    args = parser.parse_args()

    return args


def main():
    args = parse_args()
    config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER)
    if args.s3_destination and not args.s3_destination.startswith('s3://'):
        raise Exception(
            'Invalid argument for -s3 option :{}. Argument must start with "s3://"'.format(args.s3_destination))
    if sys.version_info < (3, 5):
        logger.error(
            'ERROR: "--verify" only works with python 3.5 or later. Please upgrade Python in order to continue')
        exit_error()

    s3Download = Download(config, args)
    if args.verify:
        s3Download.verify_download()
    else:
        s3Download.start()


if __name__ == "__main__":
    main()
