from NDATools.Configuration import *
from NDATools.Validation import Validation
from NDATools.BuildPackage import SubmissionPackage
from NDATools.Submission import Submission
import argparse
import os



def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to validate files and submit data into NDA. '
                    'You must enter a list of at least one file to be validated. '
                    'If your data contains manifest files, you must specify the location of the manifests. '
                    'If your data also includes associated files, you must enter a list of at least one directory '
                    'where the associated files are saved. Alternatively, if any of your data is stored in AWS, you must'
                    ' provide your account credentials, the AWS bucket, and a prefix, if it exists.  '
                    'Any files that are created while running the client (ie. results files) will be downloaded in '
                    'your home directory under NDAValidationResults. If your submission was interrupted in the middle'
                    ', you may resume your upload by entering a valid submission ID. ',
        usage='%(prog)s <file_list>')

    parser.add_argument('files', metavar='<file_list>', type=str, nargs='+', action='store',
                        help='Returns validation results for list of files')

    parser.add_argument('-l', '--listDir', metavar='<directory_list>', type=str, nargs='+', action='store',
                        help='Specifies the directories in which the associated files are files located.')

    parser.add_argument('-m', '--manifestPath', metavar='<arg>', type=str, nargs='+', action='store',
                        help='Specifies the directories in which the manifest files are located')

    parser.add_argument('-s3', '--s3Bucket', metavar='<arg>', type=str, action='store',
                        help='Specifies the s3 bucket in which the associated files are files located.')

    parser.add_argument('-pre', '--s3Prefix', metavar='<arg>', type=str, action='store',
                        help='Specifies the s3 prefix in which the associated files are files located.')

    parser.add_argument('-w', '--warning', action='store_true',
                        help='Returns validation warnings for list of files')

    parser.add_argument('-a', '--alternateEndpoint', metavar='<arg>', type=str, action='store',
                        help='An alternate upload location for the submission package')

    parser.add_argument('-b', '--buildPackage', action='store_true',
                        help='Flag whether to construct the submission package')

    parser.add_argument('-c', '--collectionID', metavar='<arg>', type=int, action='store',
                        help='The NDA collection ID')

    parser.add_argument('-d', '--description', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The description of the submission')

    parser.add_argument('-t', '--title', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The title of the submission')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, action='store',
                        help='NDA username')

    parser.add_argument('-ak', '--accessKey', metavar='<arg>', type=str, action='store',
                        help='AWS access key')

    parser.add_argument('-sk', '--secretKey', metavar='<arg>', type=str, action='store',
                        help='AWS secret key')

    parser.add_argument('-p', '--password', metavar='<arg>', type=str, action='store',
                        help='NDA password')

    parser.add_argument('-s', '--scope', metavar='<arg>', type=str, action='store',
                        help='Flag whether to validate using a custom scope. Must enter a custom scope')

    parser.add_argument('-r', '--resume', action='store_true',
                        help='Restart an in-progress submission, resuming from the last successful part in a multi-part'
                             'upload. Must enter a valid submission ID.')

    parser.add_argument('-v', '--validationAPI', metavar='<arg>', type=str, action='store',
                        help='URL of the validation tool API')

    parser.add_argument('-j', '--JSON', action='store_true',
                        help='Flag whether to additionally download validation results in JSON format.')

    parser.add_argument('-wt', '--workerThreads', metavar='<arg>', type=int, action='store',
                        help='Number of worker threads')

    parser.add_argument('-bc', '--batch', metavar='<arg>', type=int, action='store',
                        help='Batch size')

    parser.add_argument('--hideProgress', action='store_true', help='Hides upload/proccessing progress')


    args = parser.parse_args()

    return args



def configure(args):
    # create a new config file in user's home directory if one does not exist

    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username,
                                     args.password, args.accessKey, args.secretKey)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.password, args.accessKey,
                                     args.secretKey)
        config.read_user_credentials()
        config.make_config()

    if args.collectionID:
        config.collection_id = args.collectionID
    if args.alternateEndpoint:
        config.endpoint_title = args.alternateEndpoint
    if args.listDir:
        config.directory_list = args.listDir
    if args.manifestPath:
        config.manifest_path = args.manifestPath
    if args.s3Bucket:
        config.source_bucket = args.s3Bucket
    if args.s3Prefix:
        config.source_prefix = args.s3Prefix
    if args.title:
        config.title = ' '.join(args.title)
    if args.description:
        config.description = ' '.join(args.description)
    if args.scope:
        config.scope = args.scope[0]
    if args.validationAPI:
        config.validation_api = args.validationAPI[0]
    if args.JSON:
            config.JSON = True
    config.hideProgress = args.hideProgress

    return config

class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'

def resume_submission(submission_id, batch, config=None):
    submission = Submission(id=submission_id, full_file_path=None, config=config, resume=True, batch_size=batch)
    submission.check_status()
    if submission.status == Status.UPLOADING:
        directories = config.directory_list
        source_bucket = config.source_bucket
        source_prefix = config.source_prefix
        access_key = config.aws_access_key
        secret_key = config.aws_secret_key
        if submission.incomplete_files and submission.found_all_files(directories, source_bucket, source_prefix,
                                                                      access_key, secret_key, retry_allowed=True):
            submission.check_submitted_files()
            submission.complete_partial_uploads()
            submission.submission_upload(hide_progress=config.hideProgress)
        else:
           submission.submission_upload(hide_progress=config.hideProgress)

    else:
        print('Submission Completed with status {}'.format(submission.status))
        return


def validate_files(file_list, warnings, build_package, threads, config=None):
    validation = Validation(file_list, config=config, hide_progress=config.hideProgress, thread_num=threads, allow_exit=True)
    print('\nValidating files...')
    validation.validate()
    for (response, file) in validation.responses:
        if response['status'] == Status.SYSERROR:
            print('\nSystemError while validating: {}'.format(file))
            print('Please contact NDAHelp@mail.nih.gov')
        elif response['errors'] != {}:
            print('\nError! Check file: {}'.format(file))
    validation.output()
    print('Validation report output to: {}'.format(validation.log_file))

    if warnings:
        validation.get_warnings()
        print('Warnings output to: {}'.format(validation.log_file))

    else:
        if validation.w:
            print('\nNote: Your data has warnings. To save warnings, run again with -w argument.')
    print('\nAll files have finished validating.')

    # Test if no files passed validation, exit
    if not any(map(lambda x: not validation.uuid_dict[x]['errors'], validation.uuid_dict)):
        print('No files passed validation, please correct any errors and validate again.')
        return
    # If some files passed validation, show files with and without errors
    else:
        print('\nThe following files passed validation:')
        for uuid in validation.uuid_dict:
            if not validation.uuid_dict[uuid]['errors']:
                print('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
        if validation.e:
            print('\nThese files contain errors:')
            for uuid in validation.uuid_dict:
                if validation.uuid_dict[uuid]['errors']:
                    print('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
    # If some files had errors, give option to submit just the files that passed
    if validation.e and build_package:
        while True:
            proceed = input('Some files have errors, do you want to continue '
                            'and submit ONLY the files that have passed validation? <Yes/No>: ')
            if str(proceed).lower() == 'no':
                return
            elif str(proceed).lower() == 'yes':
                validation.uuid = validation.verify_uuid()
                break
            else:
                print('Your answer <{}> was not recognized, please enter yes or no.'.format(str(proceed)))
                continue
    return([validation.uuid, validation.associated_files])


def build_package(uuid, associated_files, config):
    if not config.title:
        config.title = input('Enter title for dataset name:')
    if not config.description:
        config.description = input('Enter description for the dataset submission:')

    package = SubmissionPackage(uuid, associated_files, config=config, allow_exit=True)
    package.set_upload_destination(hide_input=False)
    directories = config.directory_list
    source_bucket = config.source_bucket
    source_prefix = config.source_prefix
    access_key = config.aws_access_key
    secret_key = config.aws_secret_key
    if associated_files:
        print('\nSearching for associated files...')
        package.file_search(directories, source_bucket, source_prefix, access_key, secret_key, retry_allowed=True)
    print('Building Package')
    package.build_package()
    print('\n\nPackage Information:')
    print('validation results: {}'.format(package.validation_results))
    print('submission_package_uuid: {}'.format(package.submission_package_uuid))
    print('created date: {}'.format(package.create_date))
    print('expiration date: {}'.format(package.expiration_date))
    print('\nPackage finished building.\n')

    print('Downloading submission package.')
    package.download_package(hide_progress=config.hideProgress)
    print('\nA copy of your submission package has been saved to: {}'.
          format(os.path.join(package.package_folder, package.config.submission_packages)))

    return[package.package_id, package.full_file_path]


def submit_package(package_id, full_file_path, associated_files, threads, batch, config=None):
    submission = Submission(id=package_id, full_file_path=full_file_path, thread_num=threads, batch_size=batch, allow_exit=True, config=config)
    print('Requesting submission for package: {}'.format(submission.package_id))
    submission.submit()
    if submission.submission_id:
        print('Submission ID: {}'.format(str(submission.submission_id)))
    if associated_files:
        print('Preparing to upload associated files.')
        submission.submission_upload(hide_progress=config.hideProgress)
    if submission.status != Status.UPLOADING:
        print('\nYou have successfully completed uploading files for submission {} with status: {}'.format
              (submission.submission_id, submission.status)) 
               #do we want to include this??

def main():
    args = parse_args()
    config = configure(args)
    if args.resume:
        submission_id = args.files[0]
        resume_submission(submission_id, batch=args.batch, config=config)
    else:
        w = False
        bp = False
        if args.warning:
            w = True
        if args.buildPackage:
            bp = True
        validation_results = validate_files(args.files, w, bp, threads=args.workerThreads, config=config)
        if validation_results is not None:
            uuid = validation_results[0]
            associated_files = validation_results[1]
            # If user requested to build a package
            if bp:
                package_results = build_package(uuid, associated_files, config=config)
                package_id = package_results[0]
                full_file_path = package_results[1]
                submit_package(package_id=package_id, full_file_path=full_file_path, associated_files=associated_files,
                               threads=args.workerThreads, batch=args.batch, config=config)

if __name__ == "__main__":
    main()