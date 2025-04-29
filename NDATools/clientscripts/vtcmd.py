import argparse
import random

from NDATools.BuildPackage import SubmissionPackage
from NDATools.Configuration import *
from NDATools.NDA import NDA, display_validation_results
from NDATools.Submission import Submission
from NDATools.Utils import get_request, get_non_blank_input
from NDATools.upload.validation.io import UserIO
from NDATools.upload.validation.v1 import retrieve_replacement_submission_params

logger = logging.getLogger(__name__)


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

    parser.add_argument('-w', '--warning', action='store_true',
                        help='Returns validation warnings for list of files')

    parser.add_argument('-b', '--buildPackage', action='store_true',
                        help='Flag whether to construct the submission package')

    parser.add_argument('-c', '--collectionID', metavar='<arg>', type=int, action='store',
                        help='The integer part of an NDA collection ID, i.e., for collection C1234, enter 1234')

    parser.add_argument('-d', '--description', metavar='<arg>', type=str, action='store',
                        help='The description of the submission')

    parser.add_argument('-t', '--title', metavar='<arg>', type=str, action='store',
                        help='The title of the submission')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str.lower, action='store',
                        help='NDA username')

    parser.add_argument('-s', '--scope', metavar='<arg>', type=str, action='store',
                        help='Flag whether to validate using a custom scope. Must enter a custom scope')

    parser.add_argument('-rs', '--replace-submission', metavar='<arg>', type=str, action='store', default=0,
                        help='Use this arugment to replace a submission that has QA errors or that NDA staff has authorized manually to replace.')

    parser.add_argument('-r', '--resume', action='store_true',
                        help='Restart an in-progress submission, resuming from the last successful part in a multi-part'
                             'upload. Must enter a valid submission ID.')

    parser.add_argument('-j', '--JSON', action='store_true',
                        help='Flag whether to additionally download validation results in JSON format.')

    parser.add_argument('-wt', '--workerThreads', metavar='<arg>', type=int, action='store',
                        help='Number of worker threads')

    parser.add_argument('-bc', '--batch', metavar='<arg>', type=int, action='store',
                        help='Batch size', default=50)

    parser.add_argument('--hideProgress', action='store_true', help='Hides upload/processing progress')

    parser.add_argument('--skipLocalAssocFileCheck', action='store_true', help='Not recommended UNLESS you have already'
                                                                               ' verified all paths for associated data files are correct')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Ignores all warnings and continues without prompting for input from the user.')

    parser.add_argument('--validation-timeout', default=300, type=int, action='store',
                        help='Timeout in seconds until the program errors out with an error. '
                             'In most cases the default value of ''300'' seconds should be sufficient to validate submissions however it may'
                             'be necessary to increase this value to a specific duration.')
    parser.add_argument('--verbose', action='store_true',
                        help='Enables detailed logging.')

    parser.add_argument('--log-dir', type=str, action='store', help='Customize the file directory of logs. '
                                                                    'If this value is not provided or the provided directory does not exist, logs will be saved to NDA/nda-tools/vtcmd/logs inside your root folder.')

    args = parser.parse_args()

    return args


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'


def resume_submission(sub_id, batch, config=None):
    submission = Submission(config, submission_id=sub_id, batch_size=batch, thread_num=config.workerThreads)
    submission.resume_submission()
    if submission.status != Status.UPLOADING:
        print_submission_complete_message(submission, False)


def validate(args, config, pending_changes, original_uuids):
    api_config = get_request(f'{config.validation_api_endpoint}/config')
    percent = api_config['v2Routing']['percent']
    logger.debug('v2_routing percent: {}'.format(percent))
    # route X% of traffic to the new validation API
    v2_api = random.randint(1, 100) <= (percent * 100)

    nda = NDA(config)  # only object to contain urls
    io = UserIO(is_json=config.JSON, skip_prompt=config.force)

    if v2_api:
        logger.debug('Using the new validation API.')
        if not config.is_authenticated():
            config.read_user_credentials()
        validated_files = nda.validate_files(args.files)
    else:
        logger.debug('Using the old validation API.')
        validated_files = nda.validate_files_v1(args.files, args.warning, args.buildPackage,
                                                threads=args.workerThreads,
                                                config=config,
                                                pending_changes=pending_changes,
                                                original_uuids=original_uuids)
    io.save_validation_errors(validated_files)
    if args.warning:
        io.save_validation_warnings(validated_files)
    display_validation_results()


def build_package(uuid, config, pending_changes=None, original_uuids=None):
    if not config.title:
        config.title = get_non_blank_input('Enter title for dataset name:', 'Title')
    if not config.description:
        config.description = get_non_blank_input('Enter description for the dataset submission:', 'Description')

    package = SubmissionPackage(uuid, config=config, pending_changes=pending_changes,
                                original_uuids=original_uuids)

    logger.info('Building Package')
    package.build_package()
    logger.info('\n\nPackage Information:')
    logger.info('validation results: {}'.format(package.validation_results))
    logger.info('submission_package_uuid: {}'.format(package.submission_package_uuid))
    logger.info('created date: {}'.format(package.create_date))
    logger.info('expiration date: {}'.format(package.expiration_date))
    logger.info('\nPackage finished building.\n')

    return package


def print_submission_complete_message(submission, replacement):
    if replacement:
        print('\nYou have successfully replaced submission {}.'.format(submission.submission_id))
    else:
        print('\nYou have successfully completed uploading files for submission {} with status: {}'.format
              (submission.submission_id, submission.status))


def submit_package(package_id, threads, batch,
                   config=None, original_submission_id=None):
    submission = Submission(package_id=package_id,
                            submission_id=original_submission_id,
                            thread_num=threads,
                            batch_size=batch,
                            allow_exit=True,
                            config=config)
    logger.info('Requesting submission for package: {}'.format(submission.package_id))
    if original_submission_id:
        submission.replace_submission()
    else:
        submission.submit()
    submission.check_status()
    if submission.submission_id:
        logger.info('Submission ID: {}'.format(str(submission.submission_id)))
    if submission.status == Status.UPLOADING:
        logger.info('Preparing to upload associated files.')
        submission.upload_associated_files()
        submission.check_status()
    if submission.status != Status.UPLOADING:
        print_submission_complete_message(submission, replacement=True if original_submission_id else False)


def check_args(args):
    if args.replace_submission:
        if args.title or args.description or args.collectionID:
            message = 'Title, description, and collection ID are not allowed when replacing a submission' \
                      ' using -rs flag. Please remove -t, -d and -c when using -rs. Exiting...'
            logger.error(message)
            exit(1)


def main():
    # confirm most up-to-date version of nda-tools is installed
    args = parse_args()
    auth_req = True if args.buildPackage or args.resume or args.replace_submission or args.username else False
    config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, auth_req=auth_req)
    pending_changes, original_uuids, original_submission_id = None, None, None
    check_args(args)
    if args.replace_submission:
        pending_changes, original_uuids, original_submission_id = retrieve_replacement_submission_params(config,
                                                                                                         args.replace_submission)
    if args.resume:
        submission_id = args.files[0]
        # Need to check to see if I need to update this step!
        resume_submission(submission_id, batch=args.batch, config=config)
    else:
        uuid = validate(args, config, pending_changes, original_uuids)
        # If user requested to build a package
        if args.buildPackage:
            package = build_package(uuid,
                                    config=config,
                                    pending_changes=pending_changes,
                                    original_uuids=original_uuids)
            submit_package(package_id=package.package_id,
                           threads=args.workerThreads,
                           batch=args.batch,
                           config=config,
                           original_submission_id=original_submission_id)


if __name__ == "__main__":
    main()
