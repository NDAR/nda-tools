import argparse
import random
import sys

import requests.exceptions

from NDATools.BuildPackage import SubmissionPackage
from NDATools.Configuration import *
from NDATools.NDA import NDA
from NDATools.Submission import Submission
from NDATools.Utils import evaluate_yes_no_input, get_request, get_non_blank_input
from NDATools.Validation import Validation
from NDATools.upload.validation.api import ValidationResponse
from NDATools.upload.validation.io import UserIO

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

    parser.add_argument('-s3', '--s3Bucket', metavar='<arg>', type=str, action='store',
                        help='Specifies the s3 bucket in which the associated files are files located.')

    parser.add_argument('-pre', '--s3Prefix', metavar='<arg>', type=str, action='store',
                        help='Specifies the s3 prefix in which the associated files are files located.')

    parser.add_argument('-w', '--warning', action='store_true',
                        help='Returns validation warnings for list of files')

    parser.add_argument('-b', '--buildPackage', action='store_true',
                        help='Flag whether to construct the submission package')

    parser.add_argument('-c', '--collectionID', metavar='<arg>', type=int, action='store',
                        help='The NDA collection ID')

    parser.add_argument('-d', '--description', metavar='<arg>', type=str, action='store',
                        help='The description of the submission')

    parser.add_argument('-t', '--title', metavar='<arg>', type=str, action='store',
                        help='The title of the submission')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str.lower, action='store',
                        help='NDA username')

    parser.add_argument('--accessKey', metavar='<arg>', type=str, action='store',
                        help='AWS access key')

    parser.add_argument('--secretKey', metavar='<arg>', type=str, action='store',
                        help='AWS secret key')

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
                        help='Batch size')

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


def validate_v2(file_list, config, show_warnings: bool) -> [ValidationResponse]:
    nda = NDA(config)  # only object to contain urls
    io = UserIO(is_json=config.JSON, skip_prompt=config.force)

    results = nda.validate_files(file_list)
    io.run_validation_step_io(results, show_warnings)
    return results


def validate_v1(file_list, warnings, will_submit, threads, config=None, pending_changes=None, original_uuids=None):
    validation = Validation(file_list, config=config, hide_progress=config.hideProgress, thread_num=threads,
                            allow_exit=True, pending_changes=pending_changes, original_uuids=original_uuids)
    logger.info('\nValidating files...')
    validation.validate()
    validation.output()

    if warnings:
        warning_path = validation.get_warnings()
        logger.info('Warnings output to: {}'.format(warning_path))
    else:
        if validation.w:
            logger.info('\nNote: Your data has warnings. To save warnings, run again with -w argument.')
    logger.info('\nAll files have finished validating.')

    # Test if no files passed validation, exit
    if not any(map(lambda x: not validation.uuid_dict[x]['errors'], validation.uuid_dict)):
        logger.info('No files passed validation, please correct any errors and validate again.')
        validation.output_validation_error_messages()
        sys.exit(1)
    # If some files passed validation, show files with and without errors
    else:
        logger.info('\nThe following files passed validation:')
        for uuid in validation.uuid_dict:
            if not validation.uuid_dict[uuid]['errors']:
                logger.info('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
        if validation.e:
            logger.info('\nThese files contain errors:')
            for uuid in validation.uuid_dict:
                if validation.uuid_dict[uuid]['errors']:
                    logger.info('UUID {}: {}'.format(uuid, validation.uuid_dict[uuid]['file']))
                    validation.output_validation_error_messages()
    # If some files had errors, give option to submit just the files that passed
    if not config.replace_submission:
        # If some files had errors, give option to submit just the files that passed
        if will_submit and validation.e and not config.force:
            proceed = evaluate_yes_no_input('Some files have errors, do you want to continue '
                                            'and submit ONLY the files that have passed validation?', 'n')

            if str(proceed).lower() == 'no':
                return
            elif str(proceed).lower() == 'y':
                validation.uuid = validation.verify_uuid()
    # We are replacing a submission
    else:
        if will_submit and validation.e:
            logger.error('ERROR - At least some of the files failed validation. '
                         'All files must pass validation in order to edit submission {}. Please fix these errors and try again.'.format(
                config.replace_submission))
            exit_error()
        elif will_submit and validation.data_structures_with_missing_rows and not config.force:
            logger.warning('\nWARNING - Detected missing information in the following files: ')

            for tuple_expected_actual in validation.data_structures_with_missing_rows:
                logger.warning(
                    '\n{} - expected {} rows but found {}  '.format(tuple_expected_actual[0], tuple_expected_actual[1],
                                                                    tuple_expected_actual[2]))
            prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
            prompt += '\nAre you sure you want to continue? <Yes/No>: '
            proceed = evaluate_yes_no_input(prompt, 'n')
            if str(proceed).lower() == 'n':
                exit_error(message='')

    return validation.uuid, validation.associated_files_to_upload


def validate(args, config, pending_changes, original_uuids):
    api_config = get_request(f'{config.validation_api}/config')
    percent = api_config['v2Routing']['percent']
    logger.debug('v2_routing percent: {}'.format(percent))
    # route X% of traffic to the new validation API
    v2_api = random.randint(1, 100) <= (percent * 100)

    if v2_api:
        logger.debug('Using the new validation API.')
        if not config.is_authenticated():
            config.read_user_credentials(True)
        validation_results = validate_v2(args.files, config, args.warning)
        return [r.uuid for r in validation_results]
    else:
        logger.debug('Using the old validation API.')
        validation_results = validate_v1(args.files, args.warning, args.buildPackage,
                                         threads=args.workerThreads,
                                         config=config,
                                         pending_changes=pending_changes,
                                         original_uuids=original_uuids)
        return validation_results[0]


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


def retrieve_replacement_submission_params(config, submission_id):
    api = type('', (), {})()
    api.config = config
    auth = requests.auth.HTTPBasicAuth(config.username, config.password)

    try:
        response = get_request('/'.join([config.submission_api, submission_id, 'change-history']), auth=auth)
    except requests.exceptions.HTTPError as e:

        if e.response.status_code == 403:
            exit_error(
                message='You are not authorized to access submission {}. If you think this is a mistake, please contact NDA help desk'.format(
                    submission_id))
        else:
            exit_error(message='There was a General Error communicating with the NDA server. Please try again later')

    # check to see if the submission was already replaced?
    if not response[0]['replacement_authorized']:
        if len(response) > 1 and response[1]['replacement_authorized']:
            message = '''Submission {} was already replaced by {} on {}.
If you need to make further edits to this submission, please reach out the the NDA help desk''' \
                .format(submission_id, response[0]['created_by'], response[0]['created_date'])
            exit_error(message=message)
        else:
            exit_error(
                message='submission_id {} is not authorized to be replaced. Please contact the NDA help desk for approval to replace this submission'.format(
                    submission_id))

    response = get_request('/'.join([config.submission_api, submission_id]), auth=auth)
    if response is None:
        exit_error(message='There was a General Error communicating with the NDA server. Please try again later')

    submission_id = response['submission_id']
    config.title = response['dataset_title']
    config.description = response['dataset_description']
    config.collection_id = response['collection']['id']

    # get pending-changes for submission-id
    response = get_request('/'.join([config.submission_api, submission_id, 'pending-changes']), auth=auth)
    if response is None:
        exit_error(message='There was a General Error communicating with the NDA server. Please try again later')

    # get list of associated-files that have already been uplaoded for pending changes
    pending_changes = []
    original_submission_id = submission_id
    original_uuids = {uuid for uuid in response['validation_uuids']}
    for change in response['pendingChanges']:
        validation_uuids = change['validationUuids']
        manifest_files = []
        for uuid in validation_uuids:
            response = get_request('/'.join([config.validation_api, uuid]))
            manifest_files.extend(manifest['localFileName'] for manifest in response['manifests'])
        change['manifests'] = manifest_files
        pending_changes.append(change)

    return pending_changes, original_uuids, original_submission_id


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
