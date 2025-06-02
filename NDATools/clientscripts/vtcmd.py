import argparse
import logging
import random
import traceback

import NDATools
from NDATools import authenticate
from NDATools import exit_error
from NDATools.Configuration import ClientConfiguration
from NDATools.Utils import get_non_blank_input, get_int_input
from NDATools.upload.submission.api import CollectionApi
from NDATools.upload.submission.resubmission import check_replacement_authorized
from NDATools.upload.validation.api import ValidationV2Api

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
                    ', you may resume your upload by entering a valid submission ID. ')

    parser.add_argument('files', type=str, nargs='+', action='store',
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
                        help='Use this argument to replace a submission that has QA errors or that NDA staff has authorized manually to replace.')

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


def check_args(args, config):
    if args.replace_submission:
        if args.title or args.description or args.collectionID:
            message = 'Title, description, and collection ID are not allowed when replacing a submission' \
                      ' using -rs flag. Please remove -t, -d and -c when using -rs. Exiting...'
            logger.error(message)
            exit_error()
        check_replacement_authorized(config, args.replace_submission)


def validate(args, config):
    logger.info(f'\nValidating {len(args.files)} files...')
    # Perform the validation using v1 or v2 endpoints.
    if config.v2_enabled:
        logger.debug('Using the new validation API.')
        if not config.is_authenticated():
            authenticate(config)
        validated_files = config.upload_cli.validate(args.files, args.manifestPath)
    else:
        logger.debug('Using the old validation API.')
        validated_files = config.upload_cli.validate_v1(args.files, config.worker_threads)

    # Save errors to file
    errors_file = config.validation_results_writer.write_errors(validated_files)

    logger.info(
        '\nAll files have finished validating. Validation report output to: {}'.format(
            errors_file))
    if any(map(lambda x: x.system_error(), validated_files)):
        msg = 'Unexpected error occurred while validating one or more of the csv files.'
        msg += '\nPlease email NDAHelp@mail.nih.gov for help in resolving this error and include {} as an attachment to help us resolve the issue'
        logger.info(msg)
        exit_error()

    # Save warnings to file (if requested)
    if args.warning:
        warnings_file = config.validation_results_writer.write_warnings(validated_files)
        logger.info('Warnings output to: {}'.format(warnings_file))
    elif any(map(lambda x: x.has_warnings(), validated_files)):
        logger.info('Note: Your data has warnings. To save warnings, run again with -w argument.')

    # Preview errors for each file
    has_errors = False
    for file in validated_files:
        if file.has_errors():
            has_errors = True
            if file.has_manifest_errors():
                file.preview_manifest_errors(10)
            else:
                file.preview_validation_errors(10)

    # Exit if user intended to submit and there are any errors
    will_submit = args.buildPackage
    if will_submit and has_errors:
        if args.replace_submission:
            logger.error('ERROR - At least some of the files failed validation. '
                         'All files must pass validation in order to edit submission {}. Please fix these errors and try again.'.format(
                args.replace_submission))
        else:
            logger.info('You must correct the above errors before you can submit to NDA')
        exit_error()

    return validated_files


def resume_submission(sub_id, config):
    submission = config.upload_cli.resume(sub_id, config.directory_list)
    print_submission_complete_message(submission, False)


def collect_submission_parameters(config: ClientConfiguration):
    coll_api: CollectionApi = config.collection_api

    def get_collection_id():
        collections = coll_api.get_user_collections()
        if not collections:
            message = 'The user {} does not have permission to submit to any collections.'.format(config.username)
            exit_error(message=message)
        id = config.collection_id or get_int_input('Enter collection ID:', 'Collection ID')
        c_ids = {c.id for c in collections}
        if not id in c_ids:
            logger.info('Invalid collection ID')
            logger.error(f'You do not have access to submit to the collection: {id} ')
            logger.info(f'Please choose from one of the following collections: ')
            for coll in collections:
                logger.info('{}: {}'.format(coll.id, coll.title))

            return get_collection_id()
        return id

    collection_id = get_collection_id()
    name = config.title or get_non_blank_input('Enter title for dataset name:', 'Title')
    description = config.description or get_non_blank_input('Enter description for the dataset submission:',
                                                            'Description')
    return collection_id, name, description


def print_submission_complete_message(submission, replacement):
    if replacement:
        print('\nYou have successfully replaced submission {}.'.format(submission.id))
    else:
        print('\nYou have successfully completed uploading files for submission {} with status: {}'.format
              (submission.id, submission.status.value))


def replace_submission(validated_files, config):
    submission = config.upload_cli.replace_submission(config.replace_submission, validated_files,
                                                      config.directory_list)
    logger.info("Submission replaced successfully.")
    print_submission_complete_message(submission, replacement=True)


def submit(validated_files, config):
    collection_id, name, description = collect_submission_parameters(config)
    submission = config.upload_cli.submit(validated_files, collection_id, name, description, config.directory_list)
    print_submission_complete_message(submission, replacement=False)


def set_validation_api_version(config):
    """Enable v2 of validation svc for some percentage of requests"""
    try:
        api = ValidationV2Api(config.validation_api_endpoint, None, None)
        percent = api.get_v2_routing_percent()
        logger.debug('v2_routing percent: {}'.format(percent))
        # route X% of traffic to the new validation API
        config.v2_enabled = random.randint(1, 100) <= (percent * 100)
    except:
        traceback.print_exc()
        logger.warning('Could not get v2_routing percent. Using the old validation API.')
        config.v2_enabled = False


def main():
    # confirm latest version of nda-tools is installed
    args = parse_args()
    auth_req = True if args.buildPackage or args.resume or args.replace_submission or args.username else False
    config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, auth_req=auth_req)
    check_args(args, config)

    # route some percentage of requests to the new validation endpoints
    set_validation_api_version(config)

    if args.resume:
        # submission_id is stored in positional arg 'files'
        try:
            submission_id = int(args.files[0])
        except:
            exit_error('Invalid submission ID. Please enter a valid submission ID to resume.')
        else:
            resume_submission(submission_id, config=config)
    else:
        validated_files = validate(args, config)
        if args.replace_submission:
            replace_submission(validated_files, config)
        elif args.buildPackage:
            submit(validated_files, config)


if __name__ == "__main__":
    main()
