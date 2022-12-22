import argparse
import signal

import NDATools
from NDATools.BuildPackage import SubmissionPackage
from NDATools.Configuration import *
from NDATools.Submission import Submission
from NDATools.Utils import evaluate_yes_no_input, exit_client, get_request
from NDATools.Validation import Validation

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

    parser.add_argument('-a', '--alternateEndpoint', metavar='<arg>', type=str, action='store',
                        help='An alternate upload location for the submission package')

    parser.add_argument('-b', '--buildPackage', action='store_true',
                        help='Flag whether to construct the submission package')

    parser.add_argument('-c', '--collectionID', metavar='<arg>', type=int, action='store',
                        help='The NDA collection ID')

    parser.add_argument('-d', '--description', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The description of the submission')

    parser.add_argument('-p', '--password', help='Warning: Detected non-empty value for the -p/--password argument. '
                                                 'Support for this setting has been deprecated and will no longer be '
                                                 'used by this tool. Password storage is not recommended for security'
                                                 ' considerations')

    parser.add_argument('-t', '--title', metavar='<arg>', type=str, nargs='+', action='store',
                        help='The title of the submission')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, action='store',
                        help='NDA username')

    parser.add_argument('-ak', '--accessKey', metavar='<arg>', type=str, action='store',
                        help='AWS access key')

    parser.add_argument('-sk', '--secretKey', metavar='<arg>', type=str, action='store',
                        help='AWS secret key')

    parser.add_argument('-s', '--scope', metavar='<arg>', type=str, action='store',
                        help='Flag whether to validate using a custom scope. Must enter a custom scope')

    parser.add_argument('-rs', '--replace-submission', metavar='<arg>', type=str, action='store', default=0,
                        help='Use this arugment to replace a submission that has QA errors or that NDA staff has authorized manually to replace.')

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

    parser.add_argument('--hideProgress', action='store_true', help='Hides upload/processing progress')

    parser.add_argument('--skipLocalAssocFileCheck', action='store_true', help='Not recommended UNLESS you have already'
                                                                               ' verified all paths for associated data files are correct')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Ignores all warnings and continues without prompting for input from the user.')

    parser.add_argument('--validation-timeout', default=300, type=int, action='store', help='Timeout in seconds until the program errors out with an error. '
                                                                               'In most cases the default value of ''300'' seconds should be sufficient to validate submissions however it may'
                                                                               'be necessary to increase this value to a specific duration.')

    args = parser.parse_args()

    if args.password:
        print('Warning: Support for the password flag (-p, --password) has been removed from nda-tools due to security '
              'concerns and has been replaced with keyring.')
        args.__dict__.pop('password')

    return args


def configure(args):
    # create a new config file in user's home directory if one does not exist

    # always set password if --username flag is supplied, or if user is submitting data
    auth_req = True if args.buildPackage or args.resume or args.replace_submission or args.username else False

    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username,
                                     args.accessKey, args.secretKey)
        config.read_user_credentials(auth_req)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.accessKey,
                                     args.secretKey)
        config.read_user_credentials(auth_req)
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
    if args.validation_timeout:
        config.validation_timeout = args.validation_timeout
    if args.title:
        config.title = ' '.join(args.title)
    if args.description:
        config.description = ' '.join(args.description)
    if args.scope:
        config.scope = args.scope
    if args.validationAPI:
        config.validation_api = args.validationAPI[0]
    if args.JSON:
        config.JSON = True
    config.workerThreads = args.workerThreads
    config.hideProgress = args.hideProgress
    if args.skipLocalAssocFileCheck:
        config.skip_local_file_check = True
    if args.replace_submission:
        config.replace_submission = args.replace_submission
    config.force = True if args.force else False
    LoggingConfiguration.load_config(NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER)

    return config


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'


def resume_submission(submission_id, batch, config=None):
    submission = Submission(id=submission_id, full_file_path=None, config=config, resume=True, batch_size=batch, thread_num=config.workerThreads)
    submission.check_status()
    if submission.status == Status.UPLOADING:
        directories = config.directory_list
        source_bucket = config.source_bucket
        source_prefix = config.source_prefix

        if submission.incomplete_files and submission.found_all_files(directories, source_bucket, source_prefix,
                                                                      retry_allowed=True):
            # if not config.skip_local_file_check:
            submission.check_submitted_files()
            submission.complete_partial_uploads()
            submission.submission_upload(hide_progress=config.hideProgress)
        else:
            submission.submission_upload(hide_progress=config.hideProgress)

        submission.check_status()
        if submission.status != Status.UPLOADING:
            print_submission_complete_message(submission, False)
    else:
        logger.info('Submission Completed with status {}'.format(submission.status))
        return
def validate_files(file_list, warnings, build_package, threads, config=None, pending_changes=None, original_uuids=None):
    validation = Validation(file_list, config=config, hide_progress=config.hideProgress, thread_num=threads,
                            allow_exit=True, pending_changes=pending_changes, original_uuids=original_uuids)
    logger.info('\nValidating files...')
    validation.validate()

    for (response, file) in validation.responses:
        if response['status'] == Status.SYSERROR:
            logger.error('\nSystemError while validating: {}'.format(file))
            logger.error('Please contact NDAHelp@mail.nih.gov')
        elif response['errors'] != {}:
            logger.info('\nError! Check file: {}'.format(file))
    validation.output()
    logger.info('Validation report output to: {}'.format(validation.log_file))

    if warnings:
        validation.get_warnings()
        logger.info('Warnings output to: {}'.format(validation.log_file))
    else:
        if validation.w:
            logger.info('\nNote: Your data has warnings. To save warnings, run again with -w argument.')
    logger.info('\nAll files have finished validating.')

    # Test if no files passed validation, exit
    if not any(map(lambda x: not validation.uuid_dict[x]['errors'], validation.uuid_dict)):
        logger.info('No files passed validation, please correct any errors and validate again.')
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
    # If some files had errors, give option to submit just the files that passed
    if not hasattr(config, 'replace_submission'):
        # If some files had errors, give option to submit just the files that passed
        if build_package and validation.e and not config.force:
            proceed = evaluate_yes_no_input('Some files have errors, do you want to continue '
                                            'and submit ONLY the files that have passed validation?', 'n')

            if str(proceed).lower() == 'no':
                return
            elif str(proceed).lower() == 'y':
                validation.uuid = validation.verify_uuid()
    # We are replacing a submission
    else:
        if build_package and validation.e:
            logger.error('ERROR - At least some of the files failed validation. '
                  'All files must pass validation in order to edit submission {}. Please fix these errors and try again.'.format(
                config.replace_submission))
            sys.exit(1)
        elif build_package and validation.data_structures_with_missing_rows and not config.force:
            logger.warning('\nWARNING - Detected missing information in the following files: ')

            for tuple in validation.data_structures_with_missing_rows:
                logger.warning('\n{} - expected {} rows but found {}  '.format(tuple[0], tuple[1], tuple[2]))
            prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
            prompt += '\nAre you sure you want to continue? <Yes/No>: '
            proceed = evaluate_yes_no_input(prompt, 'n')
            if str(proceed).lower() == 'n':
                exit_client(signal=signal.SIGTERM, message='')

    return validation.uuid, validation.associated_files_to_upload



def build_package(uuid, associated_files_to_upload, config, pending_changes=None, original_uuids=None):
    if not config.title:
        config.title = input('Enter title for dataset name:')
    if not config.description:
        config.description = input('Enter description for the dataset submission:')

    package = SubmissionPackage(uuid, associated_files_to_upload, config=config, allow_exit=True,
                                pending_changes=pending_changes, original_uuids=original_uuids)
    package.set_upload_destination(hide_input=False)
    directories = config.directory_list
    source_bucket = config.source_bucket
    source_prefix = config.source_prefix
    if associated_files_to_upload:
        logger.info('\nSearching for associated files...')
        package.file_search(directories, source_bucket, source_prefix, retry_allowed=True)
    logger.info('Building Package')
    package.build_package()
    logger.info('\n\nPackage Information:')
    logger.info('validation results: {}'.format(package.validation_results))
    logger.info('submission_package_uuid: {}'.format(package.submission_package_uuid))
    logger.info('created date: {}'.format(package.create_date))
    logger.info('expiration date: {}'.format(package.expiration_date))
    logger.info('\nPackage finished building.\n')

    logger.info('Downloading submission package.')
    package.download_package(hide_progress=config.hideProgress)
    logger.info('\nA copy of your submission package has been saved to: {}'.
          format(os.path.join(NDATools.NDA_TOOLS_SUB_PACKAGE_FOLDER, package.package_folder)))

    return [package.package_id, package.full_file_path]

def print_submission_complete_message(submission, replacement):
    if replacement:
        print('\nYou have successfully replaced submission {}.'.format(submission.submission_id))
    else:
        print('\nYou have successfully completed uploading files for submission {} with status: {}'.format
              (submission.submission_id, submission.status))

def submit_package(package_id, full_file_path, associated_files_to_upload, threads, batch,
                   config=None, original_submission_id=None):
    submission = Submission(id=package_id,
                            full_file_path=full_file_path,
                            thread_num=threads,
                            batch_size=batch,
                            allow_exit=True,
                            config=config,
                            original_submission_id=original_submission_id)
    logger.info('Requesting submission for package: {}'.format(submission.package_id))
    if original_submission_id:
        submission.replace_submission()
    else:
        submission.submit()
        # see commit comment for commit #d2f4dad
        # we need to trigger the GET /id endpoint to move the submission status to complete if necessary
        submission.check_status()
    if submission.submission_id:
        logger.info('Submission ID: {}'.format(str(submission.submission_id)))
    if associated_files_to_upload:
        logger.info('Preparing to upload associated files.')
        submission.submission_upload(hide_progress=config.hideProgress)
    if submission.status != Status.UPLOADING:
        print_submission_complete_message(submission, replacement=True if original_submission_id else False)


# sets self.pendingChanges and
def retrieve_replacement_submission_params(config, submission_id):
    # get submission-id
    api = type('', (), {})()
    api.config = config
    auth = requests.auth.HTTPBasicAuth(config.username, config.password)
    # check if the qa token provided is actually the latest or not
    try:
        response = get_request('/'.join([config.submission_api, submission_id, 'change-history']), auth=auth)
    except Exception as e:

        if e.response.status_code == 403:
            exit_client(signal=signal.SIGTERM,
                        message='You are not authorized to access submission {}. If you think this is a mistake, please contact NDA help desk'.format(
                            submission_id))
        else:
            exit_client(signal=signal.SIGTERM,
                        message='There was a General Error communicating with the NDA server. Please try again later')

    # TODO - check for 404 response

    # check to see if the submission was already replaced?
    if not response[0]['replacement_authorized']:
        if len(response) > 1 and response[1]['replacement_authorized']:
            message = '''Submission {} was already replaced by {} on {}.
If you need to make further edits to this submission, please reach out the the NDA help desk''' \
                .format(submission_id, response[0]['created_by'], response[0]['created_date'])
            exit_client(signal=signal.SIGTERM, message=message)
        else:
            exit_client(signal=signal.SIGTERM,
                        message='submission_id {} is not authorized to be replaced. Please contact the NDA help desk for approval to replace this submission'.format(
                            submission_id))

    response = get_request('/'.join([config.submission_api, submission_id]), auth=auth)
    if response is None:
        exit_client(signal=signal.SIGTERM,
                    message='There was a General Error communicating with the NDA server. Please try again later')

    submission_id = response['submission_id']
    config.title = response['dataset_title']
    config.description = response['dataset_description']
    config.collection_id = response['collection']['id']

    # get pending-changes for submission-id
    response = get_request('/'.join([config.submission_api, submission_id, 'pending-changes']), auth=auth);
    if response is None:
        exit_client(signal=signal.SIGTERM,
                    message='There was a General Error communicating with the NDA server. Please try again later')

    # get list of associated-files that have already been uplaoded for pending changes
    pending_changes = []
    original_submission_id = submission_id
    original_uuids = {uuid for uuid in response['validation_uuids']}
    for change in response['pendingChanges']:
        validation_uuids = change['validationUuids']
        associated_files = []
        manifest_files = []
        for uuid in validation_uuids:
            response = get_request('/'.join([config.validation_api, uuid]))
            associated_files.extend(response['associated_file_paths'])
            manifest_files.extend(manifest['localFileName'] for manifest in response['manifests'])
        change['associatedFiles'] = associated_files
        change['manifests'] = manifest_files
        pending_changes.append(change)

    return pending_changes, original_uuids, original_submission_id


def check_args(args):
    if args.replace_submission:
        if args.title or args.description or args.collectionID:
            message = 'Neither title, description nor collection_id arguments can be specified if' \
                      ' qa token is provided. Exiting...'
            logger.error(message)
            exit(1)


def main():
    # confirm most up to date version of nda-tools is installed
    args = parse_args()
    config = configure(args)

    pending_changes, original_uuids, original_submission_id = None, None, None
    check_args(args)
    if args.replace_submission:
        pending_changes, original_uuids, original_submission_id = retrieve_replacement_submission_params(config,
                                                                                                         args.replace_submission)

    if args.resume:
        submission_id = args.files[0]
        # Need to check to see if i need to update this step!
        resume_submission(submission_id, batch=args.batch, config=config)
    else:
        w = False
        bp = False
        if args.warning:
            w = True
        if args.buildPackage:
            bp = True
        validation_results = validate_files(args.files, w, bp, threads=args.workerThreads, config=config,
                                            pending_changes=pending_changes,
                                            original_uuids=original_uuids)
        if validation_results is not None:
            uuid = validation_results[0]
            associated_files_to_upload = validation_results[1]
            # If user requested to build a package
            if bp:
                package_results = build_package(uuid, associated_files_to_upload, config=config,
                                                pending_changes=pending_changes,
                                                original_uuids=original_uuids)
                package_id = package_results[0]
                full_file_path = package_results[1]
                submit_package(package_id=package_id, full_file_path=full_file_path,
                               associated_files_to_upload=associated_files_to_upload,
                               threads=args.workerThreads, batch=args.batch, config=config,
                               original_submission_id=original_submission_id)


if __name__ == "__main__":
    main()
