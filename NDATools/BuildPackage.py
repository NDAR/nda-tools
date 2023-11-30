from __future__ import absolute_import, with_statement

import sys

import botocore
import requests.packages.urllib3.util

if sys.version_info[0] < 3:
    input = raw_input
import requests.packages.urllib3.util
from NDATools.Configuration import *
from NDATools.Utils import *

logger = logging.getLogger(__name__)

class SubmissionPackage:
    def __init__(self, uuid, associated_files, config, username=None, password=None, collection=None, title=None,
                 description=None, pending_changes=None, original_uuids=None):
        self.config = config
        self.api = self.config.submission_package_api
        self.validationtool_api = self.config.validationtool_api
        self.uuid = uuid
        self.associated_files = associated_files
        self.full_file_path = {}
        self.no_match = []
        if username:
            self.config.username = username
        if password:
            self.config.password = password
        if title:
            self.config.title = title
        if description:
            self.config.description = description
        self.username = self.config.username
        self.password = self.config.password
        self.dataset_name = self.config.title
        self.dataset_description = self.config.description
        self.package_info = {}
        self.download_links = []
        self.package_id = None
        self.package_folder = None
        self.collection_id = collection
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        self.collections = self.get_collections()

        self.validation_results = []
        self.no_read_access = set()
        self.pending_changes = pending_changes
        self.original_validation_uuids = original_uuids

    def get_collections(self):
        collections = get_request("/".join([self.validationtool_api, "user/collection"]), auth=self.auth, headers={'Accept':'application/json'})
        return { int(c['id']):c['title'] for c in collections}

    def set_upload_destination(self):
        if not self.collections:
            message = 'The user {} does not have permission to submit to any collections.'.format(self.config.username)
            exit_error(message=message)

        if self.config.collection_id:
            self.collection_id = self.config.collection_id
            if not self.collection_id in self.collections:
                message = 'The user {} does not have permission to submit to collection {}.'.format(self.config.username, self.collection_id)
                exit_error(message=message)

        while self.collection_id not in self.collections:
            user_input = input('\nEnter collection ID:')
            try:
                self.collection_id = int(user_input.strip())
                if self.collection_id not in self.collections:
                    logger.error(f'You do not have access to submit to the collection: {self.collection_id} ')
                    logger.info(f'Please choose from one of the following collections: ')
                    for collection_id in sorted(self.collections.keys()):
                        logger.info('{}: {}'.format(collection_id, self.collections[collection_id]))
            except ValueError:
                logger.error('Error: Input must be a valid integer')


    def recollect_file_search_info(self):
        retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                      '<bucket name> to locate your associated files:')
        response = retry.split(' ')
        self.directory_list = response
        if response[0] == '-s3':
            self.source_bucket = response[1]
            self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
            if self.source_prefix == "":
                self.source_prefix = None

    def file_search(self, directories=None, source_bucket=None, source_prefix=None, retry_allowed=False):
        def raise_error(error, l = []):
            m = '\n'.join([error] + list(set(l)))
            raise Exception(m)

        self.directory_list = directories
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix

        if not self.directory_list and not self.source_bucket:
            if retry_allowed:
                self.recollect_file_search_info()
            else:
                error ='Missing directory and/or an S3 bucket.'
                raise_error(error)

        if not self.no_match:
            for file in self.associated_files:
                self.no_match.append(file)

        # local files
        if self.directory_list:
            parse_local_files(self.directory_list, self.no_match, self.full_file_path, self.no_read_access,
                              self.config.skip_local_file_check)

        # files in s3
        no_access_buckets = []
        if self.source_bucket:
            if not self.config.aws_access_key:
                self.config.read_aws_credentials()

            s3_client = get_s3_client_with_config(self.config.aws_access_key, self.config.aws_secret_key, self.config.aws_session_token)
            for file in self.no_match[:]:
                key = file
                if self.source_prefix:
                    key = '/'.join([self.source_prefix, file])
                file_name = '/'.join(['s3:/', self.source_bucket, key])
                try:
                    response = s3_client.head_object(Bucket=self.source_bucket, Key=key)
                    self.full_file_path[file] = (file_name, int(response['ContentLength']))
                    self.no_match.remove(file)
                except botocore.exceptions.ClientError as e:
                    # If a client error is thrown, then check that it was a 404 error.
                    # If it was a 404 error, then the bucket does not exist.
                    error_code = int(e.response['Error']['Code'])
                    if error_code == 404:
                        pass
                    if error_code == 403:
                        no_access_buckets.append(self.source_bucket)
                        pass

        if self.no_match:
            if no_access_buckets:
                message = 'Your user does NOT have access to the following buckets. Please review the bucket ' \
                          'and/or your AWS credentials and re-run the command.'
                if retry_allowed:
                    logger.info('\n%s', message)
                    for b in set(no_access_buckets):
                        logger.info(b)
                    exit_error()
                else:
                    error = "".join(['Bucket Access:', message])
                    raise_error(error, no_access_buckets)
            message = 'You must make sure all associated files listed in your validation file' \
                      ' are located in the specified directory or AWS bucket. Associated file not found in specified directory:\n'
            if retry_allowed:
                logger.info('\n%s', message)

                for idx, file in enumerate(self.no_match):
                    logger.info(file)
                    if idx >= 49:
                        logger.info('...and (%d) other files', len(self.no_match)-50)
                        break
                self.recollect_file_search_info()
                self.file_search(self.directory_list, self.source_bucket, self.source_prefix, retry_allowed=True)
            else:
                error = "".join(['Missing Files:', message])
                raise_error(error, self.no_match)

        while self.no_read_access:
            message = 'You must make sure you have read-access to all the of the associated files listed in your validation file. ' \
                      'Please update your permissions for the following associated files:\n'
            if retry_allowed:
                logger.info(message)
                for file in self.no_read_access:
                    logger.info(file)
                self.recollect_file_search_info()
                [self.no_read_access.remove(i) for i in
                    [file for file in self.no_read_access if check_read_permissions(file)]]
            else:
                error = "".join(['Read Permission Error:', message])
                raise_error(error, self.no_match)

        self.config.directory_list = self.directory_list
        self.config.source_bucket = self.source_bucket
        self.config.source_prefix = self.source_prefix

    def build_package(self):
        def raise_error(value):
            raise Exception("Missing {}. Please try again.".format(value))

        if self.dataset_name is None:
            raise_error('dataset name')

        if self.dataset_description is None:
            raise_error('dataset description')

        if self.collection_id is None:
            raise_error('collection ID')

        self.package_info = {
            "package_info": {
                "dataset_description": self.dataset_description,
                "dataset_name": self.dataset_name,
                "collection_id": self.collection_id
            },
            "validation_results":
                self.uuid
        }
        if self.config.replace_submission:
            self.package_info['package_info']['replacement_submission'] = self.config.replace_submission
            self.print_replacement_summary()
            if not self.config.force:
                user_input = evaluate_yes_no_input("Are you sure you want to continue?", 'n')
                if user_input.lower() == 'n':
                    exit_error(message='Exiting...')


        json_data = json.dumps(self.package_info)
        response = post_request(self.api, json_data, auth=self.auth)
        if response:
            try:
                self.package_id = response['submission_package_uuid']
                for r in response['validation_results']:
                    self.validation_results.append(r['id'])
                self.submission_package_uuid = str(response['submission_package_uuid'])
                self.create_date = str(response['created_date'])
                self.expiration_date = str(response['expiration_date'])
            except KeyError:
                message = 'There was an error creating your package.'
                if response['status'] == Status.ERROR:
                    message = response['errors'][0]['message']
                exit_error(message=message)

            while response['package_info']['status'] == Status.PROCESSING:
                time.sleep(1.1)
                response = get_request("/".join([self.api, self.package_id]), auth=self.auth)
            if response['package_info']['status'] == Status.COMPLETE:
                for f in [f for f in response['files']
                          if f['type'] in ('Submission Memento', 'Submission Data Package')]:
                    for key, value in f['_links'].items():
                        for k, v in value.items():
                            self.download_links.append((v, "/".join(f['path'].split('/')[4:])))
            else:
                message = 'There was an error in building your package.'
                if response['package_info']['status'] == Status.SYSERROR:
                    message=response['errors']['system'][0]['message']
                elif 'has changed since validation' in response['errors']:
                    message = response['errors']
                exit_error(message=message)
        else:
            message='There was an error with your package request.'
            exit_error(message=message)


    def print_replacement_summary(self):
        logger.info('Below is a summary of what your submission will look like with the validation files provided:')
        logger.info('')
        logger.info ('Short-Name, Number of Rows')
        for change in self.pending_changes:
            logger.info ('{},{}'.format(change['shortName'], change['rows']))
        logger.info('')
        logger.info('')


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'complete'
    ERROR = 'error'
    PROCESSING = 'processing'
