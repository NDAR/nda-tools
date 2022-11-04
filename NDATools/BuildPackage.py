from __future__ import absolute_import, with_statement

import sys

import botocore
import requests.packages.urllib3.util

import NDATools

if sys.version_info[0] < 3:
    input = raw_input
import requests.packages.urllib3.util
from tqdm import tqdm
from NDATools.Configuration import *
from NDATools.Utils import *

logger = logging.getLogger(__name__)

class SubmissionPackage:
    def __init__(self, uuid, associated_files, config, username=None, password=None, collection=None, title=None,
                 description=None, alternate_location=None, allow_exit=False, pending_changes=None, original_uuids=None):
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
        self.endpoint_title = alternate_location
        self.collections = {}
        self.endpoints = []
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        self.get_collections()
        self.get_custom_endpoints()
        self.validation_results = []
        self.no_read_access = set()
        self.exit = allow_exit
        self.pending_changes = pending_changes
        self.original_validation_uuids = original_uuids

    def get_collections(self):
        collections = get_request("/".join([self.validationtool_api, "user/collection"]), auth=self.auth, headers={'Accept':'application/json'})
        if collections:
            for c in collections:
                self.collections.update({c['id']: c['title']})

    def get_custom_endpoints(self):
        endpoints = get_request("/".join([self.validationtool_api, "user/customEndpoints"]), auth=self.auth, headers={'Accept':'application/json'})
        if endpoints:
            for endpoint in endpoints:
                self.endpoints.append(endpoint['title'])

    def set_upload_destination(self, hide_input=True):
        if len(self.collections) > 0 or len(self.endpoints) > 0:
            user_input = None
            if self.config.collection_id and self.config.endpoint_title:
                logger.info('You selected both a collection and an alternate endpoint.\n'
                      'These options are mutually exclusive, please specify only one.')
            elif self.config.endpoint_title:
                self.endpoint_title = self.config.endpoint_title
            elif self.config.collection_id:
                self.collection_id = self.config.collection_id
            else:
                if not hide_input:
                    user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
            while True:
                if user_input:
                    try:
                        if user_input.startswith('-c'):
                            collection_id = user_input.split(" ")
                            self.collection_id = collection_id[1]
                            self.endpoint_title = ""
                        elif user_input.startswith('-a'):
                            endpoint_title = user_input.split(" ")
                            self.endpoint_title = endpoint_title[1]
                            self.collection_id = ""
                    except IndexError:
                        logger.info('Do not leave this section blank.')
                try:
                    if self.collection_id and int(self.collection_id) in self.collections:
                        break
                    elif self.endpoint_title in self.endpoints:
                        break
                    else:
                        if not hide_input:
                            print ('Collection IDs:')
                            for k, v in self.collections.items():
                                logger.info('{}: {}'.format(k, v.encode('ascii', 'ignore')))
                            logger.info('\nAlternate Endpoints:')
                            for endpoint in self.endpoints:
                                logger.info(endpoint.encode('ascii', 'ignore'))
                            message = '\nYou do not have access to submit to the collection or alternate upload location: {} '.format(self.collection_id or self.endpoint_title)
                            logger.info(message)
                            user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint> from the list above:')
                        else:
                            message = 'Incorrect/Missing collection ID or alternate endpoint.'
                            if self.exit:
                                exit_client(signal=signal.SIGTERM,
                                            message=message)
                            else:
                                raise Exception(message)
                except (AttributeError, ValueError, TypeError) as e:
                    message = 'Error: Input must start with either a -c or -a and be an integer or string value, respectively.'
                    if not hide_input:
                        logger.info(message)
                        user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
                    else:
                        message = 'Incorrect/Missing collection ID or alternate endpoint.'
                        if self.exit:
                            exit_client(signal=signal.SIGTERM, message=message)
                        else:
                            raise Exception(message)
        else:
            message = 'The user {} does not have permission to submit to any collections or alternate upload locations.'.format(self.config.username)
            if self.exit:
                exit_client(signal=signal.SIGTERM, message=message)
            else:
                raise Exception(message)

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
                    exit_client()
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

        if self.collection_id is None and self.endpoint_title is None:
            raise_error('collection ID or alternate endpoint')

        self.package_info = {
            "package_info": {
                "dataset_description": self.dataset_description,
                "dataset_name": self.dataset_name,
                "collection_id": self.collection_id,
                "endpoint_title": self.endpoint_title
            },
            "validation_results":
                self.uuid
        }
        if hasattr(self.config, 'replace_submission'):
            self.package_info['package_info']['replacement_submission'] = self.config.replace_submission
            self.print_replacement_summary()
            if not self.config.force:
                user_input = evaluate_yes_no_input("Are you sure you want to continue?", 'n')
                if user_input.lower() == 'n':
                    exit_client(signal.SIGTERM, message='Exiting...')


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
                if self.exit:
                    exit_client(signal.SIGTERM, message=message)
                else:
                    raise Exception(message)

            polling = 0
            while response['package_info']['status'] == Status.PROCESSING:
                response = get_request("/".join([self.api, self.package_id]), auth=self.auth)
                polling += 1
                self.package_id = response['submission_package_uuid']
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
                if self.exit:
                    exit_client(signal.SIGTERM, message=message)
                else:
                    raise Exception(message)
        else:
            message='There was an error with your package request.'
            if self.exit:
                exit_client(signal.SIGTERM, message=message)
            else:
                raise Exception(message)

    def download_package(self, hide_progress):
        folder = self.download_links[0][1]
        folder = folder.split('/')
        self.package_folder = folder[0]
        session = requests.session()
        total_package_size = 0
        for i, (url, file_name) in enumerate(self.download_links):
            r = session.get(url, auth=(self.username, self.password), stream=True)
            size = r.headers['content-length']
            total_package_size += int(size)
            self.download_links[i] = (url, file_name, int(size))
            r.close()
        if not hide_progress:
            package_download = tqdm(total=total_package_size,
                                    unit="bytes",
                                    unit_scale=True,
                                    desc="Submission Package Download",
                                    ascii=os.name == 'nt')
        # logger.info('dl_links: {}'.format(self.download_links))
        for url, file_name, size in self.download_links:
            path = os.path.join(NDATools.NDA_TOOLS_SUB_PACKAGE_FOLDER, self.package_folder)
            if not os.path.exists(path):
                os.mkdir(path)
            file_name = file_name.split('/')
            file_name = file_name[1]
            file = os.path.join(path, file_name)
            r = session.get(url, auth=(self.username, self.password), stream=True)
            with open(file, 'wb') as out_file:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out_file.write(chunk)
                        if not hide_progress:
                            package_download.update(sys.getsizeof(chunk))
            session.close()
        if not hide_progress:
            if package_download.total > package_download.n:
                package_download.update(package_download.total - package_download.n)
            package_download.close()

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
