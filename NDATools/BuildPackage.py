from __future__ import with_statement
from __future__ import absolute_import
import sys
import requests.packages.urllib3.util
from tqdm import tqdm
import boto3
import botocore

if sys.version_info[0] < 3:
    input = raw_input


from NDATools.Configuration import *

class SubmissionPackage:
    def __init__(self, uuid, associated_files, username=None, password=None, collection=None, title=None,
                 description=None, alternate_location=None, config=None):
        if config:
            self.config = config
        else:
            self.config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
            self.config.username = username
            self.config.password = password
        self.aws_access_key = self.config.aws_access_key
        self.aws_secret_key = self.config.aws_secret_key
        self.api = self.config.submission_package_api
        self.validationtool_api = self.config.validationtool_api
        self.uuid = uuid
        self.associated_files = associated_files
        self.full_file_path = {}
        self.username = self.config.username
        self.password = self.config.password
        self.no_match = []
        if title:
            self.dataset_name = title
        elif self.config.title:
            self.dataset_name = self.config.title
        else:
            print('\nYou must enter a title for dataset name.')
            sys.exit(1)
        if description:
            self.dataset_description = description

        elif self.config.description:
            self.dataset_description = self.config.description
        else:
            print('\nYou must enter a description for dataset submission.')
            sys.exit(1)
        self.package_info = {}
        self.download_links = []
        self.package_id = None
        self.package_folder = None
        self.collection_id = collection
        self.endpoint_title = alternate_location
        self.collections = {}
        self.endpoints = []
        self.get_collections()
        self.get_custom_endpoints()
        self.validation_results = []
        if not self.config.submission_packages:
            self.config.submission_packages = 'NDASubmissionPackages'
        self.submission_packages_dir = os.path.join(os.path.expanduser('~'), self.config.submission_packages)
        if not os.path.exists(self.submission_packages_dir):
            os.mkdir(self.submission_packages_dir)

    def get_collections(self):
        collections, session = api_request(self,
                                           "GET",
                                           "/".join([self.validationtool_api, "user/collection"]))
        if collections:
            for c in collections:
                self.collections.update({c['id']: c['title']})

    def get_custom_endpoints(self):
        endpoints, session = api_request(self,
                                         "GET",
                                         "/".join([self.validationtool_api, "user/customEndpoints"]))
        if endpoints:
            for endpoint in endpoints:
                self.endpoints.append(endpoint['title'])

    def set_upload_destination(self, hide_input=True):
        if len(self.collections) > 0 or len(self.endpoints) > 0:
            user_input = None
            if self.config.collection_id and self.config.endpoint_title:
                print('You selected both a collection and an alternate endpoint.\n'
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
                        print('Do not leave this section blank.')
                try:
                    if self.collection_id and int(self.collection_id) in self.collections:
                        break
                    elif self.endpoint_title in self.endpoints:
                        break
                    else:
                        print('Collection IDs:')
                        for k, v in self.collections.items():
                            print('{}: {}'.format(k, v.encode('ascii', 'ignore')))
                        print('\nAlternate Endpoints:')
                        for endpoint in self.endpoints:
                            print(endpoint.encode('ascii', 'ignore'))
                        print('\nYou do not have access to submit to the collection or alternate upload location: {} '.
                              format(self.collection_id or self.endpoint_title))
                        if not hide_input:
                            user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint> from the list above:')
                        else:
                            sys.exit(1)
                except (AttributeError, ValueError, TypeError) as e:
                    if not hide_input:
                        print(
                            'Error: Input must start with either a -c or -a and be an integer or string value, respectively.')
                        user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
                    else:
                        sys.exit(1)
        else:
            exit_client(signal=signal.SIGINT,
                        message='The user {} does not have permission to submit to any collections'
                                ' or alternate upload locations.'.format(self.config.username))

    def file_search(self, directories, source_bucket, source_prefix, access_key, secret_key, retry_allowed=False):
        print('\nSearching for associated files...')

        s3 = False
        self.directory_list = directories
        self.source_bucket = source_bucket

        if self.source_bucket:
            s3 = True
        self.source_prefix = source_prefix
        self.aws_access_key = access_key
        self.aws_secret_key = secret_key

        if not self.directory_list and not self.source_bucket:
            retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                          '<bucket name> to locate your associated files:')
            response = retry.split(' ')
            self.directory_list = response
            if response[0] == '-s3':
                s3 = True
                self.source_bucket = response[1]
                self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
                if self.source_prefix == "":
                    self.source_prefix = None
                if self.aws_access_key == "":
                    self.aws_access_key = input('Enter the access_key for your AWS account: ')
                if self.aws_secret_key == "":
                    self.aws_secret_key = input('Enter the secret_key for your AWS account: ')

                self.config.source_bucket = self.source_bucket
                self.config.source_prefix = self.source_prefix
                self.config.aws_access_key = self.aws_access_key
                self.config.aws_secret_key = self.aws_secret_key
                self.directory_list = None

        if not self.no_match:
            for a in self.associated_files:
                for file in a:
                    self.no_match.append(file)

        # local files
        if self.directory_list:
            for file in self.no_match[:]:
                if file.startswith('/'):
                    f = file[1:]
                else:
                    f = file
                for d in self.directory_list:
                    file_name = os.path.join(d, f)
                    if os.path.isfile(file_name):
                        self.full_file_path[file] = (file_name, os.path.getsize(file_name))
                        self.no_match.remove(file)
                        break

        # files in s3
        if s3:
            no_access_buckets = set()
            if self.aws_access_key is "":
                self.aws_access_key = input('Enter the access_key for your AWS account: ')
            if self.aws_secret_key is "":
                self.aws_secret_key = input('Enter the secret_key for your AWS account: ')
            s3 = boto3.session.Session(aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
            s3_client = s3.client('s3')
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
                        #print('This path is incorrect:', file_name, 'Please try again.')
                        pass
                    if error_code == 403:
                        no_access_buckets.add(self.source_bucket)
                        #print('You do not have access to this bucket:', self.source_bucket)
                        pass


            if no_access_buckets:
                print('\nNote: your user does NOT have access to the following buckets. Please review the bucket and/or your '
                      'AWS credentials and try again.')
                for b in no_access_buckets:
                    print(b)

        for file in self.no_match:
            print('Associated file not found in specified directory:', file)

        if self.no_match:
            print('\nYou must make sure all associated files listed in your validation file'
                ' are located in the specified directory or AWS bucket. Please try again.')
            if retry_allowed:
                retry = input(
                    'Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                    '<bucket name> to locate your associated files:')  # prefix and profile can be blank (None)
                response = retry.split(' ')
                self.directory_list = response
                if response[0] == '-s3':
                    self.source_bucket = response[1]
                    self.source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
                    if self.source_prefix == "":
                        self.source_prefix = None
                    self.aws_access_key = input('Enter the access_key for your AWS account: ')
                    self.aws_secret_key = input('Enter the secret_key for your AWS account: ')
                    self.directory_list = None


                self.config.source_bucket = self.source_bucket
                self.config.source_prefix = self.source_prefix
                self.config.aws_access_key = self.aws_access_key
                self.config.aws_secret_key = self.aws_secret_key
                self.file_search(self.directory_list, self.source_bucket, self.source_prefix, self.aws_access_key,self.aws_secret_key,
                                 retry_allowed=True)
            else:
                print("\nYou did not enter all the correct directories or AWS buckets. Try again.")
                sys.exit(1)


    def build_package(self):
        self.package_info = {
            "package_info": {
                "dataset_description": self.dataset_name,
                "dataset_name": self.dataset_description,
                "collection_id": self.collection_id,
                "endpoint_title": self.endpoint_title
            },
            "validation_results":
                self.uuid
        }

        json_data = json.dumps(self.package_info)
        response, session = api_request(self, "POST", self.api, json_data)
        if response:
            try:
                self.package_id = response['submission_package_uuid']
                #for r in response['validation_results']:
                #    self.validation_results.append(r['id'])
                self.validation_results = response['validation_results']
                self.submission_package_uuid = str(response['submission_package_uuid'])
                self.create_date = str(response['created_date'])
                self.expiration_date = str(response['expiration_date'])
            except KeyError:
                if response['status'] == 'error':
                    exit_client(signal.SIGINT, message=response['errors'][0]['message'])

                else:
                    exit_client(signal=signal.SIGINT,
                            message='There was an error creating your package.')
            polling = 0
            while response['package_info']['status'] == 'processing':
                response, session = api_request(self, "GET", "/".join([self.api, self.package_id]), session=session)
                polling += 1
                self.package_id = response['submission_package_uuid']
            if response['package_info']['status'] == 'complete':
                for f in [f for f in response['files']
                          if f['type'] in ('Submission Memento', 'Submission Data Package')]:
                    for key, value in f['_links'].items():
                        for k, v in value.items():
                            self.download_links.append((v, "/".join(f['path'].split('/')[4:])))
            else:
                if response['package_info']['status'] == 'SystemError':
                    exit_client(signal.SIGINT, message=response['errors']['system'][0]['message'])
                exit_client(signal=signal.SIGINT,
                            message='There was an error in building your package.')
        else:
            exit_client(signal=signal.SIGINT,
                        message='There was an error with your package request.')

    def download_package(self, hide_progress=True):
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
        for url, file_name, size in self.download_links:
            new_path = os.path.join(os.path.expanduser('~'), self.config.submission_packages)
            path = os.path.join(new_path, self.package_folder)
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