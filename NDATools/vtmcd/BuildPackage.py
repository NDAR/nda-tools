from __future__ import absolute_import
from __future__ import with_statement

import sys

import botocore
import requests.packages.urllib3.util

from NDATools.s3.S3Authentication import S3Authentication

if sys.version_info[0] < 3:
    input = raw_input
import requests.packages.urllib3.util
from tqdm import tqdm
from NDATools.vtmcd.Configuration import *
from NDATools.utils.Utils import *


class SubmissionPackage:
    def __init__(self, validation_uuids, associated_files, config, username=None, password=None, collection=None, title=None,
                 description=None, alternate_location=None):
        self.config = config
        self.api = self.config.submission_package_api
        self.validationtool_api = self.config.validationtool_api
        self.validation_uuids = validation_uuids
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
        self.credentials = S3Authentication(config)
        self.dataset_name = self.config.title
        self.dataset_description = self.config.description
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
        self.no_read_access = set()

    def get_collections(self):
        collections = advanced_request(self.validationtool_api + '/user/collection', username=self.username, password=self.password)
        if collections:
            for c in collections:
                self.collections.update({c['id']: c['title']})

    def get_custom_endpoints(self):
        endpoints = advanced_request(self.validationtool_api + '/user/customEndpoints',
                                     username=self.username,
                                     password=self.password)
        if endpoints:
            for endpoint in endpoints:
                self.endpoints.append(endpoint['title'])

    def post_package_request(self):
        payload = {
            "package_info": {
                "dataset_description": self.dataset_description,
                "dataset_name": self.dataset_name
            },
            "validation_results":
                self.validation_uuids
        }
        if self.collection_id:
            payload['package_info']['collection_id']=self.collection_id
        else:
            payload['package_info']['endpoint_title'] = self.endpoint_title

        response = advanced_request(self.api, verb=Verb.POST, data=payload, username=self.username, password=self.password)
        return response

    def get_package(self):
        # raise for status
        response = advanced_request(self.api + '/{}', path_params=[self.package_id], username=self.username, password=self.password)
        return response

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
                        if not hide_input:
                            print ('Collection IDs:')
                            for k, v in self.collections.items():
                                print('{}: {}'.format(k, v.encode('ascii', 'ignore')))
                            print('\nAlternate Endpoints:')
                            for endpoint in self.endpoints:
                                print(endpoint.encode('ascii', 'ignore'))
                            message = '\nYou do not have access to submit to the collection or alternate upload location: {} '.format(self.collection_id or self.endpoint_title)
                            print(message)
                            user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint> from the list above:')
                        else:
                            message = 'Incorrect/Missing collection ID or alternate endpoint.'
                            exit_client(signal=signal.SIGTERM, message=message)

                except (AttributeError, ValueError, TypeError) as e:
                    message = 'Error: Input must start with either a -c or -a and be an integer or string value, respectively.'
                    if not hide_input:
                        print(message)
                        user_input = input('\nEnter -c <collection ID> OR -a <alternate endpoint>:')
                    else:
                        message = 'Incorrect/Missing collection ID or alternate endpoint.'
                        exit_client(signal=signal.SIGTERM, message=message)
        else:
            message = 'The user {} does not have permission to submit to any collections or alternate upload locations.'.format(self.config.username)
            exit_client(signal=signal.SIGTERM, message=message)


    def build_package(self):
        def raise_error(value):
            raise Exception("Missing {}. Please try again.".format(value))

        if self.dataset_name is None:
            raise_error('dataset name')

        if self.dataset_description is None:
            raise_error('dataset description')

        if self.collection_id is None and self.endpoint_title is None:
            raise_error('collection ID or alternate endpoint')

        package_response = self.post_package_request()
        self.package_id = package_response['submission_package_uuid']
        for r in package_response['validation_results']:
            self.validation_results.append(r['id'])
        self.submission_package_uuid = str(package_response['submission_package_uuid'])
        self.create_date = str(package_response['created_date'])
        self.expiration_date = str(package_response['expiration_date'])

        polling = 0
        while package_response['package_info']['status'] == Status.PROCESSING:
            package_response = self.get_package()
            polling += 1
            self.package_id = package_response['submission_package_uuid']
        if package_response['package_info']['status'] == Status.COMPLETE:
            for f in [f for f in package_response['files']
                      if f['type'] in ('Submission Memento', 'Submission Data Package')]:
                for key, value in f['_links'].items():
                    for k, v in value.items():
                        self.download_links.append((v, "/".join(f['path'].split('/')[4:])))
        else:
            message = 'There was an error in building your package.'
            if package_response['package_info']['status'] == Status.SYSERROR:
                message=package_response['errors']['system'][0]['message']
            exit_client(signal.SIGTERM, message=message)

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


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'complete'
    ERROR = 'error'
    PROCESSING = 'processing'
