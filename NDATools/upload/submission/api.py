import enum
import time
from typing import List

import requests
from pydantic import BaseModel, Field

from NDATools.Utils import get_request, exit_error, post_request


class DataStructureDetails(BaseModel):
    short_name: str
    rows: int
    validation_uuids: List[str]
    data_structure_id: int


class SubmissionDetails(BaseModel):
    validation_uuids: List[str]
    submission_id: int
    data_structure_details: List[DataStructureDetails] = Field(..., alias='pending_changes')

    def get_data_structure_details(self, short_name):
        for data_structure in self.data_structure_details:
            if data_structure.short_name == short_name:
                return data_structure
        return None


class SubmissionHistory(BaseModel):
    replacement_authorized: bool
    created_by: int
    created_date: str


class NdaCollection(BaseModel):
    id: int
    title: str


class Submission(BaseModel):
    submission_status: str
    dataset_title: str
    dataset_description: str
    dataset_created_date: str
    dataset_modified_date: str
    submission_id: int
    collection: NdaCollection


class SubmissionApi:
    def __init__(self, config):
        self.config = config
        self.api_endpoint = self.config.submission_api_endpoint
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

    def get_submission(self, submission_id):
        tmp = get_request("/".join([self.api_endpoint, submission_id]), auth=self.auth)
        return Submission(**tmp)

    def get_submission_history(self, submission_id) -> List[SubmissionHistory]:
        try:
            tmp = get_request('/'.join([self.api_endpoint, submission_id, 'change-history']), auth=self.auth)
            return [SubmissionHistory(**t) for t in tmp]
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                exit_error(
                    message='You are not authorized to access submission {}. If you think this is a mistake, please contact NDA help desk'.format(
                        submission_id))
            else:
                exit_error(
                    message='There was a General Error communicating with the NDA server. Please try again later')
            exit(1)

    def get_submission_details(self, submission_id):
        tmp = get_request('/'.join([self.api_endpoint, submission_id, 'pending-changes']), auth=self.auth)
        return SubmissionDetails(**tmp)


class PackagingStatus(str, enum.Enum):
    SYSERROR = 'SystemError'
    COMPLETE = 'complete'
    PROCESSING = 'processing'


class SubmissionPackageInfo(BaseModel):
    status: PackagingStatus


class SubmissionPackage(BaseModel):
    package_info: SubmissionPackageInfo
    submission_package_uuid: str
    created_date: str
    expiration_date: str


class SubmissionPackageApi:
    def __init__(self, endpoint, username, password):
        self.api_endpoint = endpoint
        self.username = username
        self.password = password
        self.auth = requests.auth.HTTPBasicAuth(username, password)

    def build_package(self, collection_id, name, description, validation_uuid,
                      replace_submission_id=None) -> SubmissionPackage:
        payload = {
            "package_info": {
                "dataset_description": description,
                "dataset_name": name,
                "collection_id": collection_id
            },
            "validation_results":
                validation_uuid
        }
        if replace_submission_id:
            payload['package_info']['replacement_submission'] = replace_submission_id
        tmp = post_request(self.api_endpoint, payload=payload, auth=self.auth)
        return SubmissionPackage(**tmp)

    def wait_package_complete(self, package_id) -> SubmissionPackage:

        while True:
            time.sleep(1.1)
            response = get_request("/".join([self.api_endpoint, package_id]), auth=self.auth)
            package_status = PackagingStatus(response['package_info']['status'])
            if package_status != PackagingStatus.PROCESSING:
                # done processing. Check for erors...
                if package_status != PackagingStatus.COMPLETE:
                    message = 'There was an error in building your package.'
                    if package_status == PackagingStatus.SYSERROR:
                        message = response['errors']['system'][0]['message']
                    elif 'has changed since validation' in response['errors']:
                        message = response['errors']
                    exit_error(message=message)
                else:
                    return SubmissionPackage(**response)


class CollectionApi:
    def __init__(self, config):
        self.config = config
        self.vt_api_endpoint = self.config.validationtool_api_endpoint
        self.collection_api_endpoint = self.config.collection_api_endpoint
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

    def get_user_collections(self):
        collections = get_request("/".join([self.vt_api_endpoint, "user/collection"]), auth=self.auth,
                                  headers={'Accept': 'application/json'})
        return sorted([NdaCollection(**c) for c in collections], key=lambda x: x.id)
