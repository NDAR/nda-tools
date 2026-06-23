import datetime
import enum
import json
import logging
import time
from typing import List, Union

import requests
from pydantic import BaseModel, Field
from requests import HTTPError

from NDATools import exit_error
from NDATools.Utils import get_request, post_request, HttpErrorHandlingStrategy, DeserializeHandler, \
    put_request

logger = logging.getLogger(__name__)


class DataStructureDetails(BaseModel):
    short_name: str = Field(..., alias='shortName')
    rows: int
    validation_uuids: List[str] = Field(..., alias='validationUuids')


class SubmissionDetails(BaseModel):
    validation_uuids: List[str]
    submission_id: int = Field(..., alias='submissionId')
    data_structure_details: List[DataStructureDetails] = Field(..., alias='pendingChanges')

    def get_data_structure_details(self, short_name):
        for data_structure in self.data_structure_details:
            if data_structure.short_name == short_name:
                return data_structure
        return None


class SubmissionHistory(BaseModel):
    replacement_authorized: bool
    created_by: str
    created_date: str


class NdaCollection(BaseModel):
    id: int
    title: str


class AssociatedFileUploadCreds(BaseModel):
    id: int = Field(..., alias='submissionFileId')
    destination_uri: str
    source_uri: Union[str, None]
    access_key: str
    secret_key: str
    session_token: str


class AssociatedFileStatus(str, enum.Enum):
    READY = "Ready",
    INPROGRESS = "In Progress"
    COMPLETE = "Complete"


class AssociatedFile(BaseModel):
    id: int
    file_user_path: str
    file_remote_path: str
    status: AssociatedFileStatus
    size: int


class SubmissionStatus(str, enum.Enum):
    UPLOADING = 'Uploading'
    SUBMITTED_PROTOTYPE = 'Submitted_Prototype'
    SUBMITTED = 'Submitted'
    PROCESSING = 'Processing'
    COMPLETE = 'Upload Completed'
    ERROR_ON_DATA_LOAD = 'Error on Data Load'


class Submission(BaseModel):
    status: SubmissionStatus = Field(..., alias='submission_status')
    dataset_title: str
    dataset_description: str
    dataset_created_date: str
    dataset_modified_date: Union[str, None]
    submission_id: int
    collection: NdaCollection


class UploadProgress(BaseModel):
    associated_file_count: int
    uploaded_file_count: int


class SubmissionApi:
    def __init__(self, submission_api_endpoint, auth, reauth_func=None, create_submission_timeout=300, batch_size=50):
        self.api_endpoint = submission_api_endpoint
        self.auth = auth
        self.reauth_func = reauth_func
        self.create_submission_timeout = create_submission_timeout

    def get_submission(self, submission_id: int):
        tmp = get_request("/".join([self.api_endpoint, str(submission_id)]), auth=self.auth,
                          reauth_func=self.reauth_func)
        return Submission(**tmp)

    def get_submission_history(self, submission_id: int) -> List[SubmissionHistory]:
        try:
            tmp = get_request('/'.join([self.api_endpoint, str(submission_id), 'change-history']), auth=self.auth,
                              reauth_func=self.reauth_func)
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

    def get_submission_details(self, submission_id: int) -> SubmissionDetails:
        tmp = get_request('/'.join([self.api_endpoint, str(submission_id), 'pending-changes']), auth=self.auth,
                          reauth_func=self.reauth_func)
        return SubmissionDetails(**tmp)

    def create_submission(self, package_id: str) -> Submission:
        post_request("/".join([self.api_endpoint, package_id]) + "?async=true", auth=self.auth,
                     deserialize_handler=DeserializeHandler.none, reauth_func=self.reauth_func)
        return self._wait_submission_complete(package_id)

    def get_upload_credentials(self, submission_id, file_ids) -> List[AssociatedFileUploadCreds]:
        credentials_list = post_request("/".join(
            [self.api_endpoint, str(submission_id), 'files/batchMultipartUploadCredentials']),
            payload=json.dumps(file_ids), auth=self.auth, reauth_func=self.reauth_func)
        return [AssociatedFileUploadCreds(**c) for c in credentials_list['credentials']]

    def get_upload_progress(self, submission_id):
        response = get_request("/".join([self.api_endpoint, str(submission_id), "upload-progress"]), auth=self.auth,
                               reauth_func=self.reauth_func)
        return UploadProgress(**response)

    def replace_submission(self, submission_id, package_id):
        version_count = len(self.get_submission_history(submission_id))
        put_request(
            f"{self.api_endpoint}/{submission_id}?submissionPackageUuid={package_id}&async=true",
            auth=self.auth, deserialize_handler=DeserializeHandler.none, reauth_func=self.reauth_func)
        # poll the versions endpoint until a new one is created or until we timeout
        end_time = datetime.timedelta(seconds=self.create_submission_timeout) + datetime.datetime.now()
        while True:
            if datetime.datetime.now() > end_time:
                logger.error("Timed out waiting for submission to replace.")
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
            new_version_count = len(self.get_submission_history(submission_id))
            if new_version_count > version_count:
                return self.get_submission(submission_id)
            time.sleep(10)

    def get_files_by_page(self, submission_id, page_number, page_size, exclude_uploaded=True):
        excluded_q_param = f'&omitCompleted=true' if exclude_uploaded else ''
        try:
            get_files_url = "/".join([self.api_endpoint, str(submission_id),
                                      f'file-listing?pageNumber={page_number}&pageSize={page_size}{excluded_q_param}'])
            response = get_request(get_files_url, auth=self.auth, error_handler=HttpErrorHandlingStrategy.ignore,
                                   deserialize_handler=DeserializeHandler.none, reauth_func=self.reauth_func)
            response.raise_for_status()
            return [AssociatedFile(**f) for f in response.json()]
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 400 and 'Cannot navigate past last page' in error.response.text:
                # we got passed the last page
                return []
            else:
                logger.error(error.response.text)
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
        except requests.exceptions.RequestException as error:
            logger.error(f'Error retrieving submission file listing: {error}')
            logger.error('This was a network error while listing associated files. Please try again.')
            logger.error('\nIf the error persists, please email NDAHelp@mail.nih.gov for help in resolving this error')
            exit_error()

    def _wait_submission_complete(self, package_id):
        # poll the versions endpoint until a new one is created or until we timeout
        end_time = datetime.timedelta(seconds=self.create_submission_timeout) + datetime.datetime.now()
        while True:
            if datetime.datetime.now() > end_time:
                logger.error("Timed out waiting for submission to get created.")
                logger.error('\nPlease email NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
            submission = self._query_submissions_by_package_id(package_id)
            if submission:
                logger.debug(f"Submission: {submission.submission_id}")
                return submission
            time.sleep(10)

    def _query_submissions_by_package_id(self, package_id):
        tmp = get_request(f"{self.api_endpoint}?packageUuid={package_id}", auth=self.auth,
                          reauth_func=self.reauth_func)
        if tmp:
            return Submission(**tmp[0])
        return None


class PackagingStatus(str, enum.Enum):
    SYSERROR = 'SystemError'
    COMPLETE = 'complete'
    PROCESSING = 'processing'


class SubmissionPackage(BaseModel):
    submission_package_uuid: str
    created_date: str
    expiration_date: str
    status: PackagingStatus


class SubmissionPackageApi:
    def __init__(self, endpoint, auth, reauth_func=None):
        self.api_endpoint = endpoint
        self.auth = auth
        self.reauth_func = reauth_func

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
        tmp = post_request(self.api_endpoint, payload=payload, auth=self.auth, reauth_func=self.reauth_func)
        return SubmissionPackage(**tmp)

    def wait_package_complete(self, package_id) -> SubmissionPackage:

        while True:
            time.sleep(1.1)
            response = get_request("/".join([self.api_endpoint, package_id]), auth=self.auth,
                                   reauth_func=self.reauth_func)
            package_status = PackagingStatus(response['status'])
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
    def __init__(self, vt_api_endpoint, auth, reauth_func=None):
        self.vt_api_endpoint = vt_api_endpoint
        self.auth = auth
        self.reauth_func = reauth_func

    def get_user_collections(self):
        collections = get_request("/".join([self.vt_api_endpoint, "user/collection"]), auth=self.auth,
                                  headers={'Accept': 'application/json'}, reauth_func=self.reauth_func)
        return sorted([NdaCollection(**c) for c in collections], key=lambda x: x.id)


class RasAuthApi:
    def __init__(self, ras_login_api_endpoint):
        self.ras_login_api_endpoint = ras_login_api_endpoint

    def login(self, username, password):
        auth = requests.auth.HTTPBasicAuth(username, password)
        try:
            response = post_request(self.ras_login_api_endpoint,
                                    auth=auth,
                                    deserialize_handler=DeserializeHandler.none,
                                    error_handler=HttpErrorHandlingStrategy.reraise_status)
            return response.text
        except HTTPError as e:
            if e.response.status_code == 423:
                msg = '''
Your account is locked, which is preventing your authorized access to nda-tools. To unlock your account, set a new password by doing the following:
1. Log into NDA (https://nda.nih.gov) using your RAS credentials (eRA Commons, Login.gov, or Smart Card/CAC)')
2. Navigate to your NDA profile (https://nda.nih.gov/user/dashboard/profile)')
3. Click on the 'Update Password' button, found near the upper right corner of the page')
4. Set a new password. Once your password is successfully reset, your account will be unlocked.'''
                exit_error(message=msg)
            elif e.response.status_code == 401:
                return None
            else:
                msg = f'\nSystem Error while checking credentials for user {username}'
                msg += '\nPlease contact NDAHelp@mail.nih.gov for help in resolving this error'
                exit_error(message=msg)
