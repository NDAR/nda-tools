from typing import List

import requests
from pydantic import BaseModel, Field

from NDATools.Utils import get_request, exit_error


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
