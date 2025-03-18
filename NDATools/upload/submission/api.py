import requests
from pydantic import BaseModel

from NDATools.Utils import get_request


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
        self.api_endpoint = self.config.submission_api
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)

    def get_submission(self, submission_id):
        tmp = get_request("/".join([self.api_endpoint, submission_id]), auth=self.auth)
        return Submission(**tmp)
