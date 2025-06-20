import json
import logging
import os
import pathlib
import time
from threading import RLock
from typing import Union, List

import boto3
import requests
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError

from NDATools import exit_error
from NDATools.Utils import get_request, post_request

logger = logging.getLogger(__name__)


class NdaCredentials(BaseModel):
    access_key_id: str
    secret_access_key: str
    session_token: str

    def __init__(self, **data):
        super().__init__(**data)
        self._s3_cli = boto3.client('s3',
                                    aws_access_key_id=self.access_key_id,
                                    aws_secret_access_key=self.secret_access_key,
                                    aws_session_token=self.session_token)
        self._s3_transfer = boto3.s3.transfer.S3Transfer(self._s3_cli)

    def download(self, s3_url: str) -> str:
        assert s3_url.startswith('s3://')
        bucket, key = s3_url.replace("s3://", "").split("/", 1)
        return self._s3_cli.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

    def upload(self, file: Union[pathlib.Path, str], s3_url: str):
        assert s3_url.startswith('s3://')
        bucket, key = s3_url.replace("s3://", "").split("/", 1)
        self._s3_transfer.upload_file(str(file), bucket, key)
        logger.debug(f'Finished uploading {file} to {s3_url}')


def handle_expired(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ExpiredToken':
                logger.warning('Credentials expired. Refreshing...')

                ak = self.access_key_id
                st = self.session_token
                with self._refresh_lock:
                    if ak != self.access_key_id or st != self.session_token:
                        logger.debug('Credentials refreshed by another thread. Skipping refresh.')
                    else:
                        logger.debug('Refreshing credentials...')
                        tmp = self._refresh_func()
                        logger.debug('Finished refreshing credentials. ')
                        logger.debug(
                            f'Old AK/SK/ST: {self.access_key_id} / {self.secret_access_key} / {self.session_token}')
                        self.access_key_id = tmp.access_key_id
                        self.secret_access_key = tmp.secret_access_key
                        self.session_token = tmp.session_token
                        logger.debug(
                            f'New AK/SK/ST: {self.access_key_id} / {self.secret_access_key} / {self.session_token}')
                return func(self, *args, **kwargs)
            else:
                logger.debug(f'Unexpected error code: {error_code}: {e}')
                logger.debug(
                    f'Current AK/SK/ST: {self.access_key_id}/{self.secret_access_key}/{self.session_token}')
                raise e

    return wrapper


class AutoRefreshableCredentials(NdaCredentials):
    def __init__(self, refresh_func, **data):
        super().__init__(**data)
        self._refresh_func = refresh_func
        self._refresh_lock = RLock()

    @handle_expired
    def upload(self, file: Union[pathlib.Path, str], s3_url: str):
        super().upload(file, s3_url)

    @handle_expired
    def download(self, s3_url: str):
        return super().download(s3_url)


CSV_DATA_KEY = 'csv data'


class ValidationV2Credentials(AutoRefreshableCredentials):
    uuid: str = Field(..., alias='validation_uuid')
    read_write_permission: dict
    read_permission: dict

    def download_warnings(self):
        return json.loads(self.download(self.read_permission['warnings json']))

    def download_associated_files(self):
        return json.loads(self.download(self.read_permission['associated files json']))['associatedFiles']

    def download_manifests(self):
        return json.loads(self.download(self.read_permission['manifest json']))['manifests']

    def download_metadata(self):
        return json.loads(self.download(self.read_permission['metadata json']))

    def download_errors(self):
        return json.loads(self.download(self.read_permission['errors json']))

    def download_csv(self):
        if CSV_DATA_KEY in self.read_permission:
            return json.loads(self.download(self.read_permission[CSV_DATA_KEY]))
        else:
            return json.loads(self.download(self.read_write_permission[CSV_DATA_KEY]))

    def upload_csv(self, file: Union[pathlib.Path, str]):
        csv_s3_url = self.read_write_permission[CSV_DATA_KEY]
        self.upload(file, csv_s3_url)


class ManifestError(BaseModel):
    uuid: str
    errors: List[str]


class ValidationV2(BaseModel):
    uuid: str = Field(..., alias='validation_uuid')
    status: str
    short_name: Union[str, None]
    scope: Union[int, None]
    rows: Union[int, None]
    validation_files: dict


class ValidationManifest(BaseModel):
    local_file_name: str = Field(..., alias='localFileName')
    record_number: int = Field(..., alias='recordNumber')
    header: str
    uuid: str
    s3_destination: str = Field(..., alias='s3Destination')
    _validation_response = None

    def __init__(self, **data):
        super().__init__(**data)
        # this field is not present in the manifests file but is added to make the code simpler
        self._validation_response = data['validation_response']

    @property
    def validation_response(self):
        return self._validation_response


class ValidationV2Api:
    def __init__(self, validation_api_endpoint, username, password):
        self.api_v1_endpoint = f"{validation_api_endpoint}"
        self.api_v2_endpoint = f"{validation_api_endpoint}/v2/"
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self._refresh_creds_lock = RLock()

    def request_upload_credentials(self, file_name: str, scope=None) -> ValidationV2Credentials:
        payload = {
            "validation_file": os.path.basename(file_name)
        }
        if scope:
            payload["scope"] = scope
        tmp = post_request(self.api_v2_endpoint, auth=self.auth, payload=payload)
        return self._get_refreshable_credentials(tmp)

    def _get_refreshable_credentials(self, creds: dict):
        try:
            return ValidationV2Credentials(lambda: self.refresh_upload_credentials(creds['validation_uuid']), **creds)
        except ValidationError as v:
            for e in v.errors():
                logger.error(f'Error parsing credentials: {e}')
                logger.error(f'Credentials: {creds}')
                raise v

    def refresh_upload_credentials(self, uuid):
        url = f"{self.api_v2_endpoint}{uuid}/refresh-credentials"
        tmp = post_request(url, auth=self.auth)
        return self._get_refreshable_credentials(tmp)

    def request_download_credentials(self, uuid: str) -> ValidationV2Credentials:
        url = f"{self.api_v2_endpoint}{uuid}/download"
        tmp = get_request(url, auth=self.auth)
        return self._get_refreshable_credentials(tmp)

    def get_validation(self, uuid: str) -> ValidationV2:
        url = f"{self.api_v2_endpoint}{uuid}"
        tmp = get_request(url, auth=self.auth)
        return ValidationV2(**tmp)

    def get_manifest_errors(self, uuid: str) -> List[ManifestError]:
        url = f"{self.api_v2_endpoint}{uuid}/manifests/errors"
        results = []
        page = 0
        while True:
            tmp = get_request(f"{url}?page={page}", auth=self.auth)
            if not tmp:
                break
            results.extend([ManifestError(**t) for t in tmp])
            page += 1
        return results

    def wait_validation_complete(self, uuid, timeout_seconds, wait_manifest_upload=False) -> ValidationV2:
        timeout = time.time() + timeout_seconds
        poll_interval_sec = 1
        while True:
            validation = self.get_validation(uuid)
            status = validation.status.lower()

            if 'complete' in status:
                break
            elif 'pending' in status and not wait_manifest_upload:
                break
            elif 'error' in status:
                exit_error()
            else:
                time.sleep(poll_interval_sec)  # Wait before checking again
                poll_interval_sec = min(poll_interval_sec * 1.5, 10)
                if time.time() > timeout:
                    logger.error(f"Validation timed out for uuid {uuid}")
                    exit_error()
        return validation

    def get_v2_routing_percent(self):
        api_config = get_request(f'{self.api_v1_endpoint}/config')
        return api_config['v2Routing']['percent']
