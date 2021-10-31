from __future__ import absolute_import, with_statement

from datetime import datetime
import json
import logging
import multiprocessing
import signal
import sys
import time

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
import botocore
from botocore.client import Config
import requests

from NDATools.s3.MultiPartUpload import MultiPartsUpload, UploadMultiParts
from NDATools.s3.S3Authentication import S3Authentication
from NDATools.utils import Utils
from NDATools.utils.ThreadPool import ThreadPool
from NDATools.utils.Utils import advanced_request, exit_client, get_stack_trace, sanitize_file_path, \
    Verb

if sys.version_info[0] < 3:

    input = raw_input
else:
    pass
from tqdm import tqdm
from NDATools.vtmcd.Configuration import *


class Status:
    UPLOADING = 'Uploading'
    SUBMITTED = 'Submitted'
    PROCESSING = 'Processing'
    LOADERROR = 'Error on Data Load'
    UPLOAD_COMPLETE = 'Upload Completed'
    INVESTIGATOR_APPROVED = 'Investigator Approved'
    DAC_APPROVED = 'DAC Approved'
    SUBMITTED_PROTOTYPE = 'Submitted_Prototype'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    IN_PROGRESS = 'In Progress'
    READY = 'Ready'


class Submission:
    def __init__(self, submission_config, thread_num=None, batch_size=None, submission_id=None, submission_files=[]):
        self.config = submission_config
        self.submission_id = submission_id
        self.api = self.config.submission_api
        self.username = self.config.username
        self.password = self.config.password
        self.thread_num = max([1, multiprocessing.cpu_count() - 1])
        if thread_num:
            self.thread_num = thread_num
        self.batch_size = 10000
        if batch_size:
            self.batch_size = batch_size
        self.directory_list = self.config.directory_list
        self.submission_file_info = {f.csv_path: f for f in submission_files}

        # init state
        self.status = None

    def __authenticated_request(self, **kwargs):
        return advanced_request(**kwargs, username=self.config.username, password=self.config.password)

    def update_submission_file_status(self, file, status):
        self.__authenticated_request(endpoint=self.api + '/{}/files/{}',
                                     verb=Verb.PUT,
                                     path_params=[self.submission_id, file['id']],
                                     query_params={'submissionFileStatus': status})

    def create_submission(self, package_id):
        response = self.__authenticated_request(endpoint=self.api + '/{}', verb=Verb.POST, path_params=[package_id])
        if response:
            self.status = response['submission_status']
            self.submission_id = response['submission_id']
        else:
            message = 'There was an error creating your submission'
            if self.exit:
                exit_client(signal=signal.SIGTERM,
                            message=message)
            else:
                raise Exception("{}\n{}".format('SubmissionError', message))

    def get_submission_status(self):
        print('Checking submission Status...')
        response = self.__authenticated_request(endpoint=self.api + '/{}', path_params=[str(self.submission_id)])

        if response:
            status = response['submission_status']
        else:
            message = 'An error occurred while checking submission {} status.'.format(self.submission_id)
            exit_client(signal=signal.SIGTERM, message=message)
        return status

    def get_credentials_for_files(self, file_ids):

        def get_credentials_for_file(file_id):
            response = self.__authenticated_request(endpoint=self.api + '/{}/files/{}/multipartUploadCredentials',
                                        path_params=[str(self.submission_id), file_id])

            return response

        def get_credentials_batch(file_ids):
            s3_buck = self.config.source_bucket if hasattr(self.config, 'source_bucket') else ''
            q_params = None
            if s3_buck:
                s3_pre = self.config.source_prefix if hasattr(self.config,
                                                              'source_prefix') and self.config.source_prefix else ''
                q_params = {'s3SourceBucket': s3_buck,
                            's3SourcePrefix': s3_pre}
            return self.__authenticated_request(endpoint=self.api + '/{}/files/batchMultipartUploadCredentials',
                                    verb=Verb.POST,
                                    path_params=[self.submission_id],
                                    q_params=q_params,
                                    data=json.dumps(file_ids))

        if len(file_ids) == 1:
            return get_credentials_for_file(file_ids[0])
        else:
            all_credentials = []
            batched_ids = [file_ids[i:i + self.batch_size] for i in range(0, len(file_ids), self.batch_size)]

            count = 1
            for ids in batched_ids:
                print('Retrieving credentials for files -  batch {} of {} @ {}...'.format(count, len(batched_ids),
                                                                                          datetime.now()))
                count += 1
                credentials_list = get_credentials_batch(ids)
                all_credentials = all_credentials + credentials_list['credentials']
                time.sleep(2)

            return all_credentials

    def get_submission_files(self):
        return self.__authenticated_request(endpoint=self.api + '/{}/files', path_params=[self.submission_id])

    def get_unsubmitted_files(self, file_info):
        # TODO extract this into a separate method
        data = self.__generate_batch_update_status_payload( file_info, Status.COMPLETE)
        url = "/".join([self.api, self.submission_id, 'files/batchUpdate'])
        auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        headers = {'content-type': 'application/json'}

        batched_data = [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
        unsubmitted_ids = []

        for index, batch in enumerate(batched_data):
            print('Checking submitted files batch {}/{} @ {}'.format(index + 1, len(batched_data), datetime.now()))

            session = requests.session()
            r = session.send(requests.Request('PUT', url, headers, auth=auth, data=json.dumps(batch)).prepare(),
                             timeout=600, stream=False)
            response = json.loads(r.text)
            errors = response['errors']
            for e in errors:
                unsubmitted_ids.append(e['submissionFileId'])
            logging.debug('Finished batch request {}/{} at @ {}'.format(index + 1, len(batched_data), datetime.now()))
        return unsubmitted_ids

    def check_file_exists_in_s3(self, file, s3_client, creds_by_source=None):
        # TODO  - this method is broken and needs to be fixed
        key = file
        # special handling when locations of associated files are absolute s3 URLS
        if key.startswith('s3://'):
            # confirm that the s3bucket + s3Prefix matches the s3 url, or throw an error
            tmp = 's3://{}/{}'.format(self.source_bucket,
                                      self.source_prefix) if self.source_prefix else 's3://{}'.format(
                self.source_bucket)
            if not key.startswith(tmp):
                m = 'Error - Detected absolute s3-url in associated files with an s3 location different from the --s3-bucket/--s3-prefix command line arguments. '
                m += 'Please correct s3-url or command line arguments in order to continue. '
                m += '\n --s3-bucket = {}, --s3-prefix = {}'.format(self.source_bucket, self.source_prefix)
                m += '\n s3-url = {}'.format(key)
                print(m)
                exit_client()

            file_name = key
            key = key.replace('s3://{}/'.format(self.source_bucket), '')
            if self.source_prefix:
                key = key.replace('{}/'.format(self.source_prefix), '')
        else:
            if self.source_prefix:
                key = '/'.join([self.source_prefix, file])
            file_name = '/'.join(['s3:/', self.source_bucket, key])

        if creds_by_source:
            creds = creds_by_source[file_name]
            s3_client = S3Authentication.get_s3_client_with_config(creds['access_key'], creds['secret_key'],
                                                                   creds['session_token'])

        try:
            response = s3_client.head_object(Bucket=self.source_bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                pass
            if error_code == 403:
                pass

    def get_all_incomplete_mpu(self, submission_file_credential):

        bucket = (submission_file_credential['destination_uri']).split('/')[2]
        prefix = 'submission_{}'.format(self.submission_id)

        access_key = submission_file_credential['access_key']
        secret_key = submission_file_credential['secret_key']
        session_token = submission_file_credential['session_token']

        multipart_uploads = MultiPartsUpload(bucket, prefix, self.config, access_key, secret_key, session_token)
        partially_uploaded_files = multipart_uploads.get_multipart_uploads() # TODO - add paging in case >1000
        return partially_uploaded_files

    def resume_submission(self):

        def submit_task(file_resource, partially_submitted_files, progress_queue):
            full_path = file_resource['file_user_path']
            self.update_submission_file_status(file_resource, Status.IN_PROGRESS)

            if full_path.startswith('s3'):
                if self.is_s3_to_s3_copy(file_resource):
                    self.attempt_s3_to_s3_copy(file_resource, progress_queue)
                else:
                    # If the user didnt specify -s3 param, then we have to download and upload file
                    # which is much more inefficient then performing a direct s3-to-s3 copy
                    self.transfer_external_s3_file(file_resource, partially_submitted_files, progress_queue)
            else:
                self.transfer_local_file(file_resource, partially_submitted_files, progress_queue)
            self.update_submission_file_status(file_resource, Status.COMPLETE)

        # START OF METHOD
        all_submission_files = self.get_submission_files()
        incomplete_files = [f for f in all_submission_files if f['status'] != Status.COMPLETE]
        # if not already set from the build package step, get file-info for associated files
        if not self.submission_file_info and incomplete_files:
            if self.config.source_bucket:
                sf = Utils.s3_file_search(self.config.aws_access_key,
                                                        self.config.aws_secret_key,
                                                        self.config.source_bucket,
                                                        self.config.source_prefix,
                                                        {f['file_user_path'] for f in incomplete_files})
            else:
                sf = Utils.local_file_search(self.config.directory_list, {f['file_user_path'] for f in incomplete_files})
            self.submission_file_info = {f.csv_path: f for f in sf}

        partially_uploaded_files = []
        if incomplete_files:
            submission_file_credential = self.get_credentials_for_files([incomplete_files[0]['id']])
            partially_uploaded_files = self.get_all_incomplete_mpu(submission_file_credential)

        all_submission_files = self.get_submission_files()
        total_upload_size = sum(
            map(lambda x: int(x['size']),
                              filter(lambda x: x['status'] != Status.COMPLETE, all_submission_files))
        )

        files_to_upload = [f for f in all_submission_files if f['status'] != Status.COMPLETE]

        if not self.config.hideProgress:
            progress_queue = None
            if self.is_s3_to_s3_copy():
                progress_queue = tqdm(total=len(self.file_sizes),
                                      position=0,
                                      unit_scale=True,
                                      unit="files",
                                      desc="Total Upload Progress",
                                      ascii=os.name == 'nt',
                                      dynamic_ncols=True)
            elif total_upload_size:
                progress_queue = tqdm(total=total_upload_size,
                                      position=0,
                                      unit_scale=True,
                                      unit="bytes",
                                      desc="Total Upload Progress",
                                      ascii=os.name == 'nt',
                                      dynamic_ncols=True)

        submit_pool = ThreadPool(self.thread_num)
        for file in files_to_upload:
            submit_pool.add_task(submit_task, file, partially_uploaded_files, progress_queue)

        submit_pool.wait_completion()

        if not self.config.hideProgress:
            print('\nUploads complete.')
        status = self.get_submission_status()
        if status == Status.UPLOADING:
            raise Exception('Error creating submission - contact NDAHelpdesk')

    def is_s3_to_s3_copy(self, file_resource=None):
        # TODO - this seems overly complicated - simplify this logic
        if not file_resource:
            return hasattr(self.config,'source_bucket') and self.config.source_bucket and not self.config.aws_access_key
        else:
            path = file_resource['file_user_path']
            return path.startswith('s3') and not self.config.aws_access_key

    # TODO this is broken
    def attempt_s3_to_s3_copy(self, file_resource, progress_queue):
        creds = self.get_credentials_for_files([file_resource['file_id']])
        bucket = file_resource['']
        key = file_resource['']
        local_path = sanitize_file_path(file_resource['file_user_path'])

        bytes_added = []

        def callback(b):
            bytes_added.append(b)
        try:
            # attempt to perform a bucket to bucket transfer perform a bucket to bucket
            s3_resource = boto3.Session(aws_access_key_id=creds['access_key'],
                                        aws_secret_access_key=creds['secret_key'],
                                        aws_session_token=creds['session_token']
                                        ).resource('s3')

            if local_path.startswith('s3://'):
                # special handling when locations of associated files are absolute s3 URLS
                source_key = local_path.replace('s3://{}/'.format(self.config.source_bucket), '')
                if self.config.source_prefix:
                    source_key = local_path.replace('{}/'.format(self.config.source_prefix), '')
            else:
                source_key = key.split('/')[1:]
                source_key = '/'.join(source_key)

            # create a source dictionary that specifies bucket name and key name of the object to be copied
            copy_source = {
                'Bucket': self.config.source_bucket,
                'Key': source_key if not self.config.source_prefix else '/'.join(
                    [self.config.source_prefix, source_key])
            }
            s3_resource.meta.client.copy(copy_source, bucket, key, Callback=callback)
            progress_queue.put(1)

        except Exception as e:
            print('error during s3-to-s3 copy: {}'.format(e))
            print(get_stack_trace())
            raise e

    # TODO this is broken
    def transfer_local_file(self, file_resource, partially_submitted_files, progress_queue):
        credentials = self.get_credentials_for_files([file_resource['file_id']])
        s3_url = file_resource['file_remote_path']
        bucket, key = Utils.deconstruct_s3_url(s3_url)
        file_size = None
        full_file_path = self.submission_file_info[file_resource['file_user_path']]
        mpu_to_complete = [f for f in partially_submitted_files if f['']==source_key]

        if mpu_to_complete:
            u = UploadMultiParts(mpu_to_complete, self.file_sizes, bucket, prefix, self.config, credentials, file_size)
            u.resume_multipart_upload()
        else:
            s3 = S3Authentication.get_s3_client_with_config(credentials['access_key'],
                                                            credentials['secret_key'],
                                                            credentials['session_token'])
            config = TransferConfig(
                multipart_threshold=100 * 1024 * 1024,
                max_concurrency=2,
                num_download_attempts=10)

            s3_transfer = S3Transfer(s3, config)
            tqdm.monitor_interval = 0
            s3_transfer.upload_file(full_file_path, bucket, key, callback=self.UpdateProgress(progress_queue))

    # TODO this is broken
    def transfer_external_s3_file(self, file_resource, partially_submitted_files, progress_queue):
        credentials = self.get_credentials_for_files([file_resource['file_id']])
        bucket = file_resource['']
        key = file_resource['']
        file_size = file_resource['']
        source_key_tmp = key.split('/')[1:]
        source_key_tmp = '/'.join(source_key_tmp)
        source_key = '/'.join([self.config.source_prefix, source_key_tmp])
        full_file_path = self.submission_file_info[file_resource['file_user_path']]
        mpu_to_complete = [f for f in partially_submitted_files if f['']==source_key]

        s3_config = Config(connect_timeout=240, read_timeout=240)
        source_s3 = credentials.get_s3_resource(s3_config)

        # Try downloading and then uploading
        fileobj = source_s3.Object(self.config.source_bucket, source_key.lstrip('/')).get()['Body']
        if mpu_to_complete:
            u = UploadMultiParts(mpu_to_complete, full_file_path, bucket, prefix, self.config, credentials, file_size)
            u.resume_multipart_upload()
        else:
            # set chunk size dynamically to based on file size
            if file_size > 9999:
                multipart_chunk_size = (file_size // 9999)
            else:
                multipart_chunk_size = file_size
            transfer_config = TransferConfig(multipart_threshold=100 * 1024 * 1024,
                                             multipart_chunksize=multipart_chunk_size)

            dest = S3Authentication.get_s3_client_with_config(credentials['access_key'],
                                                                   credentials['secret_key'],
                                                                   credentials['session_token'])
            dest_bucket = bucket
            dest_key = key
            dest.upload_fileobj(
                fileobj,
                dest_bucket,
                dest_key,
                Callback=UpdateProgress(progress_queue),
                Config=transfer_config
            )

    def __generate_batch_update_status_payload(self, files, status):
        return [{
                "id": str(file['id']),
                # TODO - figure out how to get size when it was set during local-file search
                #"size": size,
                "md5sum": "None",
                "status": status
            } for file in files
        ]
