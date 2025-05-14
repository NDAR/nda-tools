import os
from unittest.mock import MagicMock, patch

import boto3

from NDATools.upload.submission.api import SubmissionStatus, Submission, NdaCollection, SubmissionApi, UploadProgress, \
    AssociatedFile, AssociatedFileStatus, AssociatedFileUploadCreds
from NDATools.upload.submission.associated_file import AssociatedFileUploader


@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_happy_path(mock_get_s3_client):
    submission_id = 89075
    collection_id = 80

    collection = NdaCollection(id=collection_id, title='best cupcake collection')
    submission = Submission(submission_status=SubmissionStatus.UPLOADING, dataset_title='best cupcake data',
                            dataset_description='survey about the best cupcake research',
                            dataset_created_date='05-14-2025', dataset_modified_date=None, submission_id=submission_id,
                            collection=collection)

    search_folders = [os.getcwd()]
    upload_progress = UploadProgress(associated_file_count=2, uploaded_file_count=0)

    mock_submission_api = MagicMock(spec=SubmissionApi)
    mock_submission_api.get_upload_progress.return_value = upload_progress

    associated_file_id1 = 111
    associated_file_id2 = 222

    associated_file1 = AssociatedFile(id=associated_file_id1, file_user_path='associated_file/README.txt',
                                      file_remote_path='s3://nda-central-dev/collection-80/submission-89075/associated-files/README',
                                      status=AssociatedFileStatus.READY, size=2074)
    associated_file2 = AssociatedFile(id=associated_file_id2, file_user_path='associated_file/README2.txt',
                                      file_remote_path='s3://nda-central-dev/collection-80/submission-89075/associated-files/README2',
                                      status=AssociatedFileStatus.READY, size=2074)
    mock_submission_api.get_files_by_page.side_effect = [[associated_file1], [associated_file2]]

    upload_creds1 = AssociatedFileUploadCreds(submissionFileId=associated_file_id1, destination_uri=associated_file1.file_remote_path,
                                              source_uri=associated_file1.file_user_path, access_key='135DFVDFBNDL',
                                              secret_key='RHGADKVNASDLG4534543534F', session_token='DFDFKSADL3452340980')
    upload_creds2 = AssociatedFileUploadCreds(submissionFileId=associated_file_id2, destination_uri=associated_file2.file_remote_path,
                                              source_uri=associated_file2.file_user_path, access_key='135DFVDFBNDL',
                                              secret_key='RHGADKVNASDLG4534543534F', session_token='DFDFKSADL3452340980')
    mock_submission_api.get_upload_credentials.side_effect = [[upload_creds1], [upload_creds2]]
    mock_submission_api.batch_update_associated_file_status.side_effect = [None, None]
    resuming_upload = False

    mock_s3_client = MagicMock(spec=boto3.client('s3'))
    mock_get_s3_client.return_value = mock_s3_client
    mock_s3_client.upload_file.side_effect = [None, None]

    associated_file_uploader = AssociatedFileUploader(mock_submission_api, 1, False, False, 1)
    associated_file_uploader.start_upload(submission, search_folders, resuming_upload)

    upload_context = associated_file_uploader.uploader.upload_context
    assert upload_context.submission.submission_id == submission_id
    assert upload_context.resuming_upload == resuming_upload
    assert upload_context.upload_progress == upload_progress
    assert upload_context.transfer_config.multipart_threshold == 5 * 1024 * 1024 * 1024
    assert upload_context.search_folders == search_folders
    assert upload_context.progress_bar.total == upload_progress.associated_file_count
    assert len(upload_context.files_not_found) == 0
