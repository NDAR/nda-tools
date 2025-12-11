from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from NDATools.upload.submission.api import SubmissionStatus, Submission, NdaCollection, SubmissionApi, UploadProgress, \
    AssociatedFile, AssociatedFileStatus, AssociatedFileUploadCreds
from NDATools.upload.submission.associated_file import AssociatedFileUploader


@pytest.fixture
def get_submission():
    collection = NdaCollection(id=80, title='best cupcake collection')
    submission = Submission(submission_status=SubmissionStatus.UPLOADING, dataset_title='best cupcake data',
                            dataset_description='survey about the best cupcake research',
                            dataset_created_date='05-14-2025', dataset_modified_date=None, submission_id=78905,
                            collection=collection)
    return submission


@pytest.fixture
def get_associated_files():
    associated_file1 = AssociatedFile(id=111, file_user_path='readme.txt',
                                      file_remote_path='s3://nda-central-dev/collection-80/submission-89075/associated-files/README',
                                      status=AssociatedFileStatus.READY, size=2074)
    associated_file2 = AssociatedFile(id=222, file_user_path='readme2.txt',
                                      file_remote_path='s3://nda-central-dev/collection-80/submission-89075/associated-files/README2',
                                      status=AssociatedFileStatus.READY, size=2074)
    return [associated_file1, associated_file2]


def fake_exit(message=None):
    raise SystemExit(message)


@pytest.fixture
def upload_creds1(get_associated_files):
    associated_file1 = get_associated_files[0]
    return AssociatedFileUploadCreds(submissionFileId=associated_file1.id,
                                     destination_uri=associated_file1.file_remote_path,
                                     source_uri=associated_file1.file_user_path, access_key='135DFVDFBNDL',
                                     secret_key='RHGADKVNASDLG4534543534F',
                                     session_token='DFDFKSADL3452340980')


@pytest.fixture
def upload_creds2(get_associated_files):
    associated_file2 = get_associated_files[1]
    return AssociatedFileUploadCreds(submissionFileId=associated_file2.id,
                                     destination_uri=associated_file2.file_remote_path,
                                     source_uri=associated_file2.file_user_path, access_key='135DFVDFBNDL',
                                     secret_key='RHGADKVNASDLG4534543534F',
                                     session_token='DFDFKSADL3452340980')


@pytest.fixture
def upload_progress():
    return UploadProgress(associated_file_count=2, uploaded_file_count=0)


@pytest.fixture
def associated_file1(get_associated_files):
    return get_associated_files[0]


@pytest.fixture
def associated_file2(get_associated_files):
    return get_associated_files[1]


@pytest.fixture
def submission_api_mock(upload_progress, associated_file1, associated_file2, upload_creds1, upload_creds2):
    mock_submission_api = MagicMock(spec=SubmissionApi)
    mock_submission_api.get_upload_progress.return_value = upload_progress
    mock_submission_api.get_files_by_page.side_effect = [[associated_file1, associated_file2]]
    mock_submission_api.get_upload_credentials.side_effect = [[upload_creds1], [upload_creds2]]
    mock_submission_api.batch_update_associated_file_status.side_effect = [None, None]
    return mock_submission_api


@pytest.fixture
def mock_s3_client():
    mock_s3_client = MagicMock(spec=boto3.client('s3'))
    mock_s3_client.upload_file.side_effect = [None, None]
    return mock_s3_client


def create_associated_file(datadir, name):
    associated_file_path = datadir / name
    associated_file_path.parent.mkdir(parents=True, exist_ok=True)
    associated_file_path.write_text('testing')


def create_associated_file1(datadir, name='readme.txt'):
    create_associated_file(datadir, name)


def create_associated_file2(datadir, name='readme2.txt'):
    create_associated_file(datadir, name)


def create_associated_files(datadir, name1='readme.txt', name2='readme2.txt'):
    create_associated_file1(datadir, name1)
    create_associated_file2(datadir, name2)


@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_happy_path(mock_get_cli, mock_s3_client, get_submission, get_associated_files, datadir,
                                 submission_api_mock, upload_progress):
    create_associated_files(datadir)
    search_folders = [datadir]
    resuming_upload = False
    mock_get_cli.return_value = mock_s3_client

    associated_file_uploader = AssociatedFileUploader(submission_api_mock, 1, False, False, 1)
    associated_file_uploader.start_upload(get_submission, search_folders, False, datadir)

    verify_upload_context(upload_context=associated_file_uploader.uploader.upload_context,
                          submission_id=get_submission.submission_id,
                          resuming_upload=resuming_upload,
                          upload_progress=upload_progress, search_folders=search_folders, num_of_files_not_found=0)

    verify_submission_api(mock_submission_api=submission_api_mock, submission_id=get_submission.submission_id,
                          get_upload_credentials_call_ct=2, batch_update_associated_file_status_call_ct=2)

    assert mock_s3_client.upload_file.call_count == 2


@patch('NDATools.upload.submission.associated_file.get_directory_input')
@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_files_not_found_reenter(mock_get_cli, mock_input, mock_s3_client, get_submission,
                                              get_associated_files,
                                              datadir, submission_api_mock, upload_progress):
    create_associated_files(datadir, name2='another_associated_file/readme2.txt')
    search_folders = [datadir]
    resuming_upload = False
    mock_input.return_value = datadir / 'another_associated_file'
    mock_get_cli.return_value = mock_s3_client

    associated_file_uploader = AssociatedFileUploader(submission_api_mock, 1, False, False, 1)
    associated_file_uploader.start_upload(get_submission, search_folders, resuming_upload, datadir)

    verify_upload_context(upload_context=associated_file_uploader.uploader.upload_context,
                          submission_id=get_submission.submission_id,
                          resuming_upload=resuming_upload,
                          upload_progress=upload_progress, search_folders=search_folders, num_of_files_not_found=0)

    verify_submission_api(mock_submission_api=submission_api_mock, submission_id=get_submission.submission_id,
                          get_upload_credentials_call_ct=3, batch_update_associated_file_status_call_ct=2)

    assert mock_s3_client.upload_file.call_count == 2
    assert mock_input.call_count == 1


@patch('NDATools.upload.submission.associated_file.exit_error', side_effect=fake_exit)
@patch('NDATools.upload.submission.associated_file.get_directory_input')
@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_files_not_found_exit(mock_get_cli, mock_input, mock_exit, mock_s3_client, get_submission,
                                           get_associated_files, datadir, submission_api_mock, upload_progress):
    create_associated_file1(datadir)
    search_folders = [datadir]
    resuming_upload = False
    mock_get_cli.return_value = mock_s3_client

    associated_file_uploader = AssociatedFileUploader(submission_api_mock, 1, True, False, 1)
    with pytest.raises(SystemExit):
        associated_file_uploader.start_upload(get_submission, search_folders, resuming_upload, datadir)

    verify_upload_context(upload_context=associated_file_uploader.uploader.upload_context,
                          submission_id=get_submission.submission_id,
                          resuming_upload=resuming_upload,
                          upload_progress=upload_progress, search_folders=search_folders, num_of_files_not_found=1)

    verify_submission_api(mock_submission_api=submission_api_mock, submission_id=get_submission.submission_id,
                          get_upload_credentials_call_ct=2, batch_update_associated_file_status_call_ct=1)

    assert mock_s3_client.upload_file.call_count == 1
    assert mock_input.call_count == 0
    assert mock_exit.call_count == 1


@patch('NDATools.upload.batch_file_uploader.exit_error', side_effect=fake_exit)
@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_s3_upload_error(mock_get_cli, mock_exit, mock_s3_client, get_submission, get_associated_files,
                                      datadir,
                                      submission_api_mock, upload_progress):
    create_associated_files(datadir)
    search_folders = [datadir]
    resuming_upload = False
    mock_s3_client.upload_file.side_effect = [Exception('Failed to upload to s3')]
    mock_get_cli.return_value = mock_s3_client

    associated_file_uploader = AssociatedFileUploader(submission_api_mock, 1, False, False, 1)

    with pytest.raises(SystemExit):
        associated_file_uploader.start_upload(get_submission, search_folders, resuming_upload, datadir)

    verify_upload_context(upload_context=associated_file_uploader.uploader.upload_context,
                          submission_id=get_submission.submission_id,
                          resuming_upload=resuming_upload,
                          upload_progress=upload_progress, search_folders=search_folders, num_of_files_not_found=0)

    verify_submission_api(mock_submission_api=submission_api_mock, submission_id=get_submission.submission_id,
                          get_upload_credentials_call_ct=1, batch_update_associated_file_status_call_ct=0)

    assert mock_s3_client.upload_file.call_count == 1
    assert mock_exit.call_count == 1


@patch('NDATools.upload.submission.associated_file.get_s3_client_with_config')
def test_start_upload_resume_upload(mock_get_cli, mock_s3_client, get_submission, get_associated_files, datadir,
                                    submission_api_mock,
                                    upload_progress):
    create_associated_files(datadir)
    search_folders = [datadir]
    resuming_upload = True
    mock_get_cli.return_value = mock_s3_client

    client_error = ClientError({'Error': {'Code': '404', 'Message': 'Not Found'}}, 'HeadObject')
    mock_s3_client.head_object.side_effect = [client_error, None]
    mock_s3_client.upload_file.side_effect = [None]

    associated_file_uploader = AssociatedFileUploader(submission_api_mock, 1, False, False, 1)
    associated_file_uploader.start_upload(get_submission, search_folders, resuming_upload, datadir)

    verify_upload_context(upload_context=associated_file_uploader.uploader.upload_context,
                          submission_id=get_submission.submission_id,
                          resuming_upload=resuming_upload,
                          upload_progress=upload_progress, search_folders=search_folders, num_of_files_not_found=0)

    verify_submission_api(mock_submission_api=submission_api_mock, submission_id=get_submission.submission_id,
                          get_upload_credentials_call_ct=2, batch_update_associated_file_status_call_ct=2)

    assert mock_s3_client.upload_file.call_count == 1
    assert mock_s3_client.head_object.call_count == 2


def verify_upload_context(upload_context, submission_id, resuming_upload, upload_progress, search_folders,
                          num_of_files_not_found):
    assert upload_context.submission.submission_id == submission_id
    assert upload_context.resuming_upload == resuming_upload
    assert upload_context.upload_progress == upload_progress
    assert upload_context.transfer_config.multipart_threshold == 5 * 1024 * 1024 * 1024
    assert upload_context.search_folders == search_folders
    assert "Uploading a batch of 1 files" in upload_context.progress_bar.desc
    assert len(upload_context.files_not_found) == num_of_files_not_found


def verify_submission_api(mock_submission_api, submission_id, get_upload_credentials_call_ct,
                          batch_update_associated_file_status_call_ct):
    mock_submission_api.get_upload_progress.assert_called_with(submission_id)
    assert mock_submission_api.get_upload_credentials.call_count == get_upload_credentials_call_ct
    assert mock_submission_api.batch_update_associated_file_status.call_count == batch_update_associated_file_status_call_ct

# def verify_sql_lite(db_path):
#     assert os.path.exists(db_path)
#     assert os.path.isfile(db_path)
#     assert os.stat(db_path).st_size > 0
