import json
import uuid
from unittest.mock import MagicMock

import pytest

from NDATools import Submission


@pytest.fixture
def config(shared_datadir, validation_config_factory):
    file_path = (shared_datadir / 'validation/file.csv')
    test_args = [str(file_path)]
    _, config = validation_config_factory(test_args)
    config.JSON = True
    return config


@pytest.fixture
def submission_with_files(load_from_file):
    submission_resource = json.loads(load_from_file('submission/api_response/create_submission_response.json'))
    creds_resource = json.loads(load_from_file('submission/api_response/batch_get_temp_creds.json'))
    file_listing = json.loads(load_from_file('submission/api_response/get_file_listing.json'))
    submission_by_submission_pkg = json.loads(load_from_file('submission/api_response/get_submission_by_sub_pkg.json'))

    return submission_resource, creds_resource, file_listing, submission_by_submission_pkg


@pytest.fixture
def batch_update_status(load_from_file):
    return json.loads(load_from_file('submission/api_response/batch_update_status_response.json'))


@pytest.fixture
def new_submission(monkeypatch, config, submission_with_files, tmpdir):
    submission_resource, creds_resource, file_listing, submission_by_submission_pkg = submission_with_files
    submission = Submission.Submission(package_id=str(str(uuid.uuid4())),
                                       thread_num=1,
                                       batch_size=20,
                                       allow_exit=True,
                                       config=config)
    # monkeypatch some of the methods that make API calls
    monkeypatch.setattr(submission, 'get_multipart_credentials', MagicMock(return_value=creds_resource['credentials']))
    monkeypatch.setattr(submission, '_create_submission', MagicMock(return_value={'status': 'success'}))
    monkeypatch.setattr(submission, 'query_submissions_by_package_id',
                        MagicMock(return_value=submission_by_submission_pkg))
    monkeypatch.setattr(submission, '_get_submission_by_id', MagicMock(return_value=submission_resource))
    submission.directory_list = [str(tmpdir)]
    return submission


@pytest.fixture
def uploading_submission(monkeypatch, new_submission, submission_with_files, batch_update_status):
    submission_resource, creds_resource, file_listing, _ = submission_with_files
    mock_batch_update = MagicMock(return_value=batch_update_status)
    monkeypatch.setattr(new_submission, 'get_multipart_credentials',
                        MagicMock(return_value=creds_resource['credentials']))
    monkeypatch.setattr(new_submission, 'batch_update_status', mock_batch_update['errors'])
    monkeypatch.setattr(new_submission, 'get_upload_progress', MagicMock(return_value=(2, 1)))
    monkeypatch.setattr(new_submission, 'get_files_from_page', MagicMock(return_vaule=file_listing))
    monkeypatch.setattr(new_submission, 'check_status', MagicMock())
    monkeypatch.setattr(new_submission, 'status', Submission.Status.UPLOADING)
    return new_submission


def test_resume_submission(uploading_submission):
    """ Test that calling resume_submission calls expected methods in Submission """
    uploading_submission.resume_submission()
    assert uploading_submission.check_status.call_count == 2
    assert uploading_submission.get_multipart_credentials.call_count == 1
    assert uploading_submission.get_upload_progress.call_count == 1
    assert uploading_submission.get_files_from_page.call_count == 1


def test_create_submission_success(new_submission):
    """ Test that calling submit causes submission status to change to complete if no unexpected errors are returned from api"""
    new_submission.submit()
    assert new_submission.submission_id is not None
    assert new_submission.status == 'Upload Completed'
    assert new_submission.query_submissions_by_package_id.call_count == 1
    assert new_submission._create_submission.call_count == 1
    assert new_submission.query_submissions_by_package_id.call_count == 1


def test_replace_submission(monkeypatch, uploading_submission):
    """ Test that calling replace_submission calls expected methods in Submission """
    monkeypatch.setattr(uploading_submission, 'get_submission_versions', MagicMock(side_effect=[[1], [1, 2]]))
    monkeypatch.setattr(uploading_submission, '_replace_submission', MagicMock())
    uploading_submission.replace_submission()
    assert uploading_submission.get_submission_versions.call_count == 2
    assert uploading_submission._replace_submission.call_count == 1
