from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NDATools
# from NDATools import Utils
from NDATools.upload.submission.submission import LocalAssociatedFile


@pytest.fixture
def mock_upload_functions_setup(monkeypatch, s3_mock):
    monkeypatch.setattr(NDATools.upload.submission.submission, 'get_s3_client_with_config',
                        MagicMock(return_value=s3_mock))
    monkeypatch.setattr(NDATools.upload.submission.submission, 'to_local_associated_file', MagicMock(
        side_effect=lambda x, y: LocalAssociatedFile(Path(x['file_user_path']), 0, x['file_user_path'], x)))


def test_resume_submission(mock_upload_functions_setup, uploading_submission, s3_mock, monkeypatch):
    """ Test that calling resume_submission calls expected methods in Submission """
    uploading_submission.resume_submission()
    assert uploading_submission.check_status.call_count == 2
    assert uploading_submission.get_multipart_credentials.call_count == 1
    assert uploading_submission.get_upload_progress.call_count == 1
    assert uploading_submission.get_files_from_page.call_count == 1
    assert uploading_submission.check_uploaded_not_complete.call_count == 1
    assert s3_mock.upload_file.call_count == 2  # one for each file

    # when check_uploaded_not_complete returns None, that the file upload is skipped
    uploading_submission.check_uploaded_not_complete.reset_mock()
    s3_mock.reset_mock()
    uploading_submission.check_uploaded_not_complete.side_effect = lambda x: []
    uploading_submission.resume_submission()
    assert uploading_submission.check_uploaded_not_complete.call_count == 1
    assert s3_mock.upload_file.call_count == 0  # No files were actually uploaded, because they were already in s3


def test_create_submission_success(new_submission, s3_mock):
    """ Test that calling submit causes submission status to change to complete if no unexpected errors are returned from api"""
    new_submission.submit()
    assert new_submission.submission_id is not None
    assert new_submission.status == 'Upload Completed'
    assert new_submission.query_submissions_by_package_id.call_count == 1
    assert new_submission._create_submission.call_count == 1
    assert new_submission.query_submissions_by_package_id.call_count == 1
    assert new_submission.check_uploaded_not_complete.call_count == 0


def test_replace_submission(monkeypatch, uploading_submission):
    """ Test that calling replace_submission calls expected methods in Submission """
    monkeypatch.setattr(uploading_submission, 'get_submission_versions', MagicMock(side_effect=[[1], [1, 2]]))
    monkeypatch.setattr(uploading_submission, '_replace_submission', MagicMock())
    uploading_submission.replace_submission()
    assert uploading_submission.get_submission_versions.call_count == 2
    assert uploading_submission._replace_submission.call_count == 1
