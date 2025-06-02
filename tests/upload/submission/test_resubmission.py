from unittest.mock import MagicMock

import pytest

from NDATools.Configuration import ClientConfiguration
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.submission.api import SubmissionHistory, NdaCollection, Submission, DataStructureDetails, \
    SubmissionDetails
from NDATools.upload.submission.resubmission import SubmissionApi, build_replacement_package_info, \
    ReplacementPackageInfo
from NDATools.upload.submission.resubmission import check_replacement_authorized

submission_id = 12345
validation_uuid = 'e7e1a9d2-1b5b-4a74-9a4e-1c21b9e3a5d8'
validation_uuid1 = 'f201711e-dd9a-4cfc-be85-5379fb569fee'
short_name = 'Bestdonut01'
short_name1 = 'Bestbagel02'
new_validation_uuid = 'n3e1a9d2-1b5b-4a74-9a4e-9c90b9e7a5d0'
new_validation_uuid1 = '9e9e5269-3152-4380-b672-3576e3d3e444'
collection_id = 123


@pytest.fixture
def mock_config():
    mock_config = MagicMock(spec=ClientConfiguration)
    mock_config.submission_api_endpoint = 'http://localhost:8080/api/submission/'
    mock_config.username = 'user_123'
    mock_config.password = 'password_123'
    return mock_config


@pytest.fixture
def submission_history():
    submission_history = SubmissionHistory(replacement_authorized=False, created_by='user_123',
                                           created_date='05-07-2025')
    submission_history1 = SubmissionHistory(replacement_authorized=True, created_by='user_123',
                                            created_date='05-09-2025')
    return [submission_history, submission_history1]


@pytest.fixture
def mock_args():
    mock_args = MagicMock()
    mock_args.replace_submission = submission_id
    return mock_args


def fake_exit(message=None):
    raise SystemExit(message)


@pytest.fixture
def submission():
    collection = NdaCollection(id=collection_id, title='best donut collection')
    submission = Submission(submission_status='Upload Completed', dataset_title='best donuts data',
                            dataset_description='what kind of donut is the best',
                            dataset_created_date='05-07-2025', dataset_modified_date=None, submission_id=submission_id,
                            collection=collection)
    return submission


def test_check_replacement_authorized_not_authorized(monkeypatch, mock_config, submission_history):
    mock_submission_api = MagicMock(spec=SubmissionApi)
    monkeypatch.setattr('NDATools.upload.submission.resubmission.SubmissionApi',
                        MagicMock(return_value=mock_submission_api))
    mock_submission_api.get_submission_history.return_value = [submission_history[0]]

    mock_exit = MagicMock()
    monkeypatch.setattr('NDATools.upload.submission.resubmission.exit_error', mock_exit)

    check_replacement_authorized(mock_config, submission_id)

    mock_submission_api.get_submission_history.assert_called_once()
    mock_exit.assert_called_with(
        message='submission_id {} is not authorized to be replaced. Please contact the NDA help desk for approval to replace this submission'.format(
            submission_id))


def test_check_replacement_authorized_already_replaced(monkeypatch, mock_config, submission_history):
    mock_submission_api = MagicMock(spec=SubmissionApi)
    monkeypatch.setattr('NDATools.upload.submission.resubmission.SubmissionApi',
                        MagicMock(return_value=mock_submission_api))
    mock_submission_api.get_submission_history.return_value = submission_history

    mock_exit = MagicMock()
    monkeypatch.setattr('NDATools.upload.submission.resubmission.exit_error', mock_exit)

    check_replacement_authorized(mock_config, submission_id)

    mock_submission_api.get_submission_history.assert_called_once()
    mock_exit.assert_called_with(
        message='''Submission {} was already replaced by {} on {}.
    If you need to make further edits to this submission, please reach out the the NDA help desk'''
        .format(submission_id, submission_history[0].created_by, submission_history[0].created_date))


def test_check_replacement_authorized_authorized(monkeypatch, mock_config, submission_history):
    mock_submission_api = MagicMock(spec=SubmissionApi)
    monkeypatch.setattr('NDATools.upload.submission.resubmission.SubmissionApi',
                        MagicMock(return_value=mock_submission_api))
    mock_submission_api.get_submission_history.return_value = [submission_history[1]]

    mock_exit = MagicMock()
    monkeypatch.setattr('NDATools.upload.submission.resubmission.exit_error', mock_exit)

    check_replacement_authorized(mock_config, submission_id)

    mock_submission_api.get_submission_history.assert_called_once()
    mock_exit.assert_not_called()


def test_build_replacement_package_info_happy_path(monkeypatch, mock_config, mock_args, submission):
    data_structure_details = DataStructureDetails(shortName=short_name, rows=2, validationUuids=[validation_uuid])
    data_structure_details1 = DataStructureDetails(shortName=short_name1, rows=2, validationUuids=[validation_uuid1])

    submission_details = SubmissionDetails(validation_uuids=[validation_uuid], submissionId=submission_id,
                                           pendingChanges=[data_structure_details, data_structure_details1])

    validated_file = MagicMock(spec=ValidatedFile)
    validated_file.short_name = short_name
    validated_file.row_count = 2
    validated_file.uuid = new_validation_uuid

    validated_file1 = MagicMock(spec=ValidatedFile)
    validated_file1.short_name = short_name1
    validated_file1.row_count = 2
    validated_file1.uuid = new_validation_uuid1

    replacement_package_info: ReplacementPackageInfo = build_replacement_package_info([validated_file, validated_file1],
                                                                                      submission, submission_details)
    assert replacement_package_info.submission_id == submission_id
    assert replacement_package_info.collection_id == collection_id
    assert replacement_package_info.title == submission.dataset_title
    assert replacement_package_info.description == submission.dataset_description
    assert new_validation_uuid in replacement_package_info.validation_uuids
    assert new_validation_uuid1 in replacement_package_info.validation_uuids


def test_build_replacement_package_info_unrecognized_ds(monkeypatch, mock_config, mock_args, submission):
    new_short_name = 'Flapjack01'

    data_structure_details = DataStructureDetails(shortName=short_name, rows=2, validationUuids=[validation_uuid])
    data_structure_details1 = DataStructureDetails(shortName=short_name1, rows=2, validationUuids=[validation_uuid1])

    submission_details = SubmissionDetails(validation_uuids=[validation_uuid], submissionId=submission_id,
                                           pendingChanges=[data_structure_details, data_structure_details1])

    validated_file = MagicMock(spec=ValidatedFile)
    validated_file.short_name = short_name
    validated_file.row_count = 2
    validated_file.uuid = new_validation_uuid

    validated_file1 = MagicMock(spec=ValidatedFile)
    validated_file1.short_name = new_short_name
    validated_file1.row_count = 2
    validated_file1.uuid = new_validation_uuid1

    mock_exit = MagicMock(side_effect=fake_exit)
    monkeypatch.setattr('NDATools.upload.submission.resubmission.exit_error', mock_exit)

    # catch the exit, so the assert can run
    with pytest.raises(SystemExit):
        build_replacement_package_info([validated_file, validated_file1], submission, submission_details)

    expected_msg = (
                       'ERROR - The following datastructures were not included in the original submission and therefore cannot '
                       'be included in the replacement submission: \r\n') + new_short_name
    mock_exit.assert_called_once_with(message=expected_msg)


def test_build_replacement_package_info_missing_data_discontinue(monkeypatch, mock_config, mock_args, submission):
    data_structure_details = DataStructureDetails(shortName=short_name, rows=4, validationUuids=[validation_uuid])
    submission_details = SubmissionDetails(validation_uuids=[validation_uuid], submissionId=submission_id,
                                           pendingChanges=[data_structure_details])

    validated_file = MagicMock(spec=ValidatedFile)
    validated_file.short_name = short_name
    validated_file.row_count = 2
    validated_file.uuid = new_validation_uuid

    mock_input = MagicMock(return_value='n')
    monkeypatch.setattr('NDATools.upload.submission.resubmission.evaluate_yes_no_input', mock_input)

    mock_exit = MagicMock(side_effect=fake_exit)
    monkeypatch.setattr('NDATools.upload.submission.resubmission.exit_error', mock_exit)

    # catch the exit, so the assert can run
    with pytest.raises(SystemExit):
        build_replacement_package_info([validated_file], submission, submission_details)

    prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
    prompt += '\nAre you sure you want to continue? (y/n): '
    mock_input.assert_called_once_with(prompt)
    mock_exit.assert_called_once()


def test_build_replacement_package_info_missing_data_continue(monkeypatch, mock_config, mock_args, submission):
    data_structure_details = DataStructureDetails(shortName=short_name, rows=4, validationUuids=[validation_uuid])
    submission_details = SubmissionDetails(validation_uuids=[validation_uuid], submissionId=submission_id,
                                           pendingChanges=[data_structure_details])

    validated_file = MagicMock(spec=ValidatedFile)
    validated_file.short_name = short_name
    validated_file.row_count = 2
    validated_file.uuid = new_validation_uuid

    mock_input = MagicMock(return_value='y')
    monkeypatch.setattr('NDATools.upload.submission.resubmission.evaluate_yes_no_input', mock_input)

    replacement_package_info: ReplacementPackageInfo = build_replacement_package_info([validated_file], submission,
                                                                                      submission_details)

    prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
    prompt += '\nAre you sure you want to continue? (y/n): '
    mock_input.assert_called_once_with(prompt)

    assert replacement_package_info.submission_id == submission_id
    assert replacement_package_info.collection_id == collection_id
    assert replacement_package_info.title == submission.dataset_title
    assert replacement_package_info.description == submission.dataset_description
    assert replacement_package_info.validation_uuids == [new_validation_uuid]
