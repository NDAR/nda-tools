import shlex
import sys
import uuid
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.clientscripts.vtcmd import validate
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.submission.api import SubmissionHistory, NdaCollection, SubmissionPackage, PackagingStatus, \
    SubmissionStatus, Submission
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationV2
from tests.conftest import MockLogger


# overwrite the fixture to read from datadir instead of global shared_datadir
@pytest.fixture
def load_from_file(datadir):
    def _load_from_file(filename: str):
        with open(datadir / filename, 'r') as f:
            return f.read()

    return _load_from_file


@pytest.fixture
def unauthorized_resubmission_response():
    return [SubmissionHistory(**{
        'replacement_authorized': False,
        'created_by': '123123',
        'created_date': '10/23/2023'
    })]


def test_check_args(monkeypatch, unauthorized_resubmission_response):
    # replace submission cant be used with -t , -c or -d args
    with monkeypatch.context() as m:
        m.setattr(sys, 'argv',
                  ['vtcmd', 'ndarsubject01.csv', '-b', '-c', '1860', '-t', 'test', '-d', 'test', '-rs', '12345', '-u',
                   'testuser'])
        m.setattr(NDATools.clientscripts.vtcmd, 'exit_error', MagicMock(side_effect=[SystemExit]))
        m.setattr(NDATools, '_get_password', MagicMock(return_value='testpassword'))
        m.setattr(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', MagicMock(return_value=True))
        try:
            NDATools.clientscripts.vtcmd.main()
        except SystemExit:
            pass
        assert NDATools.clientscripts.vtcmd.exit_error.call_count == 1

    # submission not authorized for replacement
    with monkeypatch.context() as m:
        m.setattr(sys, 'argv',
                  ['vtcmd', 'ndarsubject01.csv', '-b', '-rs', '12345', '-u', 'testuser'])
        m.setattr(NDATools.upload.submission.resubmission, 'exit_error', MagicMock(side_effect=[SystemExit]))
        m.setattr(NDATools, '_get_password', MagicMock(return_value='testpassword'))
        m.setattr(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', MagicMock(return_value=True))
        m.setattr(NDATools.upload.submission.api.SubmissionApi, 'get_submission_history',
                  MagicMock(return_value=unauthorized_resubmission_response))
        try:
            NDATools.clientscripts.vtcmd.main()
        except SystemExit:
            pass
        assert NDATools.upload.submission.resubmission.exit_error.call_count == 1


@pytest.fixture
def build_validation_v2_resource():
    def _build_validation_v2_resource(has_manifests: bool):
        return ValidationV2(**{
            'validation_uuid': str(uuid.uuid4()),
            'status': 'Complete',
            'short_name': 'fmriresults01',
            'rows': 1,
            'validation_files': {},
            'scope': None
        })

    return _build_validation_v2_resource


@pytest.fixture
def user_collections():
    return [NdaCollection(id=1860, title='test')]


@pytest.fixture
def submission_package():
    def _submission_package(status: PackagingStatus = PackagingStatus.COMPLETE):
        return SubmissionPackage(**{
            'submission_package_uuid': str(uuid.uuid4()),
            'created_date': '10/10/2025',
            'expiration_date': '10/10/2026',
            'status': status
        })

    return _submission_package


@pytest.fixture
def complete_submission_package(submission_package):
    return submission_package(PackagingStatus.COMPLETE)


@pytest.fixture
def processing_submission_package(submission_package):
    return submission_package(PackagingStatus.PROCESSING)


@pytest.fixture
def upload_creds(monkeypatch):
    def _upload_creds(validation_uuid: str = None):
        if not validation_uuid:
            validation_uuid = str(uuid.uuid4())
        tmp = ValidationV2Credentials(lambda: None, **{
            'validation_uuid': validation_uuid,
            'read_write_permission': {
                'csv data': f's3://nimhda-validation/{validation_uuid}/file-upload.csv',
                'manifests folder': f's3://nimhda-validation/{validation_uuid}/manifests/'
            },
            'read_permission': {
                'warnings json': f's3://nimhda-validation-results/{validation_uuid}/validation-warnings.json',
                'errors json': f's3://nimhda-validation-results/{validation_uuid}/validation-errors.json',
                'metadata json': f's3://nimhda-validation-results/{validation_uuid}/validation-metadata.json',
                'manifest json': f's3://nimhda-validation-results/{validation_uuid}/validation-manifests.json',
                'associated files json': f's3://nimhda-validation-results/{validation_uuid}/validation-associatedFiles.json',
            },
            'access_key_id': 'test',
            'secret_access_key': 'test',
            'session_token': 'test',
        })
        monkeypatch.setattr(tmp, '_s3_transfer', MagicMock())
        return tmp

    return _upload_creds


@pytest.fixture
def validation_v2_response():
    def _validation_v2_response(short_name, status='CompleteWithWarnings'):
        validation_uuid = str(uuid.uuid4())
        return ValidationV2(**{
            'validation_uuid': validation_uuid,
            'status': status,
            'short_name': short_name,
            'rows': 40,
            'validation_files': {
                'csv data': f's3://nimhda-validation/{validation_uuid}/file-upload.csv',
                'manifests folder': f's3://nimhda-validation/{validation_uuid}/manifests/',
                'warnings json': f's3://nimhda-validation-results/{validation_uuid}/validation-warnings.json',
                'errors json': f's3://nimhda-validation-results/{validation_uuid}/validation-errors.json',
                'metadata json': f's3://nimhda-validation-results/{validation_uuid}/validation-metadata.json',
                'manifest json': f's3://nimhda-validation-results/{validation_uuid}/validation-manifests.json',
                'associated files json': f's3://nimhda-validation-results/{validation_uuid}/validation-associatedFiles.json',
            },
            'scope': None
        })

    return _validation_v2_response


@pytest.fixture
def ndar_subject01(validation_v2_response):
    return validation_v2_response('ndar_subject01')


@pytest.fixture
def fmri_results01(validation_v2_response):
    return validation_v2_response('fmrir_results01')


@pytest.fixture
def submission():
    def _submission(title, description, collection_id, status: SubmissionStatus):
        return Submission(**{
            'submission_status': status,
            'dataset_title': title,
            'dataset_description': description,
            'dataset_modified_date': None,
            'collection': {
                'id': collection_id,
                'title': 'Title of the collection'
            },
            'submission_id': 1,
            'dataset_created_date': '10/10/2023'
        })

    return _submission


@pytest.fixture
def results_writer():
    return MagicMock()


def test_submit_no_files(monkeypatch, upload_creds, ndar_subject01, user_collections, complete_submission_package,
                         submission, results_writer):
    ndar_subject01_creds = upload_creds(ndar_subject01.uuid)
    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', shlex.split('vtcmd ndarsubject01.csv -b -c 1860 -t test -d test -u testusername'))
        m.setattr(NDATools, '_get_password', MagicMock(return_value='testpassword'))
        # mock _save_username so we dont try to write information to disk while running tests.
        m.setattr(NDATools.Configuration.ClientConfiguration, '_save_username', MagicMock(return_value=None))
        m.setattr(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', MagicMock(return_value=True))
        m.setattr(NDATools.upload.validation.results_writer.ResultsWriterFactory, 'get_writer',
                  MagicMock(return_value=results_writer))
        # set the routing percent for v2 to 100
        m.setattr(NDATools.upload.validation.api.ValidationV2Api, 'get_v2_routing_percent', MagicMock(return_value=1))
        m.setattr(NDATools.upload.validation.api.ValidationV2Api, 'request_upload_credentials',
                  MagicMock(return_value=ndar_subject01_creds))
        m.setattr(NDATools.upload.validation.api.ValidationV2Api, 'wait_validation_complete',
                  MagicMock(return_value=ndar_subject01))
        # no manifests, so no need to mock manifests-uploader

        # collection api for building package step
        m.setattr(NDATools.upload.submission.api.CollectionApi, 'get_user_collections',
                  MagicMock(return_value=user_collections))
        m.setattr(NDATools.upload.submission.api.SubmissionPackageApi, 'build_package',
                  MagicMock(return_value=complete_submission_package))
        # mock the submission api calls
        m.setattr(NDATools.upload.submission.api.SubmissionApi, 'create_submission',
                  MagicMock(return_value=submission('test', 'test', 1860, SubmissionStatus.SUBMITTED)))
        # set mock logger so we can run tests on logged stmts
        m.setattr(NDATools.clientscripts.vtcmd, 'logger', MockLogger())
        NDATools.clientscripts.vtcmd.main()
        NDATools.clientscripts.vtcmd.logger.any_call_contains('Submission ID: 1')
        NDATools.clientscripts.vtcmd.logger.any_call_contains(
            'You have successfully completed uploading files for submission')
        # run verifications against mock methods
        ndar_subject01_creds.upload_csv.assert_called_once()
        results_writer.write_errors.assert_called_once()
        results_writer.write_warnings.assert_not_called()


def test_resume():
    pass


def test_replace_submission():
    pass
