import json
import uuid
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.upload.cli import ValidatedFile, ValidationError, QaResults, NdaUploadCli
from NDATools.upload.validation.api import ValidationV2, ValidationV2Credentials, ManifestError, Qa


@pytest.fixture
def v2_creds():
    res = {
        'access_key_id': 'AWSAKIDASLDASDASD',
        'secret_access_key': 'asdfasdfkjla;sdflaksjdf',
        'session_token': 'as;dlfas;ldkfjas;ldfkj',
        'validation_uuid': '4a1c6882-fb59-4464-93a0-7c12540e1549',
        'read_write_permission': {
            'csv data': 's3://nimhda-validation/4a1c6882-fb59-4464-93a0-7c12540e1549/fmriresults01-data.csv'
        },
        'read_permission': {
            'warnings json': 's3://nimhda-validation-results/4a1c6882-fb59-4464-93a0-7c12540e1549/validation-warnings.json',
            'errors json': 's3://nimhda-validation-results/4a1c6882-fb59-4464-93a0-7c12540e1549/validation-errors.json',
            'manifest json': 's3://nimhda-validation-results/4a1c6882-fb59-4464-93a0-7c12540e1549/validation-manifests.json',
            'associated json': 's3://nimhda-validation-results/4a1c6882-fb59-4464-93a0-7c12540e1549/validation-associatedFiles.json',
        },
    }
    refresh_func = lambda x: Exception('not implementing refresh...')
    return ValidationV2Credentials(refresh_func, **res)


@pytest.fixture
def v2_resource():
    res = {
        'validation_uuid': '4a1c6882-fb59-4464-93a0-7c12540e1549',
        'status': 'CompleteWithWarnings',
        'short_name': 'fmriresults01',
        'rows': 10,
        'scope': None,
        'validation_files': {

        }
    }
    return ValidationV2(**res)


@pytest.fixture
def manifest_errors():
    res = {
        'uuid': '4a1c6882-fb59-4464-93a0-7c12540e1549',
        'errors': [
            "The following are not valid file paths: /Users/jil02/NYU Langone Health Dropbox/Lanxin Ji/LANXIN_MORIAH_SHARE_FOLDER/NDA_DATA_SHARING/DATA_BIDS_DCM/NDARAF455PWX/f1/anat/.DS_Store,/Users/jil02/NYU Langone Health Dropbox/Lanxin Ji/LANXIN_MORIAH_SHARE_FOLDER/NDA_DATA_SHARING/DATA_BIDS_DCM/NDARAF455PWX/f1/anat/T2/.DS_Store,/Users/jil02/NYU Langone Health Dropbox/Lanxin Ji/LANXIN_MORIAH_SHARE_FOLDER/NDA_DATA_SHARING/DATA_BIDS_DCM/NDARAF455PWX/.DS_Store,/Users/jil02/NYU Langone Health Dropbox/Lanxin Ji/LANXIN_MORIAH_SHARE_FOLDER/NDA_DATA_SHARING/DATA_BIDS_DCM/NDARAF455PWX/f1/.DS_Store",
            "The following are not valid file names: .DS_Store,.DS_Store,.DS_Store,.DS_Store"
        ]
    }
    return [ManifestError(**res)]


@pytest.fixture
def validated_file(v2_resource, v2_creds, validation_errors, manifest_errors, monkeypatch, tmp_path):
    def _validated_file(errors):
        validated_file = ValidatedFile(tmp_path / 'associated-file1.png', v2_resource=v2_resource, v2_creds=v2_creds,
                                       manifest_errors={})
        monkeypatch.setattr(validated_file, '_errors', errors)
        # monkeypatch.setattr(validated_file, '_manifest_errors', manifest_errors)
        return validated_file

    return _validated_file


@pytest.fixture
def validation_errors(load_from_file):
    def _validation_errors(file):
        file_content = load_from_file(file)
        file_json = json.loads(file_content)['errors']
        return [ValidationError(m.get('recordNumber'), m.get('columnName'), m.get('message'), err_type)
                for err_type, errors in file_json.items() for m in errors]

    return _validation_errors


class TestNdaUploadCli:

    def test_output_validation_error_messages(self, validated_file, validation_errors, monkeypatch, logger_mock):
        """ Test the return value of the preview_validation_errors procedure """
        with monkeypatch.context() as c:
            c.setattr(NDATools.upload.cli, 'logger', logger_mock)
            validated_file(validation_errors('validation/validation_errors1.json')).preview_validation_errors(10)

            # table_list is a list of strings
            logger_mock.info.assert_any_call_contains('Row')
            logger_mock.info.assert_any_call_contains('Column')
            logger_mock.info.assert_any_call_contains('Message')
            logger_mock.info.assert_any_call_contains('ampscz_missing_spec')
            logger_mock.info.assert_any_call_contains('chrhealth_alleoth')
            logger_mock.info.assert_any_call_contains('interview_date')

        with monkeypatch.context() as c:
            c.setattr(NDATools.upload.cli, 'logger', logger_mock)
            validated_file(validation_errors('validation/validation_errors2.json')).preview_validation_errors(10)
            logger_mock.info.assert_any_call_contains('Row')
            logger_mock.info.assert_any_call_contains('Column')
            logger_mock.info.assert_any_call_contains('Message')
            logger_mock.info.assert_any_call_contains('sex')

    def test_preview_qa_errors(self, monkeypatch, logger_mock):
        '''Test the output of the preview_qa_errors procedure'''
        with monkeypatch.context() as c:
            c.setattr(NDATools.upload.cli, 'logger', logger_mock)
            errors = {"inconsistentSex": [{"columnName": "sex",
                                           "message": "GUID NDARDF005KBU has multiple values for sex. Values for sex found: M, F.",
                                           "guid": "NDARDF005KBU"}]}

            qa_uuid = str(uuid.uuid4())
            QaResults(qa_uuid, errors).preview_errors(10)

            # table_list is a list of strings
            logger_mock.info.assert_any_call_contains('Error')
            logger_mock.info.assert_any_call_contains('Guid/Src-Subject-Id')
            logger_mock.info.assert_any_call_contains('Message')
            logger_mock.info.assert_any_call_contains('inconsistentSex')
            logger_mock.info.assert_any_call_contains('NDARDF005KBU')
            logger_mock.info.assert_any_call_contains(
                'GUID NDARDF005KBU has multiple values for sex. Values for sex found: M, F.')

    def test_qa_validated_files(self, monkeypatch, validated_file, validation_errors, config):
        with monkeypatch.context() as c:
            v = validated_file({})
            upload_cli = NdaUploadCli(config)

            api_response = {
                "qa-uuid": "0da49f9d-6704-415e-80ef-fc1d44a8b178",
                "status": "Complete",
                "done": True,
                "errors-json": "s3://results-bucket/qa/0da49f9d-6704-415e-80ef-fc1d44a8b178/qa-results.json",
                "errors": json.dumps({"inconsistentSex": [{"columnName": "sex",
                                                           "message": "GUID NDARDF005KBU has multiple values for sex. Values for sex found: M, F.",
                                                           "guid": "NDARDF005KBU"}]}),
                "validations": [{}]}
            monkeypatch.setattr(upload_cli.config, 'validation_api', MagicMock())
            monkeypatch.setattr(upload_cli.config.validation_api, 'qa_validated_files',
                                MagicMock(return_value=Qa(**api_response)))
            results = upload_cli.qa_validated_files([v])
            assert results.qa_uuid == api_response['qa-uuid']
            assert results.errors[
                       0].message == 'GUID NDARDF005KBU has multiple values for sex. Values for sex found: M, F.'
            assert results.errors[0].guid == 'NDARDF005KBU'
            assert results.errors[0].err_code == 'inconsistentSex'
            assert results.errors[0].column_name == 'sex'
            assert results.errors[0].src_subject_id is None
            assert results.has_errors() == True

            # test empty errors
            api_response['errors'] = ''
            monkeypatch.setattr(upload_cli.config.validation_api, 'qa_validated_files',
                                MagicMock(return_value=Qa(**api_response)))
            results = upload_cli.qa_validated_files([v])
            assert results.has_errors() == False
            assert results.errors == []
            assert results.qa_uuid == api_response['qa-uuid']
