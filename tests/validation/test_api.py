import time
from unittest.mock import patch, Mock, MagicMock

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

import NDATools
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationApi, ValidationV2


def test_pydantic_validation_errors(monkeypatch, validation_api):
    """Test that pydantic will cause errors when credentials do not contain fields as expected"""
    with monkeypatch.context() as m:
        # fake api response with missing access_key_id field
        mock_response = {
            # 'access_key_id': 'fake_access_key',
            'secret_access_key': 'fake_secret_key',
            'session_token': 'fake_session_token',
            'validation_uuid': 'fake_validation_uuid',
            'read_write_permission': {},
            'read_permission': {}
        }
        m.setattr(NDATools.upload.validation.api, 'post_request', Mock(side_effect=[mock_response]))
        # missing field causes Validation Error
        with pytest.raises(ValidationError):
            validation_api.initialize_validation_request('fmriresults01.csv')
        # adding the field back resolves the validation error
        mock_response['access_key_id'] = 'fake_access_key'
        m.setattr(NDATools.upload.validation.api, 'post_request', Mock(side_effect=[mock_response]))
        creds = validation_api.initialize_validation_request('fmriresults01.csv')
        assert creds.secret_access_key == 'fake_secret_key'
        assert creds.access_key_id == 'fake_access_key'
        assert creds.session_token == 'fake_session_token'


def test_refresh_credentials(monkeypatch):
    """ Test that credentials will be refreshed and upload resumed if credentials expire in middle of upload"""
    with monkeypatch.context() as m:
        mock_boto3 = MagicMock()
        m.setattr(NDATools.upload.validation.api, 'boto3', mock_boto3)

        # create mock for a successful get_object response
        mock_stream = Mock()
        mock_get_obj = {'Body': mock_stream}
        mock_stream.read.return_value = b'Hello World'
        # throw the mocked exception when the first client is called. on the second call, return success
        mock_s3 = MagicMock()
        mock_s3.get_object = MagicMock(side_effect=[ClientError({'Error': {'Code': 'ExpiredToken'}}, 'foo'),
                                                    mock_get_obj])
        mock_boto3.client = MagicMock(return_value=mock_s3)

        # fake api response with access_key, secret_key, and session_token
        api_response = {'access_key_id': 'fake_access_key',
                        'secret_access_key': 'fake_secret_key',
                        'session_token': 'fake_session_token',
                        'validation_uuid': 'fake_validation_uuid',
                        'read_write_permission': {},
                        'read_permission': {}
                        }

        # mock refresh function
        mock_refresh_func = Mock()
        v = ValidationV2Credentials(mock_refresh_func, **api_response)
        v.download('s3://fakebucket/fakekey.txt')
        # check that the refresh function was called once
        mock_refresh_func.assert_called_once()
        # check that the client was called twice
        assert mock_s3.get_object.call_count == 2

        # reset mocks
        mock_refresh_func.reset_mock()

        """Test that credentials wont be refreshed if the error caused by anything other than ExpiredCredentials """
        # update the test scenario. this time throw an invalidKey error. Refresh should not be called
        mock_s3.get_object.side_effect = [ClientError({'Error': {'Code': 'InvalidAccessKeyId'}}, 'foo')]
        try:
            v.download('s3://fakebucket/fakekey.txt')
            assert False
        except ClientError:
            mock_refresh_func.assert_not_called()


@pytest.fixture
def validation():
    def _validation_v2(status):
        uuid = '123e4567-e89b-12d3-a456-426614174000'
        return ValidationV2(validation_uuid=uuid, status=status, short_name=None, scope=None, rows=None,
                            validation_files={})

    return _validation_v2


@pytest.fixture
def pending_validation(validation):
    return validation('Pending')


@pytest.fixture
def error_validation(validation):
    return validation('Error')


@pytest.fixture
def completed_validation(validation):
    return validation('Pending')


@pytest.fixture
def validation_api():
    tmp = ValidationApi(config=MagicMock())
    tmp.get_validation = MagicMock()
    return tmp


@pytest.mark.parametrize('statuses,wait_manifest_upload', [
    (['Complete'], False),
    (['Uploading', 'Complete'], False),
    (['Uploading', 'Uploading', 'Complete'], False),
    (['Uploading', 'Uploading', 'PendingManifestFiles'], False),
    (['Uploading', 'Uploading', 'PendingManifestFiles', 'Complete'], True),
])
def test_wait_validation_complete(statuses, wait_manifest_upload, validation, validation_api, monkeypatch):
    """Test that 'wait_validation_complete' returns when expected depending on Validation status and wait_manifest_upload"""
    mocked_api_responses = list(map(lambda x: validation(x), statuses))
    validation_api.get_validation.side_effect = mocked_api_responses

    # patch time.sleep so the tests go fast
    with monkeypatch.context() as m:
        m.setattr(time, 'sleep', lambda x: None)
        returned_validation = validation_api.wait_validation_complete(mocked_api_responses[0].uuid, 5,
                                                                      wait_manifest_upload)
        assert returned_validation.uuid == mocked_api_responses[0].uuid
        assert returned_validation.status == mocked_api_responses[-1].status  # equals the last returned status
        validation_api.get_validation.assert_called_with(mocked_api_responses[0].uuid)
        assert validation_api.get_validation.call_count == len(statuses)


def test_wait_validation_complete_status_pending_wait_for_manifest_timeout(pending_validation, validation_api,
                                                                           monkeypatch):
    """Test that 'wait_validation_complete' raises a systemExit exception if validation status doesnt change"""
    validation_api.get_validation = MagicMock(return_value=pending_validation)

    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.validation.api, 'exit_error', MagicMock(side_effect=[SystemExit]))

        # catch SystemExit here so the test doesn't fail
        with pytest.raises(SystemExit) as exit_info:
            validation_api.wait_validation_complete(pending_validation.uuid, 1, True)

            validation_api.get_validation.assert_called_with(pending_validation.uuid)
            assert validation_api.get_validation.call_count == 1
            assert NDATools.upload.validation.api.exit_error.call_count == 1
            assert exit_info.value.code == 1


def test_wait_validation_complete_status_error_wait_for_manifest(pending_validation, error_validation, validation_api,
                                                                 monkeypatch):
    """Test that 'wait_validation_complete' raises a systemExit exception if validation status indicates unexpected backend error"""
    validation_api.get_validation = MagicMock(side_effect=[pending_validation, pending_validation, error_validation])
    with patch('NDATools.upload.validation.api.exit_error') as mock_exit:
        mock_exit.side_effect = SystemExit(1)

        # catch SystemExit here so the test doesn't fail
        with pytest.raises(SystemExit) as exit_info:
            validation_api.wait_validation_complete(pending_validation.uuid, 5, True)
            validation_api.get_validation.assert_called_with(pending_validation.uuid)
            assert validation_api.get_validation.call_count == 3
            assert mock_exit.call_count == 1
            assert exit_info.value.code == 1
