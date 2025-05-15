import builtins
import getpass
from unittest.mock import patch, MagicMock

import keyring
import pytest
from keyring.errors import KeyringLocked

import NDATools.Configuration
from NDATools.Configuration import ClientConfiguration
from tests.conftest import MockLogger

username = 'test_username'
password = 'test_password'


@pytest.fixture
def mock_settings_with_user(shared_datadir):
    return shared_datadir / 'mock_settings.cfg'


@pytest.fixture
def mock_settings_no_user(shared_datadir):
    return shared_datadir / 'mock_settings.cfg'


def test_read_user_credentials_no_username_set(mock_settings_no_user):
    mock_logger = MockLogger()

    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', side_effect=[True]), \
            patch.object(NDATools, '_get_keyring', False), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        client_config = ClientConfiguration(MagicMock())
        client_config.username = None

        NDATools.authenticate(client_config)

        mock_logger.any_call_contains(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        mock_logger.any_call_contains(
            'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
        mock_logger.any_call_contains(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
        mock_logger.any_call_contains(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')

        mock_get_username.assert_called_once_with('Enter your NDA account username:')
        mock_get_password.assert_called_once_with('Enter your NDA account password:')


def test_read_user_credentials_has_username_set_no_password_in_keyring(mock_settings_with_user):
    keyring.set_password('nda-tools', username, '')
    mock_logger = MockLogger()

    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', side_effect=[True]), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)

        NDATools.authenticate(client_config)

        mock_logger.assert_no_call_contains(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        mock_logger.assert_no_call_contains(
            'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
        mock_logger.assert_no_call_contains(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
        mock_logger.assert_no_call_contains(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')

        mock_get_username.assert_not_called()
        mock_get_password.assert_called_once_with('Enter your NDA account password:')


def test_read_user_credentials_has_username_set_has_password_in_keyring(mock_settings_with_user):
    keyring.set_password('nda-tools', username, 'test_password')
    mock_logger = MockLogger()

    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', side_effect=[True]), \
            patch.object(NDATools, '_get_keyring', True), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('keyring.get_password', return_value=password) as mock_keyring, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)
        NDATools.authenticate(client_config)

        mock_logger.assert_no_call_contains(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        mock_logger.assert_no_call_contains(
            'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
        mock_logger.assert_no_call_contains(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
        mock_logger.assert_no_call_contains(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')

        mock_get_username.assert_not_called()
        mock_get_password.assert_not_called()


def test_read_user_credentials_reenter_credentials(mock_settings_no_user):
    mock_logger = MockLogger()

    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', side_effect=[False, True]), \
            patch.object(NDATools, '_get_keyring', False), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        client_config = ClientConfiguration(MagicMock())
        client_config.username = None

        NDATools.authenticate(client_config)

        mock_logger.any_call_contains(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        mock_logger.any_call_contains(
            'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
        mock_logger.any_call_contains(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
        mock_logger.any_call_contains(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')

        assert mock_get_username.call_count == 2
        assert mock_get_password.call_count == 2


def test_no_keyring(monkeypatch, mock_settings_with_user):
    # mock keyring not installed on client machine.
    with monkeypatch.context() as m:
        # keyring is set to None if there is an import error
        m.setattr(NDATools, 'keyring', None)
        client_config = ClientConfiguration(MagicMock())
        # reset the username field to None in case there was a username in the settings.cfg file
        client_config.username = None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', lambda x, y, z: True)
        # patch this method to avoid writing to any files
        m.setattr(NDATools, '_try_save_password_keyring', lambda x, y: None)
        NDATools.authenticate(client_config)
        assert builtins.input.call_count == 1
        assert getpass.getpass.call_count == 1
        assert NDATools._get_keyring == False

    # mock error retrieving password from keyring. should not cause program to crash.
    with monkeypatch.context() as m:
        mock_keyring = MagicMock()
        m.setattr(NDATools, '_get_keyring', True)
        mock_keyring.get_password = MagicMock(side_effect=KeyringLocked)
        m.setattr(NDATools, 'keyring', mock_keyring)
        m.setattr(NDATools.logger, 'warning', MockLogger())
        client_config = ClientConfiguration(MagicMock())
        # reset the username field to None in case there was a username in the settings.cfg file
        client_config.username = None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(NDATools.Configuration.logger, 'warning', MockLogger())
        m.setattr(NDATools.upload.submission.api.UserApi, 'is_valid_nda_credentials', lambda x, y, z: True)
        # patch this method to avoid writing to any files
        m.setattr(NDATools, '_try_save_password_keyring', lambda x, y: None)
        NDATools.authenticate(client_config)
        assert builtins.input.call_count == 1
        assert getpass.getpass.call_count == 1
        assert NDATools._get_keyring == False
        assert NDATools.keyring.get_password.call_count == 1
        assert NDATools.logger.warning.any_call_contains('could not retrieve password from keyring:')
