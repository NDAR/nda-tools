import builtins
import getpass
import os
from unittest.mock import patch, MagicMock

import keyring
from keyring.errors import KeyringLocked

import NDATools.Configuration
from NDATools.Configuration import ClientConfiguration
from tests.conftest import MockLogger

config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/mock_settings.cfg')
username = 'test_username'
password = 'test_password'


def set_username_in_config(keep_username):
    with open(config_file_path, 'r') as file:
        lines = file.readlines()
    keep_lines = [line for line in lines if 'username' not in line]

    with open(config_file_path, 'w') as configfile:
        if not keep_username:
            keep_lines.append('\nusername =')
            configfile.writelines(keep_lines)
        else:
            keep_lines.append('\nusername = ' + username)
            configfile.writelines(keep_lines)


def test_read_user_credentials_no_username_set():
    set_username_in_config(False)
    mock_logger = MockLogger()

    with patch.object(NDATools.Configuration.logger, 'info', mock_logger), \
        patch('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path), \
            patch.object(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None), \
            patch.object(ClientConfiguration, 'is_valid_nda_credentials', side_effect=[True]), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        client_config = ClientConfiguration(MagicMock())
        client_config.username = None
        client_config._use_keyring = False

        client_config.read_user_credentials(True)

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


def test_read_user_credentials_has_username_set_no_password_in_keyring():
    set_username_in_config(True)
    keyring.set_password('nda-tools', username, '')
    mock_logger = MockLogger()

    with patch.object(NDATools.Configuration.logger, 'info', mock_logger), \
        patch('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path), \
            patch.object(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None), \
            patch.object(ClientConfiguration, 'is_valid_nda_credentials', side_effect=[True]), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)

        client_config.read_user_credentials(True)

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


def test_read_user_credentials_has_username_set_has_password_in_keyring():
    set_username_in_config(True)
    keyring.set_password('nda-tools', username, 'test_password')
    mock_logger = MockLogger()

    with patch.object(NDATools.Configuration.logger, 'info', mock_logger), \
        patch('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path), \
        patch.object(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None), \
        patch.object(ClientConfiguration, 'is_valid_nda_credentials', side_effect=[True]), \
        patch('builtins.input', return_value=username) as mock_get_username, \
        patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)

        client_config.read_user_credentials(True)

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


def test_read_user_credentials_reenter_credentials():
    set_username_in_config(False)
    mock_logger = MockLogger()

    with patch.object(NDATools.Configuration.logger, 'info', mock_logger), \
        patch('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path), \
        patch.object(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None), \
        patch.object(ClientConfiguration, 'is_valid_nda_credentials', side_effect=[False, True]), \
        patch('builtins.input', return_value=username) as mock_get_username, \
        patch('getpass.getpass', return_value=password) as mock_get_password:
        client_config = ClientConfiguration(MagicMock())
        client_config.username = None
        client_config._use_keyring = False

        client_config.read_user_credentials(True)

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


def test_no_keyring(monkeypatch):
    # mock keyring not installed on client machine.
    with monkeypatch.context() as m:
        m.setattr('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path)
        m.setattr(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None)
        # keyring is set to None if there is an import error
        m.setattr(NDATools.Configuration, 'keyring', None)
        client_config = ClientConfiguration(MagicMock())
        # reset the username field to None in case there was a username in the settings.cfg file
        client_config.username = None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(client_config, 'is_valid_nda_credentials', lambda: True)
        # patch this method to avoid writing to any files
        m.setattr(client_config, '_save_username', lambda: None)
        client_config.read_user_credentials(True)
        assert builtins.input.call_count == 1
        assert getpass.getpass.call_count == 1
        assert client_config._use_keyring == False

    # mock error retrieving password from keyring. should not cause program to crash.
    with monkeypatch.context() as m:
        m.setattr('NDATools.NDA_TOOLS_SETTINGS_CFG_FILE', config_file_path)
        m.setattr(ClientConfiguration, '_check_and_fix_missing_options', lambda x: None)
        mock_keyring = MagicMock()
        mock_keyring.get_password = MagicMock(side_effect=KeyringLocked)
        m.setattr(NDATools.Configuration, 'keyring', mock_keyring)
        client_config = ClientConfiguration(MagicMock())
        # reset the username field to None in case there was a username in the settings.cfg file
        client_config.username = None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(NDATools.Configuration.logger, 'warning', MockLogger())
        m.setattr(client_config, 'is_valid_nda_credentials', lambda: True)
        # patch this method to avoid writing to any files
        m.setattr(client_config, '_save_username', lambda: None)
        client_config.read_user_credentials(True)
        assert builtins.input.call_count == 1
        assert getpass.getpass.call_count == 1
        assert client_config._use_keyring == False
        assert NDATools.Configuration.keyring.get_password.call_count == 1
        assert NDATools.Configuration.logger.warning.any_call_contains('could not retrieve password from keyring:')
