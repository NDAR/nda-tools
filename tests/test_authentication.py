import builtins
import getpass
import requests
import threading
import time
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


def test_read_user_credentials_no_username_set(mock_settings_with_user):
    mock_logger = MockLogger()
    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.RasAuthApi, 'login', side_effect=['token-123']), \
            patch.object(NDATools, '_get_keyring', False), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = None
        client_config = ClientConfiguration(args)

        client_config.authenticate()

        assert client_config.username == username
        assert client_config.password == password
        assert client_config.token == 'token-123'
        assert client_config.config.get('User', 'username') == username

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
    keyring.set_password('nda-tools', username, '')
    mock_logger = MockLogger()

    with patch.object(NDATools.logger, 'info', mock_logger), \
            patch.object(NDATools.upload.submission.api.RasAuthApi, 'login', side_effect=['token-123']), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)
        client_config._save_username = lambda: None
        client_config._save_apis = lambda: None

        client_config.authenticate()

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
            patch.object(NDATools.upload.submission.api.RasAuthApi, 'login', side_effect=['token-123']), \
            patch.object(NDATools, '_get_keyring', True), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('keyring.get_password', return_value=password) as mock_keyring, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        args = MagicMock()
        args.username = 'test_username'
        client_config = ClientConfiguration(args)
        client_config._save_username = lambda: None
        client_config._save_apis = lambda: None
        client_config.authenticate()

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
            patch.object(NDATools.upload.submission.api.RasAuthApi, 'login', side_effect=[None, 'token-123']), \
            patch.object(NDATools, '_get_keyring', False), \
            patch('builtins.input', return_value=username) as mock_get_username, \
            patch('getpass.getpass', return_value=password) as mock_get_password:
        client_config = ClientConfiguration(MagicMock())
        client_config.username = None
        client_config._save_username = lambda: None
        client_config._save_apis = lambda: None

        client_config.authenticate()

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
        # keyring is set to None if there is an import error
        m.setattr(NDATools, 'keyring', None)
        client_config = ClientConfiguration(MagicMock())
        # reset the username field to None in case there was a username in the settings.cfg file
        client_config.username = None
        client_config._save_username = lambda: None
        client_config._save_apis = lambda: None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(NDATools.upload.submission.api.RasAuthApi, 'login', lambda x, y, z: 'token-123')
        # patch this method to avoid writing to any files
        m.setattr(NDATools, '_try_save_password_keyring', lambda x, y: None)
        client_config.authenticate()
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
        client_config._save_username = lambda: None
        client_config._save_apis = lambda: None
        m.setattr('builtins.input', MagicMock(side_effect=username))
        m.setattr('getpass.getpass', MagicMock(side_effect=password))
        m.setattr(NDATools.Configuration.logger, 'warning', MockLogger())
        m.setattr(NDATools.upload.submission.api.RasAuthApi, 'login', lambda x, y, z: 'token-123')
        # patch this method to avoid writing to any files
        m.setattr(NDATools, '_try_save_password_keyring', lambda x, y: None)
        client_config.authenticate()
        assert builtins.input.call_count == 1
        assert getpass.getpass.call_count == 1
        assert NDATools._get_keyring == False
        assert NDATools.keyring.get_password.call_count == 1
        assert NDATools.logger.warning.any_call_contains('could not retrieve password from keyring:')


def test_client_configuration_auth_uses_latest_credentials(monkeypatch):
    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        client_config.username = 'first_user'
        client_config.password = 'first_password'
        client_config.token = 'first_token'

        auth = client_config.get_auth()

        request1 = requests.Request('GET', 'https://nda.nih.gov/api/package').prepare()
        auth(request1)

        client_config.token = 'second_token'
        request2 = requests.Request('GET', 'https://nda.nih.gov/api/package').prepare()
        auth(request2)

    assert request1.headers['Authorization'] != request2.headers['Authorization']
    assert request2.headers['Authorization'] == 'Bearer second_token'


def test_client_configuration_derives_ras_login_endpoint_from_ras_base(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.cfg'
    settings_file.write_text(
        "[Endpoints]\n"
        "ras = https://revengers.nimhda.org/api/ras\n"
        "package = https://revengers.nimhda.org/api/package\n"
        "validation = https://revengers.nimhda.org/api/validation\n"
        "submission_package = https://revengers.nimhda.org/api/submission-package\n"
        "submission = https://revengers.nimhda.org/api/submission\n"
        "validationtool = https://revengers.nimhda.org/api/validationtool/v2\n"
        "datadictionary = https://revengers.nimhda.org/api/datadictionary/datastructure\n"
        "package_creation = https://revengers.nimhda.org/api/packaging-ws\n"
        "collection = https://revengers.nimhda.org/api/collection\n"
        "\n"
        "[User]\n"
        "username = test_username\n"
        "[Paths]\n"
        "nda_organization_root_dir =\n"
    )

    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)

    assert client_config.ras_login_api_endpoint == 'https://revengers.nimhda.org/api/ras/user/login'


def test_client_configuration_reauthenticate_uses_stored_credentials(monkeypatch):
    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        client_config.username = username
        client_config.password = password
        client_config.token = 'old-token'
        m.setattr(NDATools.upload.submission.api.RasAuthApi, 'login', lambda x, y, z: 'new-token')
        m.setattr(client_config, '_save_username', lambda: None)
        m.setattr(client_config, '_save_apis', lambda: None)
        mock_input = MagicMock()
        mock_getpass = MagicMock()
        m.setattr('builtins.input', mock_input)
        m.setattr('getpass.getpass', mock_getpass)

        client_config.reauthenticate()

    mock_input.assert_not_called()
    mock_getpass.assert_not_called()
    assert client_config.username == username
    assert client_config.password == password
    assert client_config.token == 'new-token'


def test_client_configuration_save_apis_uses_shared_auth(monkeypatch):
    with monkeypatch.context() as m:
        validation_api = MagicMock()
        submission_package_api = MagicMock()
        submission_api = MagicMock()
        collection_api = MagicMock()
        m.setattr(NDATools.Configuration, 'ValidationV2Api', validation_api)
        m.setattr(NDATools.Configuration, 'SubmissionPackageApi', submission_package_api)
        m.setattr(NDATools.Configuration, 'SubmissionApi', submission_api)
        m.setattr(NDATools.Configuration, 'CollectionApi', collection_api)

        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        m.setattr(client_config, '_save_username', lambda: None)
        client_config.update_with_auth(username, password, 'token-123')

    assert validation_api.call_args.kwargs['auth'] is client_config.get_auth()
    assert submission_package_api.call_args.kwargs['auth'] is client_config.get_auth()
    assert submission_api.call_args.kwargs['auth'] is client_config.get_auth()
    assert collection_api.call_args.kwargs['auth'] is client_config.get_auth()


def test_client_configuration_reauthenticate_single_flight(monkeypatch):
    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        client_config.username = username
        client_config.password = password
        client_config.token = 'old-token'
        mtx = threading.Lock()
        call_count = {'count': 0}
        started = threading.Event()
        release = threading.Event()

        def fake_authenticate():
            with mtx:
                call_count['count'] += 1
            started.set()
            release.wait(timeout=1)
            client_config.username = username
            client_config.password = password
            client_config.token = 'new-token'

        m.setattr(client_config, 'authenticate', fake_authenticate)
        m.setattr(client_config, '_save_username', lambda: None)
        m.setattr(client_config, '_save_apis', lambda: None)

        threads = [threading.Thread(target=client_config.reauthenticate, kwargs={'token_version': 0})]
        threads[0].start()
        started.wait(timeout=1)
        threads.append(threading.Thread(target=client_config.reauthenticate, kwargs={'token_version': 0}))
        threads[1].start()
        time.sleep(0.05)
        release.set()
        for thread in threads:
            thread.join()

    assert call_count['count'] == 1
    assert client_config.get_auth_generation() == 1


def test_client_configuration_reauthenticate_skips_stale_generation(monkeypatch):
    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        client_config.username = username
        client_config.password = password
        client_config._auth_generation = 1
        authenticate = MagicMock()
        m.setattr(client_config, 'authenticate', authenticate)
        client_config.reauthenticate(token_version=0)

    authenticate.assert_not_called()


def test_client_configuration_reauthenticate_propagates_failure_to_waiters(monkeypatch):
    with monkeypatch.context() as m:
        client_config = ClientConfiguration(MagicMock())
        m.setattr(client_config, '_check_and_fix_missing_options', lambda x: None)
        client_config.username = username
        client_config.password = password
        started = threading.Event()
        release = threading.Event()
        call_count = {'count': 0}
        errors = []

        def fake_authenticate(config):
            call_count['count'] += 1
            started.set()
            release.wait(timeout=1)
            raise RuntimeError('reauth failed')

        def run():
            try:
                client_config.reauthenticate(token_version=0)
            except Exception as exc:
                errors.append(str(exc))

        m.setattr(ClientConfiguration, 'authenticate', fake_authenticate)
        threads = [threading.Thread(target=run)]
        threads[0].start()
        started.wait(timeout=1)
        threads.append(threading.Thread(target=run))
        threads[1].start()
        time.sleep(0.05)
        release.set()
        for thread in threads:
            thread.join()

    assert call_count['count'] == 1
    assert errors == ['reauth failed', 'reauth failed']
