import sys
from unittest import mock
from unittest.mock import MagicMock

import keyring
import pytest

import NDATools
from NDATools.Configuration import ClientConfiguration
from NDATools.clientscripts.downloadcmd import parse_args as download_parse_args
from NDATools.clientscripts.vtcmd import parse_args as validation_parse_args


# prevent check_version from running in tests when releasing to prod
@pytest.fixture(autouse=True)
def no_check_version(monkeypatch):
    monkeypatch.setattr(NDATools, "check_version", lambda: None)


# prevent tests from making any real http requests
@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove requests.sessions.Session.request for all tests."""
    monkeypatch.delattr("requests.sessions.Session.request")


def mock_get_password(*args, **kwargs):
    return 'fake-pass'


def mock_is_valid_credentials(*args, **kwargs):
    return True


@pytest.fixture
def download_config_factory(monkeypatch):
    def _make_config(test_args):
        with monkeypatch.context() as m:
            test_args.insert(0, 'downloadcmd')
            m.setattr(sys, 'argv', test_args)
            m.setattr(keyring, 'get_password', mock_get_password)
            m.setattr(ClientConfiguration, 'is_valid_nda_credentials', mock_is_valid_credentials)
            args = download_parse_args()
            config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, auth_req=False)
            # monkey patch is_valid_nda_credentials
            config.is_valid_nda_credentials = lambda _: True
            return args, config

    return _make_config


@pytest.fixture
def validation_config_factory():
    def _make_val_config(test_args):
        with mock.patch.object(sys, 'argv', test_args):
            test_args.insert(0, 'vtcmd')
            args = validation_parse_args()
            config = NDATools.init_and_create_configuration(args, NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, auth_req=False)
            # monkey patch is_valid_nda_credentials
            config.is_valid_nda_credentials = lambda _: True

        return args, config

    return _make_val_config


@pytest.fixture
def load_from_file(shared_datadir):
    def _load_from_file(file):
        content = (shared_datadir / file).read_text()
        return content

    return _load_from_file


class MockLogger(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def logged_lines(self):
        return [args for call in self.call_args_list for args in call.args]

    def assert_any_call_contains(self, str): assert self.any_call_contains(str)

    def assert_no_call_contains(self, str): assert not self.any_call_contains(str)

    def any_call_contains(self, str): return any(map(lambda line: str in line, self.logged_lines))


@pytest.fixture
def logger_mock(monkeypatch):
    # add mock for logger so we can run verifications on what was output
    logger_mock = MagicMock()
    logger_mock.assert_any_call_contains = lambda x: any(map(lambda y: x in y[0], logger_mock.call_args_list))
    logger = MagicMock()
    logger.info = MockLogger()
    logger.error = MockLogger()
    logger.debug = MockLogger()
    return logger
