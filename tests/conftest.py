import pathlib
import sys
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
def mock_nda_paths(tmp_path):
    downloadcmd_logs_folder = tmp_path / "logs"
    downloadcmd_logs_folder.mkdir()
    return {
        "nda_tools_downloads_folder": tmp_path,
        "nda_tools_downloadcmd_logs_folder": downloadcmd_logs_folder,
    }


@pytest.fixture
def mock_settings_file(tmp_path):
    settings_file = tmp_path / "settings.cfg"
    default_settings_file = pathlib.Path(NDATools.__file__).parent / "clientscripts" / "config" / "settings.cfg"
    settings_file.write_text(default_settings_file.read_text())
    return settings_file


@pytest.fixture
def download_config_factory(monkeypatch, mock_nda_paths, mock_settings_file):
    def _make_config(test_args):
        with monkeypatch.context() as m:
            test_args.insert(0, 'downloadcmd')
            m.setattr(sys, 'argv', test_args)
            m.setattr(keyring, 'get_password', mock_get_password)
            m.setattr(NDATools, 'NDA_TOOLS_SETTINGS_CFG_FILE', str(mock_settings_file))
            args = download_parse_args()
            config = ClientConfiguration(args)
            config._nda_paths = mock_nda_paths
            return args, config

    return _make_config


@pytest.fixture
def validation_config_factory(monkeypatch, mock_settings_file):
    def _make_val_config(test_args):
        with monkeypatch.context() as m:
            test_args.insert(0, 'vtcmd')
            m.setattr(sys, 'argv', test_args)
            m.setattr(NDATools, 'NDA_TOOLS_SETTINGS_CFG_FILE', str(mock_settings_file))
            args = validation_parse_args()
            config = ClientConfiguration(args)
        return args, config

    return _make_val_config


@pytest.fixture
def top_level_datadir():
    return pathlib.Path(__file__).parent.absolute() / 'data'


@pytest.fixture
def load_from_file(top_level_datadir):
    def _load_from_file(file):
        content = (top_level_datadir / file).read_text()
        return content

    return _load_from_file


class MockLogger(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def logged_lines(self):
        return [args for call in self.call_args_list for args in call.args if args is not None]

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


@pytest.fixture
def s3_mock():
    return MagicMock()
