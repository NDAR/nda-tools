import sys
from unittest import mock

import keyring
import pytest

from NDATools.clientscripts.downloadcmd import configure as download_configure, parse_args as download_parse_args
from NDATools.clientscripts.vtcmd import configure as validation_configure, parse_args as validation_parse_args
from NDATools.Configuration import ClientConfiguration


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
            config = download_configure(args)
            return args, config
    return _make_config


@pytest.fixture
def validation_config_factory():
    def _make_val_config(test_args):
        with mock.patch.object(sys, 'argv', test_args):
            test_args.insert(0, 'vtcmd')
            args = validation_parse_args()
            config = validation_configure(args)
        return args, config

    return _make_val_config


@pytest.fixture
def load_from_file(shared_datadir):
    def _load_from_file(file):
        content = (shared_datadir / file).read_text()
        return content

    return _load_from_file