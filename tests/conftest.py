import json
import os
import sys
from unittest import mock

import pytest

from NDATools.clientscripts.downloadcmd import configure as download_configure
from NDATools.clientscripts.downloadcmd import parse_args as download_parse_args
from NDATools.clientscripts.vtcmd import parse_args as validation_parse_args
from NDATools.clientscripts.vtcmd import configure as validation_configure


@pytest.fixture
def download_config_factory():
    def _make_config(test_args):
        with mock.patch.object(sys, 'argv', test_args):
            test_args.insert(0, 'downloadcmd')
            args = download_parse_args()
            config = download_configure(args.username, args.password)
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