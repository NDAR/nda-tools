import json
import os
import sys
from unittest import mock

import pytest

from NDATools.clientscripts.downloadcmd import configure as download_configure
from NDATools.clientscripts.downloadcmd import parse_args as download_parse_args



@pytest.fixture
def download_config_factory():
    def _make_config(test_args):
        with mock.patch.object(sys, 'argv', test_args):
            args = download_parse_args()
            config = download_configure(args.username, args.password)
        return args, config

    return _make_config


@pytest.fixture
def load_from_file(shared_datadir):
    def _load_from_file(file):
        content = (shared_datadir / file).read_text()
        return content

    return _load_from_file

