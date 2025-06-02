import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.clientscripts.vtcmd import validate
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationV2
from NDATools.upload.validation.results_writer import ResultsWriterABC
from tests.conftest import MockLogger


# overwrite the fixture to read from datadir instead of global shared_datadir
@pytest.fixture
def load_from_file(datadir):
    def _load_from_file(filename: str):
        with open(datadir / filename, 'r') as f:
            return f.read()

    return _load_from_file


@pytest.fixture
def validation_warnings(load_from_file):
    return json.loads(load_from_file('validation_warnings.json'))


@pytest.fixture
def validation_errors(load_from_file):
    return json.loads(load_from_file('validation_errors1.json'))


@pytest.fixture
def validation_sys_errors(load_from_file):
    return json.loads(load_from_file('validation_sys_errors.json'))


@pytest.fixture
def validation_result(monkeypatch):
    def _validation_result(filename: str = 'file.csv', warnings=None, errors=None):
        if not warnings:
            warnings = {}
        if not errors:
            errors = {}
        validation_uuid = str(uuid.uuid4())
        mock_creds = MagicMock(spec=ValidationV2Credentials)
        mock_creds.download_errors = MagicMock(return_value=errors)
        mock_creds.download_warnings = MagicMock(return_value=warnings)
        mock_creds.uuid = validation_uuid
        status = None
        if not errors and not warnings:
            status = 'Complete'
        elif errors:
            if 'system' in errors:
                status = 'SystemError'
            else:
                status = 'CompleteWithErrors'
        elif warnings:
            status = 'CompleteWithWarnings'
        return ValidatedFile(**{'file': Path(f'/path/to/{filename}'),
                                'v2_creds': mock_creds,
                                'v2_resource': ValidationV2(
                                    **{'validation_uuid': validation_uuid,
                                       'status': status,
                                       'short_name': 'fmriresults01',
                                       'rows': 1,
                                       'validation_files': {},
                                       'scope': None})})

    return _validation_result


@pytest.fixture
def config(top_level_datadir, validation_config_factory):
    file_path = (top_level_datadir / 'validation/file.csv')
    test_args = [str(file_path)]
    _, config = validation_config_factory(test_args)
    return config


@pytest.fixture
def validation_result_writer():
    writer = MagicMock(spec=ResultsWriterABC)
    writer.write_errors = MagicMock()
    writer.write_warnings = MagicMock()
    return writer


@pytest.mark.parametrize('test_warnings,test_errors,test_sys_errors', [
    (False, False, False),
    (False, True, False),
    (True, False, False),
    (True, True, False),
    (False, False, True),
])
def test_validate(test_warnings, test_errors, test_sys_errors,
                  validation_result, monkeypatch, validation_errors,
                  validation_warnings, validation_sys_errors, config, validation_result_writer):
    """Test that information is printed to screen as expected """
    # mock important config variables, including the upload_cli
    config.validation_results_writer = validation_result_writer
    config.upload_cli = MagicMock()
    config.is_authenticated = MagicMock(return_value=True)
    config.v2_enabled = True

    errors = {}
    if test_errors:
        errors = validation_errors
    elif test_sys_errors:
        errors = validation_sys_errors

    config.upload_cli.validate = MagicMock(return_value=[validation_result('file1.csv',
                                                                           validation_warnings if test_warnings else {},
                                                                           errors)])

    with monkeypatch.context() as m:
        # set this flag to enable printout of extra messages when errors are detected in 1 or more csvs
        m.setattr(config._args, 'buildPackage', True)
        # set this flag when testing warnings
        if test_warnings:
            m.setattr(config._args, 'warning', True)

        # setup mocks to test logging calls
        mock_logger = MockLogger()
        m.setattr(NDATools.clientscripts.vtcmd.logger, 'info', mock_logger)
        m.setattr(NDATools.clientscripts.vtcmd, 'exit_error', MagicMock(side_effect=[SystemExit]))
        try:
            validate(config._args, config)
        except SystemExit:
            assert test_sys_errors or test_errors
            assert NDATools.clientscripts.vtcmd.exit_error.call_count == 1
            if test_sys_errors:
                assert mock_logger.any_call_contains('Unexpected error occurred while validating')

        # check that program outputs message indicating validation completion
        assert mock_logger.any_call_contains('All files have finished validating')
        assert validation_result_writer.write_errors.call_count == 1

        if test_warnings:
            assert mock_logger.any_call_contains('Warnings output to:')
            assert validation_result_writer.write_warnings.call_count == 1
