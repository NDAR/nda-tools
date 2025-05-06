import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationV2
from NDATools.upload.validation.results_writer import ResultsWriterFactory
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


@pytest.mark.parametrize('test_warnings,test_errors', [
    (False, False),
    (False, True),
    (True, False),
    (True, True),
])
@pytest.mark.skip
def test_validate(test_warnings, test_errors, validation_result, monkeypatch, validation_errors,
                  validation_warnings, config):
    """Test that information is printed to screen as expected """
    user_io = ResultsWriterFactory.get_writer(file_format='json')
    mock_file_writer = MagicMock()
    mock_logger = MockLogger()
    result = validation_result('file1.csv',
                               validation_warnings if test_warnings else {},
                               validation_errors if test_errors else {})
    with monkeypatch.context() as m:
        # mock file writer so we dont write actual files
        m.setattr(user_io, 'file_writer', mock_file_writer)
        # test logging calls
        m.setattr(NDATools.upload.validation.io.logger, 'info', mock_logger)
        m.setattr(NDATools.upload.validation.io, 'exit_error', MagicMock(return_value=None))

        user_io.save_validation_errors([result], save_warnings=False)
        validate()

        # check that program outputs message indicating validation completion
        assert mock_logger.any_call_contains('All files have finished validating')
        assert mock_file_writer.write_errors.call_count == 1

        # check that program doesnt indicate a system error or warnings if validation status is Complete
        assert not mock_logger.info.any_call_contains('Unexpected error')
        if test_warnings and not test_errors:
            assert mock_logger.any_call_contains('Note: Your data has warnings')
        else:
            assert not mock_logger.any_call_contains('Note: Your data has warnings')
        assert mock_file_writer.write_warnings.call_count == 0

        # check that the program outputs files with errors
        if test_errors:
            assert mock_logger.any_call_contains('These files contain errors')
            assert NDATools.upload.validation.io.exit_error.call_count == 1
            assert not mock_logger.any_call_contains('The following files passed validation')
        else:
            assert mock_logger.any_call_contains('The following files passed validation')
            assert not mock_logger.any_call_contains('These files contain errors')

        # run an additional test that warnings file is created when parameter is set
        if test_warnings and not test_errors:
            mock_logger.reset_mock()
            user_io.save_validation_errors([result], save_warnings=True)
            assert mock_file_writer.write_warnings.call_count == 1
            assert mock_logger.any_call_contains('Warnings output to:')

# def test_user_io_run_validation_step_io_sys_error(validation_result, monkeypatch):
#     """Test that information is printed to screen as expected when system error is encountered"""
#     user_io = ResultsWriterFactory.get_writer(file_format='json')
#     mock_file_writer = MagicMock()
#     mock_logger = MockLogger()
#     result = validation_result('file1.csv', {}, {})
#     result.validation_resource.status = 'SystemError'
#     with monkeypatch.context() as m, pytest.raises(SystemExit):
#         # mock file writer so we dont write actual files
#         m.setattr(user_io, 'file_writer', mock_file_writer)
#         # test logging calls
#         m.setattr(NDATools.upload.validation.io.logger, 'info', mock_logger)
#         m.setattr(NDATools.upload.validation.io, 'exit_error', MagicMock(side_effect=[SystemExit]))
#
#         user_io.save_validation_errors([result], save_warnings=False)
#
#         assert NDATools.upload.validation.io.exit_error.call_count == 1
#         assert NDATools.upload.validation.io.exit_error.assert_called_with(
#             'Unexpected error occurred while validating one or more of the csv files')
