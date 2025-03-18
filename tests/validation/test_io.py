import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationResponse, ValidationV2
from NDATools.upload.validation.io import UserIO
from tests.conftest import MockLogger


@pytest.fixture
def validation_warnings(load_from_file):
    return json.loads(load_from_file('validation_warnings.json'))


@pytest.fixture
def validation_errors(load_from_file):
    return json.loads(load_from_file('validation_errors1.json'))


@pytest.fixture
def validation_result():
    def _validation_result(filename: str = 'file.csv', warnings=None, errors=None):
        if not warnings:
            warnings = {}
        if not errors:
            errors = {}
        validation_uuid = str(uuid.uuid4())
        mock_creds = MagicMock(spec=ValidationV2Credentials)
        status = None
        if not errors and not warnings:
            status = 'Complete'
        elif errors:
            status = 'CompleteWithErrors'
        elif warnings:
            status = 'CompleteWithWarnings'
        vr = ValidationResponse(**{'file': Path(f'/path/to/{filename}'),
                                   'creds': mock_creds,
                                   'validation_resource': ValidationV2(
                                       **{'validation_uuid': validation_uuid,
                                          'status': status,
                                          'short_name': 'fmriresults01',
                                          'rows': 1,
                                          'validation_files': {},
                                          'scope': None})})

        # set method to return mock errors and warnings
        vr.rw_creds.download_errors = MagicMock(return_value=errors)
        vr.rw_creds.download_warnings = MagicMock(return_value=warnings)
        vr.rw_creds.uuid = validation_uuid

        # monkeypatch.setattr(vr, 'uuid', validation_uuid)
        return vr

    return _validation_result


def test_preview_validation_errors(validation_result, monkeypatch, validation_errors, validation_warnings):
    """Test that information is printed to screen as expected """
    vr = validation_result('file.csv', validation_warnings, validation_errors)
    with monkeypatch.context() as m:
        # test logging calls
        mock_logger = MockLogger()
        m.setattr(NDATools.upload.validation.io.logger, 'info', mock_logger)
        NDATools.upload.validation.io.preview_validation_errors([vr], limit=10)

        # check that the error messages are printed
        mock_logger.any_call_contains(validation_errors['invalidRange'][0]['message'])
        mock_logger.any_call_contains(validation_errors['tooLong'][0]['message'])
        mock_logger.any_call_contains(validation_errors['invalidDate'][0]['message'])

        # check that the column names are printed
        mock_logger.any_call_contains('ampscz_missing_spec')
        mock_logger.any_call_contains('chrhealth_alleoth')
        mock_logger.any_call_contains('interview_date')
        mock_logger.reset_mock()

        # check that the limit parameter works as expected
        NDATools.upload.validation.io.preview_validation_errors([vr], limit=1)
        mock_logger.any_call_contains('...and 2 more errors')


@pytest.mark.parametrize('test_warnings,test_errors', [
    (False, False),
    (False, True),
    (True, False),
    (True, True),
])
def test_user_io_run_validation_step_io(test_warnings, test_errors, validation_result, monkeypatch, validation_errors,
                                        validation_warnings):
    """Test that information is printed to screen as expected """

    user_io = UserIO(is_json=True, skip_prompt=False)
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

        user_io.run_validation_step_io([result], output_warnings=False)

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
            user_io.run_validation_step_io([result], output_warnings=True)
            assert mock_file_writer.write_warnings.call_count == 1
            assert mock_logger.any_call_contains('Warnings output to:')


def test_user_io_run_validation_step_io_sys_error(validation_result, monkeypatch):
    """Test that information is printed to screen as expected when system error is encountered"""
    user_io = UserIO(is_json=True, skip_prompt=False)
    mock_file_writer = MagicMock()
    mock_logger = MockLogger()
    result = validation_result('file1.csv', {}, {})
    result.validation_resource.status = 'SystemError'
    with monkeypatch.context() as m, pytest.raises(SystemExit):
        # mock file writer so we dont write actual files
        m.setattr(user_io, 'file_writer', mock_file_writer)
        # test logging calls
        m.setattr(NDATools.upload.validation.io.logger, 'info', mock_logger)
        m.setattr(NDATools.upload.validation.io, 'exit_error', MagicMock(side_effect=[SystemExit]))

        user_io.run_validation_step_io([result], output_warnings=False)

        assert NDATools.upload.validation.io.exit_error.call_count == 1
        assert NDATools.upload.validation.io.exit_error.assert_called_with(
            'Unexpected error occurred while validating one or more of the csv files')
