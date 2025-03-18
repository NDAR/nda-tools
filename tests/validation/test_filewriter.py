import csv
import itertools
import json
import uuid
from unittest.mock import MagicMock

import pytest

from NDATools.upload.validation.api import ValidationResponse, ValidationV2Credentials, ValidationV2
from NDATools.upload.validation.filewriter import JsonValidationFileWriter, CsvValidationFileWriter


@pytest.fixture
def validation(shared_datadir):
    def _validation(errors, warnings):
        file1 = shared_datadir / 'file.csv'
        rw_creds = MagicMock(spec=ValidationV2Credentials)
        rw_creds.uuid = str(uuid.uuid4())
        rw_creds.download_errors = MagicMock(return_value=errors or {})
        rw_creds.download_warnings = MagicMock(return_value=warnings or {})
        resource = ValidationV2(**{
            'validation_uuid': rw_creds.uuid,
            'status': 'CompleteWithWarnings' if not errors else 'CompleteWithErrors',
            'short_name': 'image03',
            'scope': None,
            'rows': 42,
            'validation_files': dict()
        })
        return ValidationResponse(file1, rw_creds, resource)

    return _validation


@pytest.fixture
def validation_with_errors(validation):
    return validation(
        errors={'error1a': [{'columnName': 'column 1a', 'message': 'error message 1a', 'record': 'record 1a'}],
                'error1b': [{'columnName': 'column 1b', 'message': 'error message 1b', 'record': 'record 1b'}]},
        warnings=None)


@pytest.fixture
def validation_with_warnings(validation):
    return validation(errors=None, warnings={
        'warning1a': [{'columnName': 'column 1a', 'message': 'warning message 1a', 'record': 'record 1a'}],
        'warning1b': [{'columnName': 'column 1b', 'message': 'warning message 1b', 'record': 'record 1b'}]})


@pytest.mark.parametrize("testing_errors,file_writer_class", [
    (True, JsonValidationFileWriter),
    (False, JsonValidationFileWriter),
    (True, CsvValidationFileWriter),
    (False, CsvValidationFileWriter)
])
def test_json_validation_file_writer(testing_errors, file_writer_class, tmp_path, validation_with_warnings,
                                     validation_with_errors):
    """ Verify the contents of the files produced by the classes in filewriter.py """
    validation_writer = file_writer_class(tmp_path)
    validation_responses = [validation_with_warnings, validation_with_errors]

    if testing_errors:
        validation_writer.write_errors(validation_responses)
    else:
        validation_writer.write_warnings(validation_responses)
    f = validation_writer.errors_file if testing_errors else validation_writer.warnings_file
    with open(f, 'r') as file:
        if file_writer_class == JsonValidationFileWriter:
            result_list = json.load(file)['Results']
            for (response, result) in zip(validation_responses, result_list):
                assert result['File'] == response.file.name
                assert result['ID'] == response.uuid
                assert result['Status'] == response.status
                assert result['Expiration Date'] == ''
                if testing_errors:
                    assert result['Errors'] == response.rw_creds.download_errors()
                    assert 'Warnings' not in result
                else:
                    assert result['Warnings'] == response.rw_creds.download_warnings()
        else:
            csv_reader = csv.DictReader(file)
            for (validation_uuid, warnings_or_errors_it) in itertools.groupby([row for row in csv_reader],
                                                                              lambda row: row['ID']):
                response = list(filter(lambda r: r.uuid == validation_uuid, validation_responses))[0]

                key = 'ERRORS' if testing_errors else 'WARNINGS'
                # group csv_errors by error/warning code
                for (error_code, errors_by_code_it) in itertools.groupby(list(warnings_or_errors_it),
                                                                         lambda row: row[key]):

                    errors_by_code = list(errors_by_code_it)
                    assert all(map(lambda e: e['FILE'] == response.file.name, errors_by_code))
                    assert all(map(lambda e: e['ID'] == response.uuid, errors_by_code))
                    assert all(map(lambda e: e['STATUS'] == response.status, errors_by_code))
                    assert all(map(lambda e: e['EXPIRATION_DATE'] == '', errors_by_code))
                    assert all(
                        map(lambda e: e[key] == error_code, errors_by_code))
                    for error_by_row in errors_by_code:
                        if 'RECORD' in errors_by_code:
                            if error_code == 'None':
                                assert error_by_row['COLUMN'] == 'None'
                                assert error_by_row['MESSAGE'] == 'None'
                                assert error_by_row['RECORD'] == 'None'
                            else:
                                err_matches = response.rw_creds.download_errors()[error_code]
                                error_match = \
                                    list(filter(lambda e: e['record'] == error_by_row['RECORD'], err_matches))[0]
                                assert error_by_row['COLUMN'] == error_match['column']
                                assert error_by_row['MESSAGE'] == error_match['message']
                                assert error_by_row['RECORD'] == error_match['record']
                        else:
                            # general error, not record level error
                            pass
                    if not testing_errors:
                        # Each code should produce only one record in the csv
                        assert len(errors_by_code) == 1
                        error_by_row = errors_by_code[0]
                        if error_code == 'None':
                            assert int(error_by_row['COUNT']) == 0
                        else:
                            err_matches = response.rw_creds.download_warnings()[error_code]
                            assert int(error_by_row['COUNT']) == len(list(err_matches))
                            assert error_by_row['MESSAGE'] == list(err_matches)[0]['message']
