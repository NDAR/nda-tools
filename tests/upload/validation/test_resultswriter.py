import csv
import enum
import itertools
import json
import uuid
from unittest.mock import MagicMock

import pytest

from NDATools.upload.cli import ValidatedFile, QaResults
from NDATools.upload.validation.api import ValidationV2Credentials, ValidationV2
from NDATools.upload.validation.results_writer import JsonWriter as JsonValidationFileWriter, \
    CsvWriter as CsvValidationFileWriter


@pytest.fixture
def qa(top_level_datadir):
    def _qa(errors):
        qa_uuid = str(uuid.uuid4())
        return QaResults(qa_uuid, errors)

    return _qa


@pytest.fixture
def qa_with_errors(qa):
    return qa({"inconsistentSex": [{"columnName": "sex",
                                    "message": "GUID NDARDF005KBU has multiple values for sex. Values for sex found: M, F.",
                                    "guid": "NDARDF005KBU"}]})


@pytest.fixture
def validation(top_level_datadir):
    def _validation(errors, warnings, manifest_errors):
        file1 = top_level_datadir / 'file.csv'
        rw_creds = MagicMock(spec=ValidationV2Credentials)
        rw_creds.uuid = str(uuid.uuid4())
        rw_creds.download_errors = MagicMock(return_value=errors or {})
        rw_creds.download_warnings = MagicMock(return_value=warnings or {})
        rw_creds.down = MagicMock(return_value=warnings or {})
        resource = ValidationV2(**{
            'validation_uuid': rw_creds.uuid,
            'status': 'CompleteWithWarnings' if not errors else 'CompleteWithErrors',
            'short_name': 'image03',
            'scope': None,
            'rows': 42,
            'validation_files': dict()
        })
        return ValidatedFile(file1, v2_creds=rw_creds, v2_resource=resource)

    return _validation


@pytest.fixture
def validation_with_errors(validation):
    return validation(
        errors={'error1a': [{'columnName': 'column 1a', 'message': 'error message 1a', 'recordNumber': 1}],
                'error1b': [{'columnName': 'column 1b', 'message': 'error message 1b', 'recordNumber': 1}],
                'unrecognizedColumnName	': [
                    {'columnName': 'column 1c', 'message': 'error message 1c', 'recordNumber': None}]
                },
        warnings=None,
        manifest_errors=None)


@pytest.fixture
def validation_with_warnings(validation):
    return validation(errors=None, warnings={
        'warning1a': [{'columnName': 'column 1a', 'message': 'warning message 1a', 'recordNumber': 1}],
        'warning1b': [{'columnName': 'column 1b', 'message': 'warning message 1b', 'recordNumber': 1}]},
                      manifest_errors=None)


class TestType(enum.Enum):
    ERRORS = 'errors'
    WARNINGS = 'warnings'
    QA = 'qa'


@pytest.mark.parametrize("test_type,file_writer_class", [
    (TestType.ERRORS, JsonValidationFileWriter),
    (TestType.WARNINGS, JsonValidationFileWriter),
    (TestType.ERRORS, CsvValidationFileWriter),
    (TestType.WARNINGS, CsvValidationFileWriter),
    (TestType.QA, JsonValidationFileWriter),
    (TestType.QA, CsvValidationFileWriter),
])
def test_json_validation_file_writer(test_type, file_writer_class, tmp_path, validation_with_warnings,
                                     validation_with_errors, qa_with_errors):
    """ Verify the contents of the files produced by the classes in filewriter.py """
    validation_writer = file_writer_class(tmp_path)
    validation_responses = [validation_with_warnings, validation_with_errors]

    if test_type == TestType.ERRORS:
        validation_writer.write_errors(validation_responses)
        f = validation_writer.errors_file
    elif test_type == TestType.WARNINGS:
        validation_writer.write_warnings(validation_responses)
        f = validation_writer.warnings_file
    else:
        validation_writer.write_qa_results(qa_with_errors)
        f = validation_writer.qa_file

    with open(f, 'r') as file:
        if file_writer_class == JsonValidationFileWriter:
            if TestType.QA == test_type:
                for (error, json_result) in zip(qa_with_errors.errors, json.load(file)['Results']):
                    assert json_result['QA ID'] == qa_with_errors.qa_uuid
                    assert json_result['ERROR CODE'] == error.err_code
                    assert json_result['MESSAGE'] == error.message
                    assert json_result['GUID/SRC-SUBJECT-ID'] == error.guid if error.guid else error.src_subject_id
            else:
                testing_errors = test_type == TestType.ERRORS
                for (response, json_result) in zip(validation_responses, json.load(file)['Results']):
                    assert json_result['File'] == response.file.name
                    assert json_result['ID'] == response.uuid
                    assert json_result['Status'] == response.status
                    assert json_result['Expiration Date'] == ''
                    if testing_errors:
                        assert json_result['Errors'] == response._v2_creds.download_errors()
                        assert 'Warnings' not in json_result
                    else:
                        assert json_result['Warnings'] == response._v2_creds.download_warnings()
        else:
            csv_reader = csv.DictReader(file)
            if test_type == TestType.QA:
                for row, error in zip(csv_reader, qa_with_errors.errors):
                    assert row['QA ID'] == qa_with_errors.qa_uuid
                    assert row['ERROR CODE'] == error.err_code
                    assert row['MESSAGE'] == error.message
                    assert row['GUID/SRC-SUBJECT-ID'] == error.guid if error.guid else error.src_subject_id
            else:
                testing_errors = test_type == TestType.ERRORS
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
                                    err_matches = response._v2_creds.download_errors()[error_code]
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
                                err_matches = response._v2_creds.download_warnings()[error_code]
                                assert int(error_by_row['COUNT']) == len(list(err_matches))
                                assert error_by_row['MESSAGE'] == list(err_matches)[0]['message']
