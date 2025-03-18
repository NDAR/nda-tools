import json
import time
from unittest.mock import MagicMock

import pytest

from NDATools import Validation


@pytest.fixture
def validation(monkeypatch, validation_config_factory, load_from_file, tmp_path, shared_datadir):
    file_path = (shared_datadir / 'validation/file.csv')
    args, config = validation_config_factory([str(file_path)])
    return Validation.Validation(args.files, config=config, hide_progress=config.hideProgress, thread_num=1,
                                 allow_exit=True)


def test_validate(validation, monkeypatch, load_from_file):
    """ Test that validation.validate calls the expected methods and returns the expected result """
    # have to monkey patch methods in ValidationTask since these are instantiated inside validation class
    with monkeypatch.context() as m:
        # mock sleep calls to make tests faster
        m.setattr(time, 'sleep', lambda x: None)
        mock_create_response = json.loads(load_from_file('validation/api_response/initiate_validation.json'))
        m.setattr(Validation.Validation.ValidationTask, '_create_validation',
                  MagicMock(return_value=mock_create_response))
        # monkeypatch some of the methods that make API calls
        get_responses = ['validation/api_response/initiate_validation.json',
                         'validation/api_response/initiate_validation.json',
                         'validation/api_response/finish_validation.json']
        mock_get_response = list(map(lambda x: json.loads(load_from_file(x)), get_responses))
        m.setattr(Validation.Validation.ValidationTask, '_get_validation', MagicMock(side_effect=mock_get_response))

        # test
        validation.validate()
        result = validation.responses[0][0]

        assert validation.responses[0][1] == validation.file_list[0]
        assert result['id'] == 'test'
        assert result['status'] == 'Complete'
        assert result['expiration_date'] == '07/14/2021'
        assert result['errors'] == {}


@pytest.fixture
def validation_errors(load_from_file):
    def _validation_errors(file='validation/validation_errors1.json'):
        return json.loads(load_from_file(file))

    return _validation_errors


def test_output_validation_error_messages(validation, validation_errors):
    """ Test the return value of the  output_validation_error_messages procedure """
    validation.responses = []
    validation.responses.append((validation_errors('validation/validation_errors1.json'), 'C:\\test1.csv'))
    validation.responses.append((validation_errors('validation/validation_errors2.json'), 'C:\\test2.csv'))

    table_list = validation.output_validation_error_messages()
    assert len(table_list) == 2

    # table_list is a list of strings
    table0 = table_list[0]
    assert 'Row' in table0
    assert 'Column' in table0
    assert 'Message' in table0
    assert 'ampscz_missing_spec' in table0
    assert 'chrhealth_alleoth' in table0
    assert 'interview_date' in table0

    table1 = table_list[1]
    assert 'Row' in table1
    assert 'Column' in table1
    assert 'Message' in table1
    assert 'sex' in table1
