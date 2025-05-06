import json
import time
from unittest.mock import MagicMock

import pytest

from NDATools.upload.validation.v1 import Validation


@pytest.fixture
def validation(monkeypatch, validation_config_factory, load_from_file, tmp_path, shared_datadir):
    file_path = (shared_datadir / 'validation/file.csv')
    args, config = validation_config_factory([str(file_path)])
    return Validation(args.files, config=config, hide_progress=config.hide_progress, thread_num=1,
                      allow_exit=True)


def test_validate(validation, monkeypatch, load_from_file):
    """ Test that validation.validate calls the expected methods and returns the expected result """
    # have to monkey patch methods in ValidationTask since these are instantiated inside validation class
    with monkeypatch.context() as m:
        # mock sleep calls to make tests faster
        m.setattr(time, 'sleep', lambda x: None)
        mock_create_response = json.loads(load_from_file('validation/api_response/initiate_validation.json'))
        m.setattr(Validation.ValidationTask, '_create_validation',
                  MagicMock(return_value=mock_create_response))
        # monkeypatch some of the methods that make API calls
        get_responses = ['validation/api_response/initiate_validation.json',
                         'validation/api_response/initiate_validation.json',
                         'validation/api_response/finish_validation.json']
        mock_get_response = list(map(lambda x: json.loads(load_from_file(x)), get_responses))
        m.setattr(Validation.ValidationTask, '_get_validation', MagicMock(side_effect=mock_get_response))

        # test
        validation.validate()
        result = validation.responses[0][0]

        assert validation.responses[0][1] == validation.file_list[0]
        assert result['id'] == 'test'
        assert result['status'] == 'Complete'
        assert result['expiration_date'] == '07/14/2021'
        assert result['errors'] == {}
