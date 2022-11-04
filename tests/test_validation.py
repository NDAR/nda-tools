import json
from unittest.mock import MagicMock

import pytest

from NDATools import Validation


class TestValidation:

    def _provide_get_responses(self):
        for x in ['validation/api_response/initiate_validation.json',
                  'validation/api_response/initiate_validation.json',
                  'validation/api_response/initiate_validation.json',
                  'validation/api_response/initiate_validation.json',
                  'validation/api_response/initiate_validation.json',
                  'validation/api_response/finish_validation.json',
                  'validation/api_response/finish_validation.json']:
            yield self._load_from_file(x)

    def mocked_api_request_successful(self, *args, **kwargs):
        session = MagicMock()
        verb = args[1]
        endpoint = args[2]
        text = None

        if verb == 'GET':
            text = next(self._get_responses)

        json_object = json.loads(text)

        return json_object


    @pytest.fixture(autouse=True)
    def setup(self, load_from_file):
        self._get_responses = self._provide_get_responses()
        self._load_from_file = load_from_file

    def test_validation_with_scope(self, monkeypatch,
                                   validation_config_factory,
                                   shared_datadir,
                                   tmp_path):

        file_path = (shared_datadir / 'validation/file.csv')

        test_args = ['--scope', '123123', str(file_path)]

        args, config = validation_config_factory(test_args)
        config.JSON = True

        def mocked_post_request(*args, **kwargs):
            return json.loads(self._load_from_file('validation/api_response/initiate_validation.json'))

        def mocked_get_request(*args, **kwargs):
            return json.loads(next(self._get_responses))

        with monkeypatch.context() as m:
            m.setattr(Validation, 'post_request', mocked_post_request)
            m.setattr(Validation, 'get_request', mocked_get_request)
            validation = Validation.Validation(args.files, config=config, hide_progress=config.hideProgress, thread_num=1,
                                    allow_exit=True)
            validation.validate()
            result = validation.responses[0][0]

            assert validation.responses[0][1] == str(file_path)
            assert result['id'] == 'test'
            assert result['status'] == 'Complete'
            assert result['expiration_date'] == '07/14/2021'
            assert result['errors'] == {}
