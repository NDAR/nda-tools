import pytest
import json

from unittest.mock import patch, MagicMock

import NDATools.clientscripts.vtcmd
from NDATools.clientscripts.vtcmd import main as validation_main


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

        if verb == 'POST':
            text = self._load_from_file('validation/api_response/initiate_validation.json')

        if verb == 'GET':
            text = next(self._get_responses)

        json_object = json.loads(text)

        return json_object, session

    @pytest.fixture(autouse=True)
    def setup(self, load_from_file):
        self._get_responses = self._provide_get_responses()
        self._load_from_file = load_from_file


    @patch('NDATools.Validation.open')
    @patch('NDATools.clientscripts.vtcmd.configure')
    @patch('NDATools.clientscripts.vtcmd.parse_args')
    @patch('NDATools.Validation.api_request')
    def test_validation_with_scope(self, requests_mock, parse_args_mock, config_mock,
                                   mock_file_open, validation_config_factory, shared_datadir, tmp_path):

        file_path = (shared_datadir / 'validation/file.csv')

        test_args = ['--scope', '123123', str(file_path)]

        args, config = validation_config_factory(test_args)
        config.JSON = True

        parse_args_mock.return_value = args
        config_mock.return_value = config

        requests_mock.side_effect = self.mocked_api_request_successful

        # we plan on opening a file 2 times -
        # the first time for writing the ds file, and the second time for reading the ds file
        mock_file = MagicMock()
        val_results_path = tmp_path / "test_val_results.txt"
        val_results_file = open( val_results_path,'w')
        mock_file_open.side_effect = [mock_file, val_results_file]


        validation_main()
        json_data = json.loads(val_results_path.read_text())
        results = json_data['Results']
        result = results[0]

        assert result['File'] == str(file_path)
        assert result['ID'] == 'test'
        assert result['Status'] == 'Complete'
        assert result['Expiration Date'] == '07/14/2021'
        assert result['Errors'] == {}
