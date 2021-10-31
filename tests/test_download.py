import json
import os
from unittest.mock import patch, MagicMock

import pytest
from requests import HTTPError

import NDATools.clientscripts.downloadcmd
import NDATools.utils.Utils

FAKE_FILE_BYTES = [0x10, 0x10]
MISSING_FILE = 's3://NDAR_Central_1/submission_43568/4.png'


class TestDownload:

    def mock_post_api_response(self, *args, **kwargs):
        kwargs['verb'] = 'POST'
        return self.mock_api_response(*args, **kwargs)

    def mock_get_api_response(self, *args, **kwargs):
        kwargs['verb'] = 'GET'
        return self.mock_api_response(*args, **kwargs)

    def _get_fake_s3_response_file_not_found(self):
        for i in ['api_responses/s3/ds_test/fmriresults01_with_file_not_found.txt',
                  'api_responses/s3/ds_test/1.png',
                  'api_responses/s3/ds_test/2.png',
                  'api_responses/s3/ds_test/3.png']:
            yield self._load_from_file(i)

    def _get_fake_s3_response(self):
        for i in ['api_responses/s3/ds_test/fmriresults01.txt',
                  'api_responses/s3/ds_test/1.png',
                  'api_responses/s3/ds_test/2.png',
                  'api_responses/s3/ds_test/3.png']:
            yield self._load_from_file(i)

    def mock_api_response(self, *args, **kwargs):
        endpoint = args[0] if 'endpoint' not in kwargs else kwargs['kwargs']
        verb = kwargs['verb']
        payload = None
        if verb == 'POST':
            payload = kwargs['json']
        response_text = None
        response = MagicMock()

        if '/api/datadictionary' in endpoint:
            response_text = self._load_from_file('api_responses/datadictionary/ds_test/get_datastructure_response.json')
            response.json.return_value = json.loads(response_text)
        elif '/api/package' in endpoint:
            if 'files/batchGeneratePresignedUrls' in endpoint:
                if not self.should_return_done_resource:
                    self.should_return_done_resource = True
                    response_text = self._load_from_file('api_responses/package/ds_test/get_ds_presigned_url.json')
                else:
                    response_text = self._load_from_file('api_responses/package/ds_test/get_afiles_presigned_urls.json')
            elif verb == 'GET' and '/files?page=1&size=all' in endpoint:
                response_text = self._load_from_file('api_responses/package/ds_test/all_files.json')
            elif verb == 'POST' and '/files' in endpoint:
                if self.request.node.name == 'test_ds_argument_file_not_found' and verb == 'POST' and MISSING_FILE in payload:
                    response_text = '''The following files are invalid\r\n{}'''.format(MISSING_FILE)
                    response.text = response_text
                    response.status_code = 404
                    raise HTTPError(response=response)
                else:
                    response_text = self._load_from_file('api_responses/package/ds_test/get_files_by_s3.json')
                    response.json.return_value = json.loads(response_text)
        else:
            if self.request.node.name == 'test_ds_argument_file_not_found':
                response_text = self._get_fake_s3_response_file_not_found()
            else:
                response_text = self._get_fake_s3_response()
            response.__enter__.return_value.iter_content.return_value.__iter__.return_value = FAKE_FILE_BYTES
            self.s3_file_download_count += 1

        response.text = response_text
        return response

    @pytest.fixture(autouse=True)
    def class_setup(self, load_from_file, request):
        self.should_return_done_resource = False
        self._load_from_file = load_from_file
        self.s3_file_download_count = 0
        self.request = request

    @patch('NDATools.Download.open')
    @patch('NDATools.Download.os')
    @patch('NDATools.clientscripts.downloadcmd.configure')
    @patch('NDATools.clientscripts.downloadcmd.parse_args')
    @patch('NDATools.Download.requests')
    def test_ds_argument_file_not_found(self,
                                        requests_mock,
                                        parse_args_mock,
                                        configure_mock,
                                        os_mock,
                                        mock_file_open,
                                        download_config_factory,
                                        shared_datadir,
                                        capsys):
        test_package_id = '1189934'
        test_data_structure_file = 'fmriresults01'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file]

        download_config_data_structure = download_config_factory(test_args)

        requests_mock.post.side_effect = self.mock_post_api_response
        requests_mock.get.side_effect = self.mock_get_api_response

        session_mock = MagicMock()
        requests_mock.session.return_value = session_mock
        session_mock.get = self.mock_get_api_response

        os_mock.path.isfile.return_value = False
        os_mock.path.sep = os.path.sep

        parse_args_mock.return_value = download_config_data_structure[0]
        configure_mock.return_value = download_config_data_structure[1]

        mock_ds_file = MagicMock()
        mock_a_file1 = MagicMock()
        mock_a_file2 = MagicMock()
        mock_a_file3 = MagicMock()
        # we plan on opening a file 2 times -
        # the first time for writing the ds file, and the second time for reading the ds file
        mock_file_open.side_effect = [mock_ds_file,
                                      open(os.path.join(shared_datadir, 'api_responses/s3/ds_test/fmriresults01_with_file_not_found.txt')),
                                      mock_a_file1,
                                      mock_a_file2,
                                      mock_a_file3]

        NDATools.clientscripts.downloadcmd.main()

        captured = capsys.readouterr()
        assert 'WARNING: The following associated files were not found' in captured.out
        assert 'Beginning download of 3 files' in captured.out

    @patch('NDATools.clientscripts.downloadcmd.configure')
    @patch('NDATools.clientscripts.downloadcmd.parse_args')
    @patch('NDATools.Download.requests')
    def test_ds_argument_structure_not_found(self, requests_mock,
                                             parse_args_mock,
                                             configure_mock,
                                             download_config_factory,
                                             capsys):

        test_package_id = '1189934'
        test_data_structure_file = 'image03'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file]

        download_config_data_structure = download_config_factory(test_args)
        parse_args_mock.return_value = download_config_data_structure[0]
        configure_mock.return_value = download_config_data_structure[1]

        requests_mock.post.side_effect = self.mock_post_api_response
        requests_mock.get.side_effect = self.mock_get_api_response

        session_mock = MagicMock()
        requests_mock.session.return_value = session_mock
        session_mock.get = self.mock_get_api_response

        with pytest.raises(SystemExit):
            NDATools.clientscripts.downloadcmd.main()

        captured = capsys.readouterr()
        # Program must print out that the structure was not found
        assert "{} data structure is not included in the package".format(test_data_structure_file) in captured.out

        # Program must print out list of structures in the package
        valid_ds_file = 'fmriresults01'
        assert "{}".format(valid_ds_file) in captured.out

    @patch('NDATools.Download.open')
    @patch('NDATools.Download.os')
    @patch('NDATools.clientscripts.downloadcmd.configure')
    @patch('NDATools.clientscripts.downloadcmd.parse_args')
    @patch('NDATools.Download.requests')
    def test_ds_argument_success(self,
                                 requests_mock,
                                 parse_args_mock,
                                 configure_mock,
                                 os_mock,
                                 mock_file_open,
                                 download_config_factory,
                                 shared_datadir):

        test_package_id = '1189934'
        test_data_structure_file = 'fmriresults01'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file]

        download_config_data_structure = download_config_factory(test_args)

        requests_mock.post.side_effect = self.mock_post_api_response
        requests_mock.get.side_effect = self.mock_get_api_response

        session_mock = MagicMock()
        requests_mock.session.return_value = session_mock
        session_mock.get = self.mock_get_api_response

        os_mock.path.isfile.return_value = False
        os_mock.path.sep = os.path.sep

        parse_args_mock.return_value = download_config_data_structure[0]
        configure_mock.return_value = download_config_data_structure[1]

        mock_ds_file = MagicMock()
        mock_a_file1 = MagicMock()
        mock_a_file2 = MagicMock()
        mock_a_file3 = MagicMock()
        # we plan on opening a file 2 times -
        # the first time for writing the ds file, and the second time for reading the ds file
        mock_file_open.side_effect = [mock_ds_file,
                                      open(os.path.join(shared_datadir, 'api_responses/s3/ds_test/fmriresults01.txt')),
                                      mock_a_file1,
                                      mock_a_file2,
                                      mock_a_file3]

        NDATools.clientscripts.downloadcmd.main()
        # write should be called for each fake byte in the file (see above mock_api_response call)
        assert mock_ds_file.write.call_count == len(FAKE_FILE_BYTES)
        assert mock_a_file1.write.call_count == len(FAKE_FILE_BYTES)
        assert mock_a_file2.write.call_count == len(FAKE_FILE_BYTES)
        assert mock_a_file3.write.call_count == len(FAKE_FILE_BYTES)
        assert self.s3_file_download_count == 3 + 1  # number of files in the DS file + 1 for the DS file itself
