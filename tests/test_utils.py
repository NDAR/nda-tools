import logging
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch, mock_open
from urllib.parse import quote

import pytest

from NDATools.Utils import parse_local_files, sanitize_file_path, check_read_permissions, \
    sanitize_windows_download_filename, deconstruct_s3_url, collect_directory_list

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logger = logging.getLogger(__name__)


class TestUtils(TestCase):

    def test_sanitize_file_path(self):
        windows_path = r'C:\\Users\\test\\path\\file.csv'
        linux_path = '/home/test/path/file.csv'
        self.assertEqual(sanitize_file_path(windows_path), 'Users/test/path/file.csv')
        self.assertEqual(sanitize_file_path(linux_path), 'home/test/path/file.csv')

    def test_sanitize_windows_download_filename(self):
        path_containing_prohibited_windows_char = r'C:\\Users\\test\\path><\\package|15?*15\\file:12:22.csv'
        self.assertEqual(r'C%3A\\Users\\test\\path%3E%3C\\package%7C15%3F%2A15\\file%3A12%3A22.csv',
                         sanitize_windows_download_filename(path_containing_prohibited_windows_char))

    @patch('os.path.getsize')
    @patch('os.path.isfile')
    @patch('NDATools.Utils.check_read_permissions')
    def test_parse_local_files(self, mock_file_size, mock_isfile, mock_check_read_permissions):
        mock_file_size.return_value = 1
        mock_isfile.return_value = True
        mock_check_read_permissions.return_value = True

        # Test relative paths
        windows_relative_path = 'windows_file.csv'
        linux_relative_path = 'linux_file.csv'
        mac_relative_path = 'mac_file.csv'

        file_list = [windows_relative_path, linux_relative_path, mac_relative_path]

        windows_directory = r'C:\Users\test\path'
        linux_directory = '/home/user/path'
        mac_directory = '/Users/user/test'

        directory_list = [windows_directory, linux_directory, mac_directory]

        file_size_full_path_dict = {}
        invalid_file_array = []

        parse_local_files(directory_list, file_list, file_size_full_path_dict, invalid_file_array, True)

        logger.debug(file_list)
        logger.debug(invalid_file_array)
        logger.debug(file_size_full_path_dict)

        self.assertEqual(len(invalid_file_array), 0)
        self.assertEqual(len(file_list), 0)
        self.assertEqual(len(file_size_full_path_dict), 3)

        # Test full paths
        windows_full_path = r'C:\Users\test\path\windows_file_full.csv'
        linux_full_path = '/home/user/path/linux_file_full.csv'
        mac_full_path = '/Users/user/test/mac_file_full.csv'

        file_list = [windows_full_path, linux_full_path, mac_full_path]

        parse_local_files(directory_list, file_list, file_size_full_path_dict, invalid_file_array, False)

        logger.debug(file_list)
        logger.debug(invalid_file_array)
        logger.debug(file_size_full_path_dict)

        self.assertEqual(len(invalid_file_array), 0)
        self.assertEqual(len(file_list), 0)
        self.assertEqual(len(file_size_full_path_dict), 6)

    @patch('builtins.open', mock_open(read_data=''))
    def test_check_read_permissions(self):
        test_file = os.path.join(os.path.expanduser('~'), 'NDATools\\clientscripts\\config\\settings.cfg')
        self.assertTrue(check_read_permissions(test_file))

    @patch('builtins.open')
    def test_check_read_permissions_denied(self, mock_file):
        mock_file.side_effect = IOError()
        test_file = os.path.join(os.path.expanduser('~'), 'NDATools\\clientscripts\\config\\settings.cfg')
        self.assertFalse(check_read_permissions(test_file))

    @patch('os.path.isdir', side_effect=[True, True])
    @patch('builtins.input', return_value=f"{Path.cwd()}\\data\\api_responses,{Path.cwd()}\\data\\validation")
    def test_collect_directory_list_has_multiple_dir(self, mock_input, mock_isdir):
        directories = collect_directory_list()
        self.assertEquals(2, len(directories))
        self.assertTrue(Path(f"{Path.cwd()}\\data\\api_responses") in directories)
        self.assertTrue(Path(f"{Path.cwd()}\\data\\validation") in directories)

    @patch('os.path.isdir', return_value=True)
    @patch('builtins.input', return_value=f"{Path.cwd()}\\data\\api_responses")
    def test_collect_directory_list(self, mock_input, mock_isdir):
        directories = collect_directory_list()
        self.assertEquals(1, len(directories))
        self.assertTrue(Path(f"{Path.cwd()}\\data\\api_responses") in directories)

    @patch('os.path.isdir', side_effect=[False, True])
    @patch('builtins.input', side_effect=["invalid/path", f"{Path.cwd()}\\data\\api_responses"])
    def test_collect_directory_list_retry(self, mock_input, mock_isdir):
        directories = collect_directory_list()
        self.assertEquals(1, len(directories))
        self.assertTrue(Path(f"{Path.cwd()}\\data\\api_responses") in directories)




@pytest.mark.parametrize('bucket,key', [
    ('nda-central', 'collection-1860/submission-12345/dog.png'),
    ('nda-central', 'collection-1860/submission-12345/folder with space/dog.png'),
    ('nda-central', 'collection-1860/submission-12345/folder with space and special %/dog.png')
])
def test_deconstruct_s3_url(bucket, key):
    """Verify presigned urls and s3 urls are correctly deconstructed"""
    b, k = deconstruct_s3_url(f's3://{bucket}/{key}')
    assert b == bucket
    assert k == key
    url = f'https://s3.amazonaws.com/' + quote(f'{bucket}/{key}')
    b, k = deconstruct_s3_url(url)
    assert b == bucket
    assert k == key
    url = f'https://{bucket}.s3.amazonaws.com/' + quote(key)
    b, k = deconstruct_s3_url(url)
    assert b == bucket
    assert k == key
