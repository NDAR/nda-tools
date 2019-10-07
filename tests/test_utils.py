import logging
import os

from NDATools.Utils import parse_local_files, sanitize_file_path, check_read_permissions
from unittest import TestCase
try:
    import mock
except ImportError:
    from mock import patch


class TestUtils(TestCase):
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

    def test_sanitize_file_path(self):
        windows_path = r'C:\\Users\\test\\path\\file.csv'
        linux_path = '/home/test/path/file.csv'
        self.assertEqual(sanitize_file_path(windows_path), 'Users/test/path/file.csv')
        self.assertEqual(sanitize_file_path(linux_path), 'home/test/path/file.csv')

    def test_parse_local_files(self):
        # full path, multi, single, relative, windows, mac, linux
        windows_full_path = r'C:\\Users\\test\path\\file.csv'
        windows_relative_path = 'file.csv'
        linux_full_path = '/home/user/path/file.csv'
        linux_relative_path = 'file.csv'
        mac_full_path = '/Users/user/test/file.csv'
        mac_relative_path = 'file.csv'

        file_list = [windows_relative_path, linux_relative_path, mac_relative_path]

        windows_directory = 'C:\\Users\\test\\path'
        linux_directory = '/home/user/test'
        mac_directory = '/Users/user/test'

        directory_list = [windows_directory, linux_directory, mac_directory]

        file_size_full_path_dict = {}
        invalid_file_array = []

        self._test_parse_local_files_copy(directory_list, file_list, file_size_full_path_dict, invalid_file_array, True)
        logging.debug('test')
        logging.debug(file_list)
        logging.debug(invalid_file_array)
        logging.debug(file_size_full_path_dict)

    @mock.patch('builtins.open', mock.mock_open(read_data='foo\nbar\nbaz\n'))
    def test_check_read_permissions(self):
        test_file = os.path.join(os.path.expanduser('~'), 'NDATools\\clientscripts\\config\\settings.cfg')
        self.assertTrue(check_read_permissions(test_file))

    def test_check_read_permissions_denied(self):
        with mock.patch('builtins.open') as mock_file:
            mock_file.side_effect = OSError()
            test_file = os.path.join(os.path.expanduser('~'), 'NDATools\\clientscripts\\config\\settings.cfg')
            self.assertFalse(check_read_permissions(test_file))

    @staticmethod
    def _test_parse_local_files_copy(directory_list, no_match, full_file_path, no_read_access, skip_local_file_check):
        logging.debug(directory_list)

        for file in no_match[:]:
            file_key = sanitize_file_path(file)
            for d in directory_list:
                if skip_local_file_check:
                    file_name = os.path.join(d, file)
                    try:
                        # full_file_path[file_key] = (file_name, os.path.getsize(file_name))
                        full_file_path[file_key] = (file_name, 1)  # arbitrary number for file size
                        no_match.remove(file)
                    except (OSError, IOError) as err:
                        if err.errno == 13:
                            print('Permission Denied: {}'.format(file_name))
                        continue
                    break
                else:
                    if os.path.isfile(file):
                        file_name = file
                    elif os.path.isfile(os.path.join(d, file)):
                        file_name = os.path.join(d, file)
                    else:
                        continue
                    if not check_read_permissions(file_name):
                        no_read_access.add(file_name)
                    full_file_path[file_key] = (file_name, os.path.getsize(file_name))
                    no_match.remove(file)
                    break
