import json
import os
import shlex
import shutil
from unittest.mock import MagicMock

import boto3
import pytest
from requests import HTTPError
from requests.structures import CaseInsensitiveDict

import NDATools
from NDATools.Download import Download, DownloadRequest
from tests.conftest import MockLogger


def get_presigned_urls_mock(*args, **kwargs):
    return {f: 's3://fake-presigned-url' for f in args[0]}


@pytest.fixture
def download_mock(load_from_file, download_config_factory, monkeypatch, tmp_path, shared_datadir, logger_mock):
    def mock_get_package_info(*args, **kwargs):
        return json.loads(load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

    def fail_fast(*args, **kwargs):
        raise Exception('Should not download in tests')

    def _download_mock(*, args):
        # return json.loads(load_from_file('api_responses/s3/ds_test/get-presigned-url-response.json'))
        config, args = download_config_factory(args)
        # override the NDATools.NDA_TOOLS_DOWNLOADS_FOLDER before calling constructor so downloads go to tmpdir
        monkeypatch.setattr(NDATools, 'NDA_TOOLS_DOWNLOADS_FOLDER', tmp_path)
        download = Download(args, config)
        # monkeypatch some of the methods that make API calls in Download class
        monkeypatch.setattr(download, 'get_package_info', mock_get_package_info)
        monkeypatch.setattr(download, 'get_presigned_urls', get_presigned_urls_mock)
        # make these mocks so that we can run verifications on how they were called, if needed
        monkeypatch.setattr(download, 'download_local', MagicMock())
        monkeypatch.setattr(download, 'download_to_s3', MagicMock())
        # fail fast if there is an attempt to download the metadata file with these methods
        monkeypatch.setattr(download, 'get_package_file_metadata_creds', fail_fast)
        monkeypatch.setattr(download, 'generate_metadata_and_get_creds', fail_fast)

        # update metadata_file_path to use the corresponding file in the tmpdir.
        monkeypatch.setattr(download, 'metadata_file_path',
                            shared_datadir / 'package_metadata' / f'package_file_metadata_{download.package_id}.txt')
        # patch logger so we can run verifications on how they were called, if needed
        monkeypatch.setattr(NDATools.Download, 'logger', logger_mock)

        # patch the exit call so that we dont actually exit the program
        def fake_exit(*args, **kwargs):
            raise SystemExit()

        monkeypatch.setattr(NDATools, '_exit_client', fake_exit)
        return download

    return _download_mock


def test_download_by_links_file(download_mock, logger_mock, tmp_path):
    # create a links.txt file in tmp_path
    links_file = tmp_path / 'links.txt'
    with open(links_file, 'w') as f:
        f.write('s3://gpop-stage/ndar_data/QueryPackages/REVENGERS/README.pdf\n')
        f.write(
            's3://gpop-stage/ndar_data/QueryPackages/REVENGERS/773981645558:ndar_administrator/Package_1189934/dataset_collection.txt\n')
    ds_download = download_mock(args=['-dp', '1189934', '-t', str(links_file)])
    ds_download.start()
    logger_mock.info.assert_any_call_contains('Number of files in package: 7')
    logger_mock.info.assert_any_call_contains('Total errors encountered: 0')
    # 2 links in the s3links file
    logger_mock.info.assert_any_call_contains('Total download requests: 2')
    logger_mock.info.assert_any_call_contains('Exiting Program...')
    logger_mock.error.assert_not_called()
    assert ds_download.download_local.call_count == 2


def test_download_structure_not_found(download_mock, logger_mock):
    """ User inputs a datastructure that isnt found in the package. Should alert user and exit """
    ds_download = download_mock(args=['-dp', '1189934', '-ds', 'fmriresults01'])
    with pytest.raises(SystemExit):
        ds_download.start()
    logger_mock.info.assert_any_call_contains('fmriresults01 data structure is not included in the package 1189934')
    ds_download.download_local.assert_not_called()


# see package_file_metadata_1189934.txt for the list of files in this test fixture
@pytest.mark.parametrize("download_args,expected_file_count", [
    ("-dp 1189934", 7),
    ("-dp 1189934 -ds image03", 1),
    ("-dp 1189934 --file-regex .*.txt", 5),
    ("-dp 1189934 --file-regex .*.png -ds image03", 1),
    ("-dp 1189934 s3://gpop-stage/ndar_data/QueryPackages/REVENGERS/README.pdf", 1),
])
def test_download(download_args, expected_file_count, logger_mock, download_mock):
    """ Test various inputs to downloadcmd and verify that the output matches what is expected. """
    ds_download = download_mock(args=shlex.split(download_args))
    ds_download.start()
    logger_mock.info.assert_any_call_contains(f'Number of files in package: 7')
    logger_mock.info.assert_any_call_contains('Total errors encountered: 0')
    # 5 files that have .txt in the path
    logger_mock.info.assert_any_call_contains(f'Total download requests: {expected_file_count}')
    logger_mock.info.assert_any_call_contains('Exiting Program...')
    logger_mock.error.assert_not_called()
    assert ds_download.download_local.call_count == expected_file_count


def test_invalid_regex(download_mock, logger_mock):
    """ User inputs a regex that is invalid. Should alert user and exit"""
    ds_download = download_mock(args=['-dp', '1189934', '--file-regex', '.*.asdfasdf'])
    ds_download.start()
    logger_mock.info.assert_any_call_contains('No file was found that matched the regex pattern')
    logger_mock.info.assert_any_call_contains('Exiting Program...')
    ds_download.download_local.assert_not_called()


def test_download_with_all_completed_files(download_mock, logger_mock, tmp_path, shared_datadir):
    """ Test that the program uses the files in .download-progress to check for already completed files"""
    ds_download = download_mock(args=['-dp', '1189934'])
    job_uuid = '346886af-9c50-4aaf-ace3-db22c3b3a4c9'  # needs to match the uuid in the manifest

    manifest = shared_datadir / 'download-progress' / 'download-job-manifest.csv'
    report = shared_datadir / 'download-progress' / 'download-progress-report.csv'
    # copy the above files to the expected path so the program finds and uses them
    shutil.copy(manifest,
                os.path.join(ds_download.download_directory, '.download-progress', 'download-job-manifest.csv'))
    os.mkdir(os.path.join(ds_download.download_directory, '.download-progress', job_uuid))
    shutil.copy(report,
                os.path.join(ds_download.download_directory, '.download-progress', job_uuid,
                             'download-progress-report.csv'))
    ds_download.download_job_uuid = job_uuid
    ds_download.start()
    # program should exit early because all files have already been downloaded
    logger_mock.info.assert_any_call_contains('All files have been downloaded')
    ds_download.download_local.assert_not_called()
    assert not logger_mock.info.any_call_contains('Beginning download')


def test_download_with_some_completed_files(download_mock, logger_mock, tmp_path, shared_datadir):
    """ Test that the program uses the files in .download-progress to check for already completed files"""
    ds_download = download_mock(args=['-dp', '1189934'])
    job_uuid = '346886af-9c50-4aaf-ace3-db22c3b3a4c9'  # needs to match the uuid in the manifest

    manifest = shared_datadir / 'download-progress' / 'download-job-manifest.csv'
    report = shared_datadir / 'download-progress' / 'download-progress-report-incomplete.csv'
    # copy the above files to the expected path so the program finds and uses them
    shutil.copy(manifest,
                os.path.join(ds_download.download_directory, '.download-progress', 'download-job-manifest.csv'))
    os.mkdir(os.path.join(ds_download.download_directory, '.download-progress', job_uuid))
    shutil.copy(report,
                os.path.join(ds_download.download_directory, '.download-progress', job_uuid,
                             'download-progress-report.csv'))
    ds_download.download_job_uuid = job_uuid
    ds_download.start()
    # some files are downloaded and should be skipped
    logger_mock.info.assert_any_call_contains('Skipping 4 files which have already been downloaded')
    logger_mock.info.assert_any_call_contains('Beginning download of the remaining 2 files')


@pytest.fixture
def download_mock2(load_from_file, download_config_factory, monkeypatch, tmp_path, logger_mock):
    """mock for testing download_local and download_to_s3 methods"""

    def _download_mock(*, args):
        # return json.loads(load_from_file('api_responses/s3/ds_test/get-presigned-url-response.json'))
        args, config = download_config_factory(args)
        return Download(config, args)

    return _download_mock


@pytest.fixture
def package_file():
    return {
        'package_file_id': 12345678,
        'download_alias': 'image03/testing.txt',
        'file_size': 123
    }


@pytest.fixture
def download_request(tmp_path, package_file):
    presigned_url = 'https://s3.amazonaws.com/nda-central/collection-1860/submission-12345/testing.txt?signature=123123'
    return DownloadRequest(package_file, presigned_url, 123456789, tmp_path)


class Response:
    def __init__(self, status_code=200, text='{}', elapsed=2000, headers=None):
        if headers is None:
            headers = CaseInsensitiveDict({'Content-Type': 'application/json'})
        self.status_code = status_code
        self.text = text
        self.elapsed = elapsed
        self.headers = headers

    @property
    def ok(self):
        return self.status_code == 200

    def json(self):
        return json.loads(self.text)

    def iter_content(self, chunk_size: int):
        # split up self.text into chunks of chunk_size
        for i in range(0, len(self.text), chunk_size):
            yield self.text[i:i + chunk_size].encode('utf-8')

    def raise_for_status(self):
        pass


def test_download_local(monkeypatch, download_mock2, download_request):
    download = download_mock2(args=['-dp', '1189934'])
    mock_session = MagicMock()
    mock_response_context = MagicMock()
    mock_session.return_value.__enter__.return_value.get.return_value = mock_response_context
    mock_response_context.__enter__.return_value = Response()
    with monkeypatch.context() as m:
        m.setattr('requests.session', mock_session)
        m.setattr(os, 'rename', MagicMock())
        download.download_local(download_request)
        assert download_request.actual_file_size == 2
        assert os.rename.called_with(download_request.partial_download_abs_path,
                                     download_request.completed_download_abs_path)
        assert download_request.nda_s3_url == 's3://nda-central/collection-1860/submission-12345/testing.txt'

    # test that a download continues where it left off when a file is alreay present on disk
    # mock a response that returns the last byte of the file
    mock_response_context.__enter__.return_value = Response(text='}')
    with monkeypatch.context() as m:
        m.setattr('requests.session', mock_session)
        m.setattr(os, 'rename', MagicMock())
        m.setattr(os.path, 'isfile', MagicMock(side_effect=[False, True]))
        m.setattr(os.path, 'getsize', MagicMock(return_value=1))
        download.download_local(download_request)
        assert download_request.actual_file_size == 2
        assert mock_session.headers.update.called_once_with({'Range': 'bytes=1-'})

    # test that a download is skipped when the file is already downloaded
    with monkeypatch.context() as m:
        m.setattr('requests.session', mock_session)
        m.setattr(os, 'rename', MagicMock())
        m.setattr(os.path, 'isfile', MagicMock(return_value=True))
        m.setattr(os.path, 'getsize', MagicMock(return_value=2))
        download.download_local(download_request)
        assert download_request.actual_file_size == 2
        assert download_request.exists is True
        assert not os.rename.called


def test_download_to_s3(monkeypatch, download_mock2, download_request):
    download = download_mock2(args=['-dp', '1189934'])
    with monkeypatch.context() as m:
        creds = {
            'access_key': 'XXX',
            'secret_key': '123',
            'session_token': '123',
            'source_uri': 's3://nda-central/collection-1860/submission-12345/testing.txt',
            'destination_uri': 's3://personal-bucket/prefix/image03/testing.txt'
        }
        m.setattr(download, 'get_temp_creds_for_file', MagicMock(return_value=creds))
        s3_session = MagicMock()
        s3_client = MagicMock()
        s3_resource = MagicMock()
        head_object_response = {
            'ContentLength': '999999999999',
            'ETag': '123123123',
        }

        m.setattr(boto3.session, 'Session', MagicMock(return_value=s3_session))
        m.setattr(s3_session, 'client', MagicMock(return_value=s3_client))
        m.setattr(s3_session, 'resource', MagicMock(return_value=s3_resource))
        m.setattr(s3_client, 'head_object', MagicMock(return_value=head_object_response))
        download.download_to_s3(download_request)
        assert s3_resource.meta.client.copy.called_once_with({
            'Bucket': 'nda-central',
            'Key': 'collection-1860/submission-12345/testing.txt',
        }, 'personal-bucket', 'prefix/image03/testing.txt')
        assert 'Config' in s3_resource.meta.client.copy.call_args_list[0].kwargs
        assert 'Callback' in s3_resource.meta.client.copy.call_args_list[0].kwargs


# line 552
def test_download_handle_credentials_expired(monkeypatch, download_mock2, download_request, package_file):
    download = download_mock2(args=['-dp', '1189934'])

    with monkeypatch.context() as m:
        expired_error = HTTPError(response=Response(status_code=403, text='Request has expired'))
        m.setattr(download, 'download_local', MagicMock(side_effect=[expired_error, None]))
        m.setattr(download, 'get_temp_creds_for_file', MagicMock())
        download_request = download.download_from_s3link(package_file, 'https://asdfasdf/asdfasdf')
        assert download_request is not None
        assert download_request.exists
        assert download_request.download_complete_time is not None
        assert download.get_temp_creds_for_file.call_count == 1


def test_handle_download_exception(monkeypatch, download_mock2, download_request, tmp_path):
    download = download_mock2(args=['-dp', '1189934'])
    failed_s3_links_file = tmp_path / 'failed-files.txt'
    # test handling a generic exception
    with monkeypatch.context() as m:
        m.setattr(download, 'write_to_failed_download_link_file', MagicMock())
        m.setattr(NDATools.Download.logger, 'error', MockLogger())
        download.handle_download_exception(download_request, Exception('test'), failed_s3_links_file)
        assert download.write_to_failed_download_link_file.called_once_with(failed_s3_links_file)

    # test extra logging for 404 error
    not_found_error = HTTPError(response=Response(status_code=404))
    with monkeypatch.context() as m:
        m.setattr(download, 'write_to_failed_download_link_file', MagicMock())
        m.setattr(NDATools.Download.logger, 'error', MockLogger())
        download.handle_download_exception(download_request, not_found_error, failed_s3_links_file)
        assert download.write_to_failed_download_link_file.called_once_with(failed_s3_links_file)
        assert NDATools.Download.logger.error.any_call_contains('This path is incorrect')

    # test extra logging for 403 error
    forbidden_error = HTTPError(response=Response(status_code=403))
    with monkeypatch.context() as m:
        m.setattr(download, 'write_to_failed_download_link_file', MagicMock())
        m.setattr(NDATools.Download.logger, 'error', MockLogger())
        download.handle_download_exception(download_request, forbidden_error, failed_s3_links_file)
        assert download.write_to_failed_download_link_file.called_once_with(failed_s3_links_file)
        assert NDATools.Download.logger.error.any_call_contains('This is a private bucket')

    # test extra logging for s3-to-s3 transfer errors
    with monkeypatch.context() as m:
        m.setattr(download, 'write_to_failed_download_link_file', MagicMock())
        m.setattr(NDATools.Download.logger, 'error', MockLogger())
        download.handle_download_exception(download_request, Exception('operation: Access Denied'),
                                           failed_s3_links_file)
        assert download.write_to_failed_download_link_file.called_once_with(failed_s3_links_file)
        assert NDATools.Download.logger.error.any_call_contains(
            'This error is likely caused by a misconfiguration on the target s3 bucket')


def test_verify(monkeypatch, download_mock2, tmp_path, datadir):
    download = download_mock2(args=['-dp', '1228592', '--verify', '-s3', 's3://personalbucket/abc'])
    # test that error is raised when user attempts to verify download with -s3 arg specified
    with monkeypatch.context() as m:
        m.setattr(download, 'get_and_display_package_info', MagicMock())
        m.setattr(download, 'download_package_metadata_file', MagicMock())
        m.setattr(NDATools.Download, 'exit_error', MagicMock(side_effect=Exception()))
        with pytest.raises(Exception):
            download.verify_download()
            assert NDATools.Download.exit_error.call_count == 1

    download_dir = datadir / 'download_dir'
    downloadcmd_downloads_dir = datadir / 'packages'
    with monkeypatch.context() as m:
        m.setattr(NDATools, 'NDA_TOOLS_DOWNLOADS_FOLDER', str(downloadcmd_downloads_dir))
        download = download_mock2(args=['-dp', '1228592', '--verify', '-d', str(download_dir)])
        m.setattr(download, 'get_and_display_package_info', MagicMock())
        m.setattr(download, 'download_package_metadata_file', MagicMock())
        m.setattr(NDATools.Download.logger, 'info', MockLogger())
        m.setattr(download, 'download_job_uuid', '196d36c8-336b-406e-8051-1f0afe413bc7')
        download.verify_download()
        assert os.path.exists(downloadcmd_downloads_dir / '1228592' / 'download-verification-report.csv')
        assert NDATools.Download.logger.info.any_call_contains('9 files are expected to have been downloaded')
        assert NDATools.Download.logger.info.any_call_contains('Found 4 complete file downloads according to log file')
        assert os.path.exists(downloadcmd_downloads_dir / '1228592' / 'download-verification-retry-s3-links.csv')
        with open(downloadcmd_downloads_dir / '1228592' / 'download-verification-retry-s3-links.csv') as f:
            assert f.read() == 's3://nda-central/collection-1860/image4.png\n'
