import json
import os
import shlex
import shutil
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.Download import Download


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

        # path the exit call so that we dont actually exit the program
        def fake_exit(*args, **kwargs):
            raise SystemExit()

        monkeypatch.setattr(NDATools.Utils, '_exit_client', fake_exit)
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


def test_download_with_some_completed_files(download_mock, logger_mock, tmp_path, shared_datadir):
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
