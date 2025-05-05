import builtins
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.validation.api import ValidationManifest, ValidationV2Credentials, \
    ValidationV2Api, ValidationV2
from NDATools.upload.validation.manifests import ManifestsUploader, _manifests_not_found_msg, ManifestFile

validation_uuid = str(uuid.uuid4())
found_manifest = {
    'localFileName': 'manifest.json',
    's3Destination': f's3://nimhda-validation/{validation_uuid}/manifests/manifest.json',
    'recordNumber': 1,
    'header': 'manifest',
    'uuid': str(uuid.uuid4())
}
not_found_manifest = {
    'localFileName': 'not-found-manifest.json',
    's3Destination': f's3://nimhda-validation/{validation_uuid}/manifests/not-found-manifest.json',
    'recordNumber': 1,
    'header': 'manifest',
    'uuid': str(uuid.uuid4())
}


@pytest.fixture
def validation_creds(monkeypatch):
    mock_creds = MagicMock(spec=ValidationV2Credentials)
    # mock the methods on the credentials that make API calls
    mock_creds.download_manifests = MagicMock(side_effect=[[found_manifest]])
    mock_creds.upload = MagicMock(return_value=[])
    return mock_creds


@pytest.fixture
def validation_creds_not_found(validation_creds):
    validation_creds.download_manifests = MagicMock(side_effect=[[not_found_manifest]])
    return validation_creds


@pytest.fixture
def validation_manifest(monkeypatch):
    def _validation_manifest(filename: str = 'manifest.json'):
        validation_uuid = str(uuid.uuid4())
        mock_creds = MagicMock(spec=ValidationV2Credentials)
        validation_response = ValidatedFile(**{'file': Path('/path/to/manifest.json'),
                                               'v2_creds': mock_creds,
                                               'v2_resource': ValidationV2(
                                                   **{'validation_uuid': validation_uuid,
                                                      'status': 'PendingManifests',
                                                      'short_name': 'fmriresults01',
                                                      'rows': 1,
                                                      'validation_files': {},
                                                      'scope': None})}
                                            )
        manifest_data = {
            'localFileName': filename,
            's3Destination': f's3://nimhda-validation/{validation_uuid}/manifests/manifest.json',
            'recordNumber': 1,
            'header': 'manifest',
            'uuid': str(uuid.uuid4())
        }
        vm = ValidationManifest(**{**manifest_data, 'validation_response': validation_response})
        # mock the methods on the credentials that make API calls
        mock_creds.download_manifests = MagicMock(return_value=[manifest_data])
        return vm

    return _validation_manifest


count = 0


@pytest.fixture
def manifest_file():
    validation_uuid = str(uuid.uuid4())

    def _manifest_file(filename: str = 'manifest.json'):
        global count
        mf_uuid = str(uuid.uuid4())
        s3_destination = f's3://nimhda-validation/{validation_uuid}/manifests/{filename}'
        count += 1
        return ManifestFile(filename, s3_destination, mf_uuid, count, 'manifest')

    return _manifest_file


def test_manifests_not_found_msg(manifest_file):
    """ Test the message output to the console """
    manifests_not_found = [manifest_file(f'manifest{i}.json') for i in range(1, 21)]
    test_path = 'test/manifest/dir'
    msg = _manifests_not_found_msg(manifests_not_found, test_path)

    assert f'The following manifests could not be found in {test_path}:\n' in msg
    for i in range(1, 21):
        assert f'manifest{i}.json' in msg
    assert '...' not in msg

    # any more than 20 manifests should be abbreviated
    manifests_not_found.append(manifest_file('manifest21.json'))
    msg = _manifests_not_found_msg(manifests_not_found, test_path)
    assert f'\n... and 1 more' in msg


def test_upload_manifest(top_level_datadir, validation_creds):
    """Test that a manifest will be uploaded if the manifest is found"""
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationV2Api), 1, True, True)
    manifest_uploader.upload_manifests([validation_creds], top_level_datadir / 'validation')
    assert validation_creds.upload.call_count == 1


def test_upload_manifest_not_found_non_interactive(top_level_datadir, validation_creds_not_found, monkeypatch):
    """Test that upload_manifests exits program with error if manifest is not found and interactive is false"""
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationV2Api), 1, True, True)
    with monkeypatch.context() as m, pytest.raises(SystemExit):
        m.setattr(NDATools.upload.validation.manifests, 'exit_error', MagicMock(side_effect=SystemExit))
        manifest_uploader.upload_manifests([validation_creds_not_found], top_level_datadir)


def test_upload_manifest_not_found_interactive(top_level_datadir, validation_creds, monkeypatch, tmp_path):
    """Test that upload_manifests prompts user if manifest is not found, and re-attempts upload if interactive is true"""
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationV2Api), 1, False, True)
    manifest_uploader._handle_manifests_not_found = MagicMock(wraps=manifest_uploader._handle_manifests_not_found)
    with monkeypatch.context() as m:
        correct_directory = str(top_level_datadir / 'validation')
        m.setattr('builtins.input', MagicMock(return_value=correct_directory))
        manifest_uploader.upload_manifests([validation_creds], tmp_path)
        assert manifest_uploader._handle_manifests_not_found.call_count == 1
        validation_creds.upload.assert_called_with(f'{correct_directory}/{found_manifest["localFileName"]}',
                                                   found_manifest['s3Destination'])
        assert validation_creds.upload.call_count == 1
        builtins.input.assert_called_once_with(
            'Press the "Enter" key to specify location for manifest files and try again:')


def test_upload_manifest_unexpected_error(top_level_datadir, validation_creds, monkeypatch):
    """ Test that an unexpected error causes the program to exit early """
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationV2Api), 1, True, True)
    validation_creds.upload = MagicMock(side_effect=Exception('Unexpected error'))

    with pytest.raises(SystemExit), monkeypatch.context() as m:
        m.setattr(NDATools.upload.validation.manifests, 'exit_error', MagicMock(side_effect=SystemExit))
        manifest_uploader.upload_manifests([validation_creds], top_level_datadir)
