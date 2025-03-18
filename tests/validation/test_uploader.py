import builtins
import uuid
from pathlib import Path
from unittest.mock import MagicMock, ANY

import pytest

import NDATools
from NDATools.upload.validation.api import ValidationManifest, ValidationV2Credentials, \
    ValidationApi, ValidationResponse, ValidationV2
from NDATools.upload.validation.uploader import ManifestNotFoundError, ManifestsUploader
from NDATools.upload.validation.uploader import _manifests_not_found_msg


def create_manifest_not_found(file_name, exception: Exception):
    validation_manifest = MagicMock(spec=ValidationManifest)
    validation_manifest.local_file_name = file_name
    manifest_not_found = ManifestNotFoundError(validation_manifest, exception)
    return manifest_not_found


@pytest.fixture
def validation_manifest(monkeypatch):
    def _validation_manifest(filename: str = 'manifest.json'):
        validation_uuid = str(uuid.uuid4())
        mock_creds = MagicMock(spec=ValidationV2Credentials)
        validation_response = ValidationResponse(**{'file': Path('/path/to/manifest.json'),
                                                    'creds': mock_creds,
                                                    'validation_resource': ValidationV2(
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


@pytest.fixture
def manifest_not_found_error(validation_manifest):
    def _manifest_not_found(file_name):
        vm = validation_manifest(file_name)
        manifest_not_found = ManifestNotFoundError(vm)
        return manifest_not_found

    return _manifest_not_found


def test_manifests_not_found_msg(manifest_not_found_error):
    """ Test the message output to the console """
    manifests_not_found = [manifest_not_found_error(f'manifest{i}.json') for i in range(1, 21)]
    test_path = 'test/manifest/dir'
    msg = _manifests_not_found_msg(manifests_not_found, test_path)

    assert f'The following manifests could not be found in {test_path}:\n' in msg
    for i in range(1, 21):
        assert f'manifest{i}.json' in msg
    assert '...' not in msg

    # any more than 20 manifests should be abbreviated
    manifests_not_found.append(manifest_not_found_error('manifest21.json'))
    msg = _manifests_not_found_msg(manifests_not_found, test_path)
    assert f'\n... and 1 more' in msg


def test_upload_manifest(shared_datadir, validation_manifest):
    """Test that a manifest will be uploaded if the manifest is found"""
    vm = validation_manifest('manifest.json')
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationApi), 1, False, True)
    manifest_uploader.upload_manifests([vm], shared_datadir)
    assert vm.validation_response.rw_creds.upload.call_count == 1


def test_upload_manifest_not_found_non_interactive(shared_datadir, validation_manifest, monkeypatch):
    """Test that upload_manifests exits program with error if manifest is not found and interactive is false"""
    vm = validation_manifest('notfound.json')
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationApi), 1, False, True)
    with monkeypatch.context() as m, pytest.raises(SystemExit):
        m.setattr(NDATools.upload.validation.uploader, 'exit_error', MagicMock(side_effect=SystemExit))
        manifest_uploader.upload_manifests([vm], shared_datadir)


def test_upload_manifest_not_found_interactive(shared_datadir, validation_manifest, monkeypatch, tmp_path):
    """Test that upload_manifests prompts user if manifest is not found, and re-attempts upload if interactive is true"""
    vm = validation_manifest('manifest.json')
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationApi), 1, True, True)
    manifest_uploader._handle_manifests_not_found = MagicMock(wraps=manifest_uploader._handle_manifests_not_found)
    manifest_uploader._upload_manifests = MagicMock(wraps=manifest_uploader._upload_manifests)
    with monkeypatch.context() as m:
        m.setattr('builtins.input', MagicMock(return_value=str(shared_datadir)))
        manifest_uploader.upload_manifests([vm], tmp_path)
        assert manifest_uploader._handle_manifests_not_found.call_count == 1
        manifest_uploader._upload_manifests.assert_called_with([vm], shared_datadir, ANY)
        assert vm.validation_response.rw_creds.upload.call_count == 1
        builtins.input.assert_called_once_with(
            'Press the "Enter" key to specify location for manifest files and try again:')


def test_upload_manifest_unexpected_error(shared_datadir, validation_manifest, monkeypatch):
    """ Test that an unexpected error causes the program to exit early """
    vm = validation_manifest('manifest.json')
    manifest_uploader = ManifestsUploader(MagicMock(spec=ValidationApi), 1, False, True)
    vm.validation_response.rw_creds.upload = MagicMock(side_effect=Exception('Unexpected error'))

    with pytest.raises(SystemExit), monkeypatch.context() as m:
        m.setattr(NDATools.upload.validation.uploader, 'exit_error', MagicMock(side_effect=SystemExit))
        manifest_uploader.upload_manifests([vm], shared_datadir)
