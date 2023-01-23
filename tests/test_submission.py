import json
import uuid
from unittest.mock import MagicMock

import pytest

from NDATools import Submission, Utils
from NDATools.Submission import Status


class TestSubmission():

    @pytest.fixture
    def api_endpoint(self):
        def fix_factory(*args, **kwargs):
            api_response = args[0]
            def __api_result(*args, **kwargs):
                return api_response
            return __api_result
        return fix_factory

    @pytest.fixture
    def config(self, shared_datadir, validation_config_factory):
        file_path = (shared_datadir / 'validation/file.csv')
        test_args = [str(file_path)]
        _, config = validation_config_factory(test_args)
        config.JSON = True
        return config

    @pytest.fixture
    def submission_with_files(self, load_from_file):
        files_resource = json.loads(load_from_file('submission/api_response/get_files_response.json'))
        submission_resource =  json.loads(load_from_file('submission/api_response/create_submission_response.json'))
        creds_resource =  json.loads(load_from_file('submission/api_response/batch_get_temp_creds.json'))
        return (submission_resource, files_resource, creds_resource)

    @pytest.fixture
    def batch_update_status(self, load_from_file):
        return json.loads(load_from_file('submission/api_response/batch_update_status_response.json'))

    def test_resume_submission(self, monkeypatch, config, submission_with_files, api_endpoint, tmpdir, batch_update_status):
        submission_resource, files_resource, creds_resource = submission_with_files
        file_key = Utils.sanitize_file_path(list(filter(lambda x: x['file_type']=='Submission Associated File', files_resource))[0]['file_user_path'])
        fake_file_size = 10
        fake_abs_path = str(tmpdir/file_key)
        with monkeypatch.context() as m:
            mock_status = MagicMock()
            mock_get_creds = MagicMock(return_value =creds_resource['credentials'])
            mock_batch_update = MagicMock(return_value =batch_update_status)
            mock_abort_previous_uploads = MagicMock()
            mock_upload_files = MagicMock()
            m.setattr(Submission.Submission, 'check_status', mock_status)
            m.setattr(Submission.Submission, 'get_multipart_credentials', mock_get_creds)
            m.setattr(Submission.Submission, 'abort_previous_upload_attempts', mock_abort_previous_uploads)
            m.setattr(Submission.Submission, 'batch_update_status', mock_batch_update['errors'])
            m.setattr(Submission.Submission, 'upload_associated_files', mock_upload_files)
            m.setattr(Submission, 'post_request', api_endpoint(submission_resource))
            m.setattr(Submission, 'get_request', api_endpoint(files_resource))
            def mock_complicated_search_file_logic(*args, **kwargs):
                submission.full_file_path = { file_key: (fake_abs_path, fake_file_size)}
                args[1].clear()
                return
            mock_search_file_sys = MagicMock(wraps=mock_complicated_search_file_logic)
            m.setattr(Submission, 'parse_local_files',mock_search_file_sys)

            submission = Submission.Submission(submission_id=submission_resource['submission_id'],
                                               full_file_path={},
                                               thread_num=1,
                                               batch_size=20,
                                               allow_exit=True,
                                               config=config)
            submission.status = Status.UPLOADING
            submission.directory_list = [str(tmpdir)]
            submission.resume_submission()

            assert mock_status.call_count == 2
            assert mock_upload_files.call_count == 1
            assert mock_get_creds.call_count == 1
            assert mock_abort_previous_uploads.call_count ==1 
            assert submission.get_files()==files_resource

    def test_create_submission_success(self, monkeypatch, config, submission_with_files, api_endpoint):
        submission_resource, files_resource, _ = submission_with_files

        with monkeypatch.context() as m:
            m.setattr(Submission, 'post_request', api_endpoint(submission_resource))
            m.setattr(Submission, 'get_request', api_endpoint(files_resource))
            package_id = uuid.uuid4()

            submission = Submission.Submission(package_id=str(package_id),
                                                         full_file_path={},
                                                         thread_num=1,
                                                         batch_size=20,
                                                         allow_exit=True,
                                                         config=config)
            submission.submit()

            assert submission.submission_id == submission_resource['submission_id']
            assert submission.status == submission_resource['submission_status']
            assert submission.get_files()==files_resource

    def test_replace_submission(self, monkeypatch, config, submission_with_files, api_endpoint):

        submission_resource, files_resource, _ = submission_with_files

        with monkeypatch.context() as m:
            m.setattr(Submission, 'put_request', api_endpoint(submission_resource))
            m.setattr(Submission, 'get_request', api_endpoint(files_resource))
            package_id = uuid.uuid4()

            submission = Submission.Submission(submission_id=submission_resource['submission_id'],
                                               package_id=str(package_id),
                                               full_file_path={},
                                               thread_num=1,
                                               batch_size=20,
                                               allow_exit=True,
                                               config=config)
            submission.replace_submission()

            assert submission.submission_id == submission_resource['submission_id']
            assert submission.status == submission_resource['submission_status']
            assert submission.get_files()==files_resource

