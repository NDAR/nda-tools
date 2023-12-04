import datetime
import itertools
import json
import math
import os
import shutil
import sys
from unittest.mock import ANY, MagicMock

import pytest
from requests import HTTPError

import NDATools.Utils
import NDATools.clientscripts.downloadcmd
from NDATools.Download import Download

FAKE_FILE_BYTES = [0x10, 0x10]
MISSING_FILE = 's3://NDAR_Central_1/submission_43568/not-found.png'


class TestDownload:

    @pytest.fixture(autouse=True)
    def class_setup(self, load_from_file):
        self.load_from_file = load_from_file


    def test_s3links_argument_file_not_found(self,
                                        download_config_factory,
                                        shared_datadir,
                                        capsys,
                                        monkeypatch,
                                        tmp_path,
                                        tmpdir):
        test_package_id = '1189934'
        test_text_structure_file = 'api_responses/s3/ds_test/test-not-found.csv'
        test_args = ['-dp', test_package_id, '-t', os.path.join(shared_datadir, test_text_structure_file)]

        config, args = download_config_factory(test_args)

        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            meta = tmpdir / "meta"
            meta.mkdir()
            self.package_metadata_directory = str(meta)
            logs = tmpdir / "logs"
            logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            return str(tmpdir/ "test_dd.csv")
        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

        def mock_generate_download_batch_file_ids(*args, **kwargs):
            return []

        def mock_get_completed_files_in_download(*args, **kwargs):
            return []

        download_mock = MagicMock(return_value = {'actual_file_size':10, 'download_complete_time': datetime.datetime.now()})

        def get_presigned_urls_mock(*args, **kwargs):
            return {f:'s3://fake-presigned-url' for f in args[1]}


        def mock_post_request (*args, **kwargs):
            response = MagicMock()
            payload = args[1]
            if  MISSING_FILE in payload:
                response_text = '''The following files are invalid\r\n{}'''.format(MISSING_FILE)
                response.text = response_text
                response.status_code = 404
                e = HTTPError(response=response)

                def rethrow(*args, **kwargs):
                    raise e

                response.ok.return_value = False
                response.raise_for_status.side_effect = rethrow
                return response
            else:
                response_text = self.load_from_file('api_responses/package/ds_test/get_files_by_s3.json')
                response.json.return_value = json.loads(response_text)
                response.text = response_text
                return response

        with monkeypatch.context() as m:
            m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
            m.setattr(Download, 'get_package_info', mock_get_package_info)
            m.setattr(Download, 'get_completed_files_in_download', mock_get_completed_files_in_download)
            m.setattr(NDATools.Download, 'post_request', mock_post_request)
            m.setattr(Download, 'download_from_s3link', download_mock)
            m.setattr(Download, 'get_presigned_urls', get_presigned_urls_mock)

            Download(args, config).start()

            captured = capsys.readouterr()
            assert 'WARNING: The following associated files were not found' in captured.out
            assert 'Beginning download of 3 files' in captured.out
            assert download_mock.call_count == 3  # number of files in the DS file


    def test_ds_argument_structure_not_found(self,
                                             download_config_factory,
                                             capsys, monkeypatch, shared_datadir, tmp_path, tmpdir):

        test_package_id = '1189934'
        test_data_structure_file = 'image03'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file]

        config, args = download_config_factory(test_args)

        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            meta = tmpdir / "meta"
            meta.mkdir()
            self.package_metadata_directory = str(meta)
            logs = tmpdir / "logs"
            logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            return str(tmpdir/ "test_dd.csv")
        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))
        def mock_generate_download_batch_file_ids(*args, **kwargs):
            return []

        def mock_get_completed_files_in_download(*args, **kwargs):
            return []

        def mock_os_exit(*args, **kwargs):
            sys.exit()
        def mock_get_files_from_datastructure(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/package/ds_test/no-files.json'))['results']

        with monkeypatch.context() as m:
            m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
            m.setattr(Download, 'get_package_info', mock_get_package_info)
            m.setattr(Download, 'get_completed_files_in_download', mock_get_completed_files_in_download)
            m.setattr(Download, 'get_files_from_datastructure', mock_get_files_from_datastructure)

            m.setattr(Download, 'generate_download_batch_file_ids', mock_generate_download_batch_file_ids)
            m.setattr(os, '_exit', mock_os_exit)


            with pytest.raises(SystemExit):
                Download(args, config).start()

            captured = capsys.readouterr()
            # Program must print out that the structure was not found
            assert "{} data structure is not included in the package".format(test_data_structure_file) in captured.out


    def test_ds_argument_success(self,
                                 download_config_factory,
                                 shared_datadir, tmp_path, tmpdir, monkeypatch):

        test_package_id = '1189934'
        test_data_structure_file = 'fmriresults01'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file, '-d', str(tmpdir)]

        config, args = download_config_factory(test_args)
        def get_presigned_urls_mock(*args, **kwargs):
            return {f:'s3://fake-presigned-url' for f in args[1]}

        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            meta = tmpdir / "meta"
            meta.mkdir()
            self.package_metadata_directory = str(meta)
            logs = tmpdir / "logs"
            logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            return str(tmpdir / "test_dd.csv")

        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

        def mock_get_files_from_datastructure(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/package/ds_test/all_mri_files.json'))['results']

        def mock_get_completed_files_in_download(*args, **kwargs):
            return []

        download_mock = MagicMock(return_value = {'actual_file_size':10, 'download_complete_time': datetime.datetime.now()})
        with monkeypatch.context() as m:
            m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
            m.setattr(Download, 'get_package_info', mock_get_package_info)
            m.setattr(Download, 'get_completed_files_in_download', mock_get_completed_files_in_download)
            m.setattr(Download, 'use_data_structure', mock_get_files_from_datastructure)
            m.setattr(Download, 'download_from_s3link', download_mock)
            m.setattr(Download, 'get_presigned_urls', get_presigned_urls_mock)

            Download(args, config).start()
            # write should be called for each fake byte in the file (see above mock_api_response call)

            assert download_mock.call_count == 3  # number of files in the DS file


    def test_resume_download_using_download_history_report(self,
                                 download_config_factory,
                                 shared_datadir, tmp_path, tmpdir, monkeypatch):

        test_package_id = '1189934'
        test_data_structure_file = 'fmriresults01'
        test_args = ['-dp', test_package_id, '-ds', test_data_structure_file, '-d', str(tmpdir)]

        config, args = download_config_factory(test_args)
        def get_presigned_urls_mock(*args, **kwargs):
            return {f:'s3://fake-presigned-url' for f in args[1]}

        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            meta = tmpdir / "meta"
            meta.mkdir()
            self.package_metadata_directory = str(meta)
            logs = tmpdir / "logs"
            logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            self.download_job_uuid = '9479421578f74b8997f62cd962a1cb08'

            tmp1 = meta / '.download-progress'
            tmp1.mkdir()

            tmp2 = tmp1 / self.download_job_uuid
            tmp2.mkdir()

            shutil.copy(os.path.join(shared_datadir, 'api_responses/package/ds_test/download-progress-report.csv'), str(tmp2))

            return str(tmpdir / "test_dd.csv")

        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

        def mock_get_files_from_datastructure(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/package/ds_test/all_mri_files.json'))['results']

        download_mock = MagicMock(return_value = {'actual_file_size':10, 'download_complete_time': datetime.datetime.now()})
        with monkeypatch.context() as m:
            m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
            m.setattr(Download, 'get_package_info', mock_get_package_info)

            m.setattr(Download, 'use_data_structure', mock_get_files_from_datastructure)
            m.setattr(Download, 'download_from_s3link', download_mock)
            m.setattr(Download, 'get_presigned_urls', get_presigned_urls_mock)

            Download(args, config).start()

            assert download_mock.call_count == 1  # number of files in the DS file
            download_mock.assert_called_with(3132764123, 's3://fake-presigned-url', failed_s3_links_file=ANY)


    def test_s3links_argument_success(self,
                                      download_config_factory,
                                      shared_datadir,
                                      capsys,
                                      monkeypatch,
                                      tmp_path,
                                      tmpdir):
        test_package_id = '1189934'
        test_text_structure_file = 'api_responses/s3/ds_test/test-found.csv'
        test_args = ['-dp', test_package_id, '-t', os.path.join(shared_datadir, test_text_structure_file)]

        config,args = download_config_factory(test_args)

        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            meta = tmpdir / "meta"
            meta.mkdir()
            self.package_metadata_directory = str(meta)
            logs = tmpdir / "logs"
            logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            return str(tmpdir / "test_dd.csv")

        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

        def mock_generate_download_batch_file_ids(*args, **kwargs):
            return []

        def mock_get_completed_files_in_download(*args, **kwargs):
            return []
        def mock_use_s3_link_file(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/package/ds_test/get_files_by_s3.json'))

        with monkeypatch.context() as m:
            m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
            m.setattr(Download, 'get_package_info', mock_get_package_info)
            m.setattr(Download, 'get_completed_files_in_download', mock_get_completed_files_in_download)


            m.setattr(Download, 'use_s3_links_file', mock_use_s3_link_file)
            m.setattr(Download, 'generate_download_batch_file_ids', mock_generate_download_batch_file_ids)

            # m.setattr(os, 'path.sep', self.mock_post_api_response)
            Download(args, config).start()

            captured = capsys.readouterr()
            assert 'WARNING: The following associated files were not found' not in captured.out
            assert 'Beginning download of 3 files' in captured.out

    def test_generate_s3_files_batch(self,
                                      download_config_factory,
                                      shared_datadir,
                                      capsys,
                                      monkeypatch,
                                      tmp_path,
                                      tmpdir):


        def mock_initialize_verification_files(*args, **kwargs):
            self = args[0]
            self.default_download_batch_size=TEST_BATCH_SIZE
            meta = tmpdir / "meta"
            if not os.path.exists(meta):
                meta.mkdir()
            self.package_metadata_directory = str(meta)
            self.download_job_uuid = '123e4567-e89b-12d3-a456-426614174000'
            logs = tmpdir / "logs"
            if not os.path.exists(logs):
                logs.mkdir()
            NDATools.NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = str(logs)
            return str(tmpdir / "test_dd.csv")

        def get_presigned_urls_mock(*args, **kwargs):
            return {f:'s3://fake-presigned-url' for f in args[0]}

        def mock_get_package_info(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-package-info-response.json'))

        def mock_get_completed_files_in_download(*args, **kwargs):
            def _mock_fn(*args2, **kwargs2):
                return [{'package_file_id':int(r), 'actual_file_size': 10, 'download_complete_time': datetime.datetime.now()} for r in kwargs['completed_files']]
            return _mock_fn

        def get_package_files_by_page_method_mock(*args, **kwargs):
            page=args[0]
            batch_size = args[1]
            res = json.loads(self.load_from_file('api_responses/s3/ds_test/get-files-from-datastructure-response.json'))[
                'results']
            from_i = ((page-1) * batch_size)
            to_i = from_i + batch_size
            return res[from_i: to_i]

        def mock_use_datastructure(*args, **kwargs):
            return json.loads(self.load_from_file('api_responses/s3/ds_test/get-files-from-datastructure-response.json'))[
                'results']

        all_files = json.loads(self.load_from_file('api_responses/s3/ds_test/get-files-from-datastructure-response.json'))['results']
        all_file_ids = set(map(lambda x: int(x['package_file_id']), all_files))

        def test(config,args, batch_size, completed_files, get_package_files_by_page_args_list):
            completed_file_ids = set(map(lambda x: int(x['package_file_id']), completed_files))
            with monkeypatch.context() as m:
                m.setattr(Download, 'initialize_verification_files', mock_initialize_verification_files)
                m.setattr(Download, 'get_package_info', mock_get_package_info)
                m.setattr(Download, 'get_completed_files_in_download', mock_get_completed_files_in_download(completed_files=completed_file_ids))

                #mock_presigned_url_method = MagicMock()
                mock_download_method = MagicMock(return_value={'actual_file_size':10, 'download_complete_time': datetime.datetime.now()})
                m.setattr(Download, 'download_from_s3link', mock_download_method)

                d = Download(args, config)
                wrap_download_batch_file_ids = MagicMock(wraps=d.generate_download_batch_file_ids)
                m.setattr(Download, 'generate_download_batch_file_ids', wrap_download_batch_file_ids)
                m.setattr(d, 'default_download_batch_size', batch_size)
                get_presigned_wrap = MagicMock(wraps=get_presigned_urls_mock)
                m.setattr(Download, 'get_presigned_urls', get_presigned_wrap)
                m.setattr(Download, 'use_data_structure', mock_use_datastructure)

                mock_get_package_files_by_page_method = MagicMock(wraps=get_package_files_by_page_method_mock)
                m.setattr(Download, 'get_package_files_by_page', mock_get_package_files_by_page_method)

                d.start()

                assert wrap_download_batch_file_ids.call_count == 1 # its a generator method so it will always be called once, although it may yield many values
                assert list(map(lambda x: x[0], mock_get_package_files_by_page_method.call_args_list)) == get_package_files_by_page_args_list
                assert mock_download_method.call_count == len(all_file_ids) - len(completed_file_ids) # call download method for each file that hasnt been downloaded
                assert set(map(lambda x: x[0][0], mock_download_method.call_args_list)) == {f for f in all_file_ids if f not in completed_file_ids}
                # we started batching calls to this endpoint, so it would be the
                if d.download_mode=='package':
                    assert get_presigned_wrap.call_count == len(get_package_files_by_page_args_list) - 1 == math.ceil((len(all_file_ids) - len(completed_file_ids)) / TEST_BATCH_SIZE)
                    assert set(d.local_file_names.keys()) == all_file_ids - completed_file_ids
                else:
                    assert get_presigned_wrap.call_count == math.ceil((len(all_file_ids) - len(completed_file_ids)) / TEST_BATCH_SIZE)
                    assert set(d.local_file_names.keys()) == all_file_ids #
                assert set(itertools.chain(*map(lambda x: x[0][0], get_presigned_wrap.call_args_list))) == {f for f in all_file_ids if f not in completed_file_ids}


        test_package_id = '1189934'
        test_args = ['-dp', test_package_id, '-u', 'ndar_administrator']
        config,args = download_config_factory(test_args)
        TEST_BATCH_SIZE = 3

        # test package download
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=[], get_package_files_by_page_args_list=[(1, TEST_BATCH_SIZE), (2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:1], get_package_files_by_page_args_list=[(1, TEST_BATCH_SIZE), (2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:2], get_package_files_by_page_args_list=[(1, TEST_BATCH_SIZE), (2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:3], get_package_files_by_page_args_list=[(2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:4], get_package_files_by_page_args_list=[(2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:5], get_package_files_by_page_args_list=[(2, TEST_BATCH_SIZE), (3, TEST_BATCH_SIZE)])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:], get_package_files_by_page_args_list=[(3, TEST_BATCH_SIZE)])

        # test download ds
        test_package_id = '1189934'
        test_args = ['-dp', test_package_id, '-ds', 'fmriresults01', '-u', 'ndar_administrator']
        config,args = download_config_factory(test_args)
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=[], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:1], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:2], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:3], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:4], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:5], get_package_files_by_page_args_list=[])
        test(config,args, batch_size=TEST_BATCH_SIZE, completed_files=all_files[:], get_package_files_by_page_args_list=[])
