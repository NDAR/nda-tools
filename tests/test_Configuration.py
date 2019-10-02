from NDATools.Configuration import *
import pytest


def test_init():
    new_config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
    assert new_config.config.get("Endpoints", "validation") is not None
    assert new_config.config.get("Endpoints", "submission_package")
    assert new_config.config.get("Endpoints", "submission")
    assert new_config.config.get("Endpoints", "validationtool")
    assert new_config.config.get("Endpoints", "data_manager")
    assert new_config.config.get("Files", "validation_results")
    assert new_config.config.get("Files", "submission_packages")
    assert new_config.config.get("User", "username")
    assert new_config.config.get("User", "password")
    assert new_config.config.get("User", "access_key")
    assert new_config.config.get("User", "secret_key")
    assert new_config.config.get("User", "session_token")


def test_invalid_path_raises_exception():
    with pytest.raises(ValueError):
        new_config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings1.cfg'))
        assert new_config.config.get("Endpoints", "validation") is not None
        assert new_config.config.get("Endpoints", "submission_package")
        assert new_config.config.get("Endpoints", "submission")
        assert new_config.config.get("Endpoints", "validationtool")
        assert new_config.config.get("Endpoints", "data_manager")
        assert new_config.config.get("Files", "validation_results")
        assert new_config.config.get("Files", "submission_packages")
        assert new_config.config.get("User", "username")
        assert new_config.config.get("User", "password")
        assert new_config.config.get("User", "access_key")
        assert new_config.config.get("User", "secret_key")
    # assert new_config.config.get("User", "session_token")
    # if settings_file == os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'):
    #   config_location = settings_file
    # else:
    #   config_location = resource_filename(__name__, settings_file)
    # self.config.read(config_location)
    # self.validation_api = self.config.get("Endpoints", "validation")
    # self.submission_package_api = self.config.get("Endpoints", "submission_package")
    # self.submission_api = self.config.get("Endpoints", "submission")
    # self.validationtool_api = self.config.get("Endpoints", "validationtool")
    # self.datamanager_api = self.config.get("Endpoints", "data_manager")
    # self.validation_results = self.config.get("Files", "validation_results")
    # self.submission_packages = self.config.get("Files", "submission_packages")
    # self.collection_id = None
    # self.endpoint_title = None
    # self.scope = None
    # self.directory_list = None
    # self.manifest_path = None
    # self.aws_access_key = self.config.get("User", "access_key")
    # self.aws_secret_key = self.config.get("User", "secret_key")
    # self.aws_session_token = self.config.get("User", "session_token")
    # self.source_bucket = None
    # self.source_prefix = None
    # self.title = None
    # self.description = None
    # self.JSON = False
    # self.hideProgress = False
    # self.skip_local_file_check = False
    # self.username = self.config.get("User", "username")
    # self.password = self.config.get("User", "password")
    # if username:
    #   self.username = username
    # if password:
    #   self.password = password
    # if access_key:
    #   self.aws_access_key = access_key
    # if secret_key:
    #   self.aws_secret_key = secret_key
