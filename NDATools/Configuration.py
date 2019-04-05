from __future__ import with_statement
from __future__ import absolute_import
import sys
import getpass
if sys.version_info[0] < 3:
    import ConfigParser as configparser
    input = raw_input
    import thread
else:
    import configparser
    import _thread as thread
import os

from pkg_resources import resource_filename

class ClientConfiguration:
    def __init__(self, settings_file, username=None, password=None, access_key=None, secret_key=None):
        self.config = configparser.ConfigParser()
        if settings_file == os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'):
            config_location = settings_file
        else:
            config_location = resource_filename(__name__, settings_file)
        self.config.read(config_location)
        self.validation_api = self.config.get("Endpoints", "validation")
        self.submission_package_api = self.config.get("Endpoints", "submission_package")
        self.submission_api = self.config.get("Endpoints", "submission")
        self.validationtool_api = self.config.get("Endpoints", "validationtool")
        self.datamanager_api = self.config.get("Endpoints", "data_manager")
        self.validation_results = self.config.get("Files", "validation_results")
        self.submission_packages = self.config.get("Files", "submission_packages")
        self.collection_id = None
        self.endpoint_title = None
        self.scope = None
        self.directory_list = None
        self.manifest_path = None
        self.aws_access_key = self.config.get("User", "access_key")
        self.aws_secret_key = self.config.get("User", "secret_key")
        self.source_bucket = None
        self.source_prefix = None
        self.title = None
        self.description = None
        self.JSON = False
        self.username = self.config.get("User", "username")
        self.password = self.config.get("User", "password")
        if username:
            self.username = username
        if password:
            self.password = password
        if access_key:
            self.aws_access_key = access_key
        if secret_key:
            self.aws_secret_key = secret_key


    def make_config(self):
        file_path = os.path.join(os.path.expanduser('~'), '.NDATools')
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        config_path = os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')

        copy_config = configparser.ConfigParser()

        copy_config.add_section("Endpoints")
        copy_config.set("Endpoints", "data_manager", self.datamanager_api)
        copy_config.set("Endpoints", "validation", self.validation_api)
        copy_config.set("Endpoints", "submission_package", self.submission_package_api)
        copy_config.set("Endpoints", "submission", self.submission_api)
        copy_config.set("Endpoints", "validationtool", self.validationtool_api)

        copy_config.add_section("Files")
        copy_config.set("Files", "validation_results", self.validation_results)
        copy_config.set("Files", "submission_packages", self.submission_packages)

        copy_config.add_section("User")
        copy_config.set("User", "username", self.username)
        copy_config.set("User", "password", self.password)
        copy_config.set("User", "access_key", self.aws_access_key)
        copy_config.set("User", "secret_key", self.aws_secret_key)


        with open(config_path, 'w') as configfile:
            copy_config.write(configfile)


    def read_user_credentials(self):
        if not self.username:
            self.username = input('Enter your NIMH Data Archives username:')

        if not self.password:
            self.password = getpass.getpass('Enter your NIMH Data Archives password:')

        if not self.aws_access_key:
            self.aws_access_key = getpass.getpass('Enter your aws_access_key. If none, hit "Enter:"')

        if not self.aws_secret_key:
            self.aws_secret_key = getpass.getpass('Enter your aws_secret_key. If none, hit "Enter":')
