from __future__ import with_statement
from __future__ import absolute_import

import json
import logging
import shutil
import sys
import getpass
import time

import requests
from requests import HTTPError

from NDATools import NDA_TOOLS_LOGGING_YML_FILE, Utils
import logging.config
import yaml

from NDATools.Utils import exit_client, HttpErrorHandlingStrategy

if sys.version_info[0] < 3:
    import ConfigParser as configparser
    input = raw_input
    import thread
else:
    import configparser
    import _thread as thread
import os
from pkg_resources import resource_filename

logger = logging.getLogger(__name__)

class LoggingConfiguration:

    def __init__(self):
        pass

    @staticmethod
    def load_config(logs_directory):
        def make_config():
            file_path = os.path.join(os.path.expanduser('~'), '.NDATools')
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            if not os.path.exists(NDA_TOOLS_LOGGING_YML_FILE):
                shutil.copyfile(resource_filename(__name__, 'clientscripts/config/logging.yml'),
                                NDA_TOOLS_LOGGING_YML_FILE)

        if not os.path.exists(NDA_TOOLS_LOGGING_YML_FILE):
            make_config()

        with open(NDA_TOOLS_LOGGING_YML_FILE, 'r') as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        log_file = os.path.join(logs_directory, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
        config['handlers']['file']['filename']=log_file
        logging.config.dictConfig(config)

class ClientConfiguration:

    def __init__(self, settings_file, username=None, password=None, access_key=None, secret_key=None):
        self.config = configparser.ConfigParser()
        if settings_file == os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'):
            config_location = settings_file
            self._check_and_fix_missing_options(config_location)
        else:
            config_location = resource_filename(__name__, settings_file)

        self.config.read(config_location)
        self.validation_api = self.config.get("Endpoints", "validation")
        self.submission_package_api = self.config.get("Endpoints", "submission_package")
        self.submission_api = self.config.get("Endpoints", "submission")
        self.validationtool_api = self.config.get("Endpoints", "validationtool")
        self.package_api = self.config.get("Endpoints", "package")
        self.datadictionary_api = self.config.get("Endpoints", "datadictionary")
        self.user_api = self.config.get("Endpoints", "user")
        self.aws_access_key = self.config.get("User", "access_key")
        self.aws_secret_key = self.config.get("User", "secret_key")
        self.aws_session_token = self.config.get('User', 'session_token')
        self.username = self.config.get("User", "username")
        self.password = self.config.get("User", "password")

        # self.check_deprecated_settings()

        if username:
            self.username = username
        elif self.username:
            logger.warning("-u/--username argument not provided. Using default value of '%s' which was saved in %s",
                        self.username, os.path.join(os.path.expanduser('~'), '.NDATools' + os.sep + 'settings.cfg'))
        if password:
            self.password = password


        if access_key:
            self.aws_access_key = access_key
        if secret_key:
            self.aws_secret_key = secret_key
        self.collection_id = None
        self.endpoint_title = None
        self.scope = None
        self.directory_list = None
        self.manifest_path = None
        self.source_bucket = None
        self.source_prefix = None
        self.title = None
        self.description = None
        self.JSON = False
        self.hideProgress = False
        self.skip_local_file_check = False

    def _check_and_fix_missing_options(self, config_location):
        default_config = configparser.ConfigParser()
        default_file_path = resource_filename(__name__, 'clientscripts/config/settings.cfg')
        default_config.read(default_file_path)
        default_settings = dict(default_config._sections)
        self.config.read(config_location)
        user_settings = dict(self.config._sections)

        for section in default_settings:
            for option in default_settings[section]:
                if option not in user_settings[section]:
                    logger.debug('[{}][{}] is missing'.format(section, option))
                    with open(config_location, 'w') as configfile:
                        self.config.set(section, option, default_settings[section][option])
                        self.config.write(configfile)

    def make_config(self):
        file_path = os.path.join(os.path.expanduser('~'), '.NDATools')
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        config_path = os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')

        copy_config = configparser.ConfigParser()

        copy_config.add_section("Endpoints")
        copy_config.set("Endpoints", "user", self.user_api)
        copy_config.set("Endpoints", "package", self.package_api)
        copy_config.set("Endpoints", "validation", self.validation_api)
        copy_config.set("Endpoints", "submission_package", self.submission_package_api)
        copy_config.set("Endpoints", "submission", self.submission_api)
        copy_config.set("Endpoints", "validationtool", self.validationtool_api)
        copy_config.set("Endpoints", "datadictionary", self.datadictionary_api)

        copy_config.add_section("User")
        copy_config.set("User", "username", self.username)
        copy_config.set("User", "password", self.password)
        copy_config.set("User", "access_key", self.aws_access_key)
        copy_config.set("User", "secret_key", self.aws_secret_key)
        copy_config.set("User", "session_token", self.aws_session_token)

        with open(config_path, 'w') as configfile:
            copy_config.write(configfile)

    def read_user_credentials(self, auth_req=True):

        if auth_req:
            while True:
                while not self.username:
                    self.username = input('Enter your NIMH Data Archives username:')
                while not self.password:
                    self.password = getpass.getpass('Enter your NIMH Data Archives password:')
                if not self.is_valid_nda_credentials():
                        logger.error("The password that was entered for user '%s' is invalid ...", self.username)
                        self.password = None
                else:
                    break

        # Only ask for access-key/secret-key when needed (which is only when a user is creating a submission
        # and the files are stored in an s3 bucket.
        if self.source_bucket:
            self.read_aws_credentials()

    def read_aws_credentials(self):
        if not self.aws_access_key:
            self.aws_access_key = getpass.getpass("Enter your aws_access_key (must have read access to the %s bucket): " % self.source_bucket)

        if not self.aws_secret_key:
            self.aws_secret_key = getpass.getpass('Enter your aws_secret_key:')

    '''
    leaving this method here in case we decide to move ahead with plan to drop support for -p flag
    def check_deprecated_settings(self):
        if self.config.has_option('User','password') and self.config.get("User", "password"):
            print('Warning: Detected non-empty value for "password" in settings.cfg. Support for this setting has been deprecated and will no longer be used by this '
                  'tool. Password storage is not recommended for security considerations')
    '''

    def is_valid_nda_credentials(self):
        try:
            # will raise HTTP error 401 if invalid creds
            tmp = Utils.get_request(self.user_api, headers={'content-type': 'application/json'},
                          auth=requests.auth.HTTPBasicAuth(self.username, self.password),
                                    error_handler=HttpErrorHandlingStrategy.reraise_status)
            return True
        except HTTPError as e:
            if e.response.status_code == 401:
                if 'locked' in e.response.text:
                    # user account is locked
                    tmp = json.loads(e.response.text)
                    logger.error('\nError: %s', tmp['message'])
                    logger.error('\nPlease contact NDAHelp@mail.nih.gov for help in resolving this error')
                    exit_client()
                else:
                    return False
            else:
                logger.error('\nSystemError while checking credentials for user %s', self.username)
                logger.error('\nPlease contact NDAHelp@mail.nih.gov for help in resolving this error')
                exit_client()
