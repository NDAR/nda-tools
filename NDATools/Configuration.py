import configparser
import getpass
import json
import logging
import logging.config
import os
import time

import requests
import yaml
from pkg_resources import resource_filename
from requests import HTTPError

import NDATools
from NDATools import NDA_TOOLS_LOGGING_YML_FILE
from NDATools.Utils import exit_error, HttpErrorHandlingStrategy, get_request
from NDATools.upload.cli import NdaUploadCli
from NDATools.upload.validation.api import ValidationV2Api
from NDATools.upload.validation.manifests import ManifestsUploader
from NDATools.upload.validation.results_writer import ResultsWriterFactory

logger = logging.getLogger(__name__)

try:
    import keyring
except Exception as e:
    logger.debug(f'Error while importing keyring module: {str(e)}')
    keyring = None


class LoggingConfiguration:

    def __init__(self):
        pass

    @staticmethod
    def load_config(default_log_directory, verbose=False, log_dir=None):

        with open(NDA_TOOLS_LOGGING_YML_FILE, 'r') as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        if log_dir and os.path.exists(log_dir):
            log_file = os.path.join(log_dir, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
        else:
            log_file = os.path.join(default_log_directory, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
        config['handlers']['file']['filename'] = log_file
        if verbose:
            config['loggers']['NDATools']['level'] = 'DEBUG'
            config['handlers']['console']['formatter'] = 'detailed'
        logging.config.dictConfig(config)


class ClientConfiguration:
    SERVICE_NAME = 'nda-tools'

    def __init__(self, args):
        self._use_keyring = True if keyring is not None else False
        self.config = configparser.ConfigParser()
        logger.info('Using configuration file from {}'.format(NDATools.NDA_TOOLS_SETTINGS_CFG_FILE))
        self.config.read(NDATools.NDA_TOOLS_SETTINGS_CFG_FILE)
        self._check_and_fix_missing_options()
        self.validation_api_endpoint = self.config.get("Endpoints", "validation")
        self.submission_package_api_endpoint = self.config.get("Endpoints", "submission_package")
        self.submission_api_endpoint = self.config.get("Endpoints", "submission")
        self.validationtool_api_endpoint = self.config.get("Endpoints", "validationtool")
        self.package_creation_api_endpoint = self.config.get("Endpoints", "package_creation")
        self.package_api_endpoint = self.config.get("Endpoints", "package")
        self.datadictionary_api_endpoint = self.config.get("Endpoints", "datadictionary")
        self.collection_api_endpoint = self.config.get("Endpoints", "collection")
        self.user_api_endpoint = self.config.get("Endpoints", "user")
        self.username = self.config.get("User", "username").lower()
        self._args = args

        # options that appear in both vtcmd and downloadcmd
        self.workerThreads = args.workerThreads

        if args.username:
            self.username = args.username
        elif self.username:
            logger.warning("-u/--username argument not provided. Using default value of '%s' which was saved in %s",
                           self.username, NDATools.NDA_TOOLS_SETTINGS_CFG_FILE)
        self.password = None

        is_vtcmd = 'collectionID' in args
        if is_vtcmd:
            self.v2_enabled = False
            self.validation_results_writer = ResultsWriterFactory.get_writer(file_format='json' if args.JSON else 'csv')
            self.validation_api = None
            self.manifests_uploader = None
            self.upload_cli = NdaUploadCli(self)
        if self.username:
            logger.info('proceeding as NDA user: {}'.format(self.username))

    @property
    def hide_progress(self):
        return self._args.hideProgress

    @property
    def force(self):
        return True if self._args.force else False

    @property
    def collection_id(self):
        return self._args.collectionID

    @property
    def directory_list(self):
        return self._args.listDir

    @property
    def manifest_path(self):
        return self._args.manifestPath

    @property
    def validation_timeout(self):
        return self._args.validation_timeout

    @property
    def title(self):
        return self._args.title

    @property
    def description(self):
        return self._args.description

    @property
    def scope(self):
        return self._args.scope

    @property
    def replace_submission(self):
        return self._args.replace_submission

    def _check_and_fix_missing_options(self):
        default_config = configparser.ConfigParser()
        default_file_path = resource_filename(__name__, 'clientscripts/config/settings.cfg')
        default_config.read(default_file_path)
        change_detected = False
        for section in default_config.sections():
            if section not in self.config.sections():
                logger.debug(f'adding {section} to settings.cfg')
                self.config.add_section(section)
                change_detected = True
            for option in default_config[section]:
                if option not in self.config[section]:
                    logger.debug('[{}][{}] is missing'.format(section, option))
                    self.config.set(section, option, default_config[section][option])
                    change_detected = True
        if change_detected:
            logger.debug(f'updating settings.cfg')
            with open(NDATools.NDA_TOOLS_SETTINGS_CFG_FILE, 'w') as configfile:
                self.config.write(configfile)
        else:
            logger.debug(f'settings.cfg is up to date')

    def is_authenticated(self):
        return self.username and self.password

    def _get_password(self):
        try:
            if self._use_keyring:
                self.password = keyring.get_password(self.SERVICE_NAME, self.username)
                if not self.password:
                    logger.debug('no password found in keyring')
                    self._use_keyring = False
                    self._get_password()
                logger.debug('retrieved password from keyring')
            else:
                self.password = getpass.getpass('Enter your NDA account password:')
        except Exception as e:
            logger.warning(f'could not retrieve password from keyring: {str(e)}')
            self._use_keyring = False
            self._get_password()

    def _try_save_password_keyring(self):
        try:
            if self._use_keyring:
                keyring.set_password(self.SERVICE_NAME, self.username, self.password)
        except Exception as e:
            logger.warning(f'could not save password to keyring: {str(e)}')

    def _save_username(self):
        with open(NDATools.NDA_TOOLS_SETTINGS_CFG_FILE, 'w') as configfile:
            self.config.set('User', 'username', self.username)
            self.config.write(configfile)

    def read_user_credentials(self):
        def prompt_for_username():
            self.username = str(input('Enter your NDA account username:')).lower()
            self._save_username()

        # username is fetched from settings.cfg, and it is not present at the first time use of nda-tools
        # display NDA account instructions
        if not self.username:
            logger.info(
                '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
            logger.info(
                'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
            logger.info(
                'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
            logger.info(
                'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')

        while not self.username:
            prompt_for_username()

        while not self.password:
            self._get_password()

        # validate credentials
        while not self.is_valid_nda_credentials():
            logger.info(
                'Unable to authenticate your NDA account credentials with nda-tools. Please check your NDA account credentials.\n')
            self._use_keyring = False
            prompt_for_username()
            self._get_password()
        self._try_save_password_keyring()
        self._save_apis()

    def _save_apis(self):
        self.validation_api = ValidationV2Api(self.validation_api_endpoint, self.username, self.password)
        # self.force and self.hide_progress is only set in vtcmd
        hide_progress, force = False, False
        if hasattr(self, 'force'):
            force = self.force
        if hasattr(self, 'hide_progress'):
            hide_progress = self.hide_progress

        self.manifests_uploader = ManifestsUploader(self.validation_api,
                                                    self.workerThreads,
                                                    force,
                                                    hide_progress)

    def is_valid_nda_credentials(self):
        try:
            # will raise HTTP error 401 if invalid creds
            get_request(self.user_api_endpoint, headers={'content-type': 'application/json'},
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
                    exit_error()
                else:
                    return False
            else:
                logger.error('\nSystemError while checking credentials for user %s', self.username)
                logger.error('\nPlease contact NDAHelp@mail.nih.gov for help in resolving this error')
                exit_error()
