import configparser
import logging
import logging.config
import multiprocessing
import os
import time

import yaml
from pkg_resources import resource_filename

import NDATools
from NDATools import NDA_TOOLS_LOGGING_YML_FILE
from NDATools.upload.cli import NdaUploadCli
from NDATools.upload.submission.api import SubmissionPackageApi, SubmissionApi, CollectionApi
from NDATools.upload.submission.associated_file import AssociatedFileUploader
from NDATools.upload.validation.api import ValidationV2Api
from NDATools.upload.validation.manifests import ManifestFileUploader
from NDATools.upload.validation.results_writer import ResultsWriterFactory

logger = logging.getLogger(__name__)


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

    def __init__(self, args):
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
        # TODO remove args from config
        self._args = args

        if args.username:
            self.username = args.username
            logger.info('proceeding as NDA user: {}'.format(self.username))
        elif self.username:
            logger.warning("-u/--username argument not provided. Using default value of '%s' which was saved in %s",
                           self.username, NDATools.NDA_TOOLS_SETTINGS_CFG_FILE)
        self.password = None

        if self._is_vtcmd():
            self.v2_enabled = False
            self.validation_results_writer = ResultsWriterFactory.get_writer(file_format='json' if args.JSON else 'csv')
            self.validation_api = None
            self.submission_api = None
            self.submission_package_api = None
            self.collection_api = None
            self.manifests_uploader = None
            self.associated_files_uploader = None
            self.upload_cli = NdaUploadCli(self)

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

    @property
    def worker_threads(self):
        # default value between 1 and 20, based on cpu_count
        default_value = min(max([1, multiprocessing.cpu_count() - 1]), 20)
        return self._args.workerThreads or default_value

    @property
    def batch_size(self):
        return self._args.batch

    def _is_vtcmd(self):
        return 'collectionID' in self._args

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

    def update_with_auth(self, username, password):
        self.username = username
        self.password = password
        self._save_username()
        self._save_apis()

    def _save_username(self):
        with open(NDATools.NDA_TOOLS_SETTINGS_CFG_FILE, 'w') as configfile:
            self.config.set('User', 'username', self.username)
            self.config.write(configfile)

    def _save_apis(self):
        self.validation_api = ValidationV2Api(self.validation_api_endpoint, self.username, self.password)
        self.submission_package_api = SubmissionPackageApi(self.submission_package_api_endpoint,
                                                           self.username,
                                                           self.password)
        self.submission_api = SubmissionApi(self.submission_api_endpoint, self.username,
                                            self.password)
        self.collection_api = CollectionApi(self.validationtool_api_endpoint, self.username, self.password)

        if self._is_vtcmd():
            self.manifests_uploader = ManifestFileUploader(self.validation_api,
                                                           self.worker_threads,
                                                           self.force,
                                                           self.hide_progress)
            self.associated_files_uploader = AssociatedFileUploader(self.submission_api,
                                                                    self.worker_threads,
                                                                    self.force,
                                                                    self.hide_progress,
                                                                    self.batch_size)
