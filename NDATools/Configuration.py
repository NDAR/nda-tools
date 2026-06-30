import configparser
import importlib
import logging
import logging.config
import multiprocessing
import os
import threading
import time
from requests.auth import AuthBase

import yaml

import NDATools
from NDATools.Utils import REQUEST_TOKEN_VERSION_ATTR
from NDATools.upload.cli import NdaUploadCli
from NDATools.upload.submission.api import SubmissionPackageApi, SubmissionApi, CollectionApi
from NDATools.upload.submission.associated_file import AssociatedFileUploader
from NDATools.upload.validation.api import ValidationV2Api
from NDATools.upload.validation.manifests import ManifestFileUploader
from NDATools.upload.validation.results_writer import ResultsWriterFactory

logger = logging.getLogger(__name__)
from importlib.resources import files
from pathlib import Path


class LoggingConfiguration:

    def __init__(self):
        pass

    @staticmethod
    def load_config(logging_yml_file, default_log_directory, verbose=False, log_dir=None):

        with open(logging_yml_file, 'r') as stream:
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


class DynamicBearerAuth(AuthBase):
    def __init__(self, config):
        self._config = config

    def __call__(self, request):
        setattr(request, REQUEST_TOKEN_VERSION_ATTR, self._config.get_auth_generation())
        request.headers['Authorization'] = f'Bearer {self._config.token}'
        return request


class ClientConfiguration:

    def __init__(self, args):
        self.config = configparser.ConfigParser()
        self._nda_paths = self._set_nda_paths()

        self._check_and_fix_missing_options()
        self.validation_api_endpoint = self.config.get("Endpoints", "validation")
        self.submission_package_api_endpoint = self.config.get("Endpoints", "submission_package")
        self.submission_api_endpoint = self.config.get("Endpoints", "submission")
        self.validationtool_api_endpoint = self.config.get("Endpoints", "validationtool")
        self.package_creation_api_endpoint = self.config.get("Endpoints", "package_creation")
        self.package_api_endpoint = self.config.get("Endpoints", "package")
        self.datadictionary_api_endpoint = self.config.get("Endpoints", "datadictionary")
        self.collection_api_endpoint = self.config.get("Endpoints", "collection")
        ras_api_endpoint = self.config.get("Endpoints", "ras")
        self.ras_login_api_endpoint = f"{ras_api_endpoint}/user/login"
        self.username = self.config.get("User", "username").lower()

        # TODO remove args from config
        self._args = args

        if args.username:
            self.username = args.username
            logger.info('proceeding as NDA user: {}'.format(self.username))
        elif self.username:
            logger.warning("-u/--username argument not provided. Using default value of '%s' which was saved in %s",
                           self.username, self._nda_paths['nda_tools_settings_cfg_file'])
        self.password = None
        self.token = None
        self._auth_generation = 0
        self._reauth_lock = threading.Lock()
        self._reauth_condition = threading.Condition(self._reauth_lock)
        self._reauth_in_progress = False
        self._reauth_error = None
        self._auth = DynamicBearerAuth(self)

        if self._is_vtcmd():
            self.qa_enabled = True
            self.validation_results_writer = ResultsWriterFactory(self.nda_paths['nda_tools_val_folder']).get_writer(file_format='json' if args.JSON else 'csv')
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

    @property
    def nda_paths(self):
        return self._nda_paths

    def _is_vtcmd(self):
        return 'collectionID' in self._args

    def _check_and_fix_missing_options(self):
        default_config = configparser.ConfigParser()
        t = files('NDATools').joinpath('clientscripts/config/settings.cfg')
        with importlib.resources.as_file(t) as f:
            default_config.read(f)
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
            logger.debug('updating settings.cfg')
            with open(self._nda_paths['nda_tools_settings_cfg_file'], 'w') as configfile:
                self.config.write(configfile)
        else:
            logger.debug('settings.cfg is up to date')

    def is_authenticated(self):
        return self.username and self.password

    def get_auth(self):
        return self._auth

    def get_auth_generation(self):
        return self._auth_generation

    def reauthenticate(self, token_version=None):
        """Refresh the shared bearer token once for a given failed request version.

        ``token_version`` is the auth generation used by the request that got a 401.
        If another thread already refreshed the token and advanced the current
        generation, that 401 is stale and no new login is needed. Otherwise, one
        thread performs the login while the others wait and reuse the result.
        """
        with self._reauth_condition:
            # A newer token is already available, so this 401 came from a stale request.
            if token_version is not None and token_version < self._auth_generation:
                return
            while self._reauth_in_progress:
                # Another thread is already logging in; wait for its result.
                self._reauth_condition.wait()
                if token_version is not None and token_version < self._auth_generation:
                    return
                if self._reauth_error is not None:
                    raise self._reauth_error
            if token_version is not None and token_version < self._auth_generation:
                return
            self._reauth_in_progress = True
            self._reauth_error = None

        try:
            self.authenticate()
            with self._reauth_condition:
                self._auth_generation += 1
        except Exception as exc:
            with self._reauth_condition:
                self._reauth_error = exc
            raise
        finally:
            with self._reauth_condition:
                # Wake all waiters so they can either reuse the new token or see the failure.
                self._reauth_in_progress = False
                self._reauth_condition.notify_all()

    def authenticate(self):
        username, password, token = NDATools._get_user_credentials(self)
        self.update_with_auth(username, password, token)

    def update_with_auth(self, username, password, token):
        self.username = username
        self.password = password
        self.token = token
        self._save_username()
        self._save_apis()

    def _save_username(self):
        with open(self._nda_paths['nda_tools_settings_cfg_file'], 'w') as configfile:
            self.config.set('User', 'username', self.username)
            self.config.write(configfile)

    def _save_apis(self):
        self.validation_api = ValidationV2Api(self.validation_api_endpoint, auth=self.get_auth(),
                                              reauth_func=self.reauthenticate)
        self.submission_package_api = SubmissionPackageApi(self.submission_package_api_endpoint,
                                                           auth=self.get_auth(),
                                                           reauth_func=self.reauthenticate)
        self.submission_api = SubmissionApi(self.submission_api_endpoint, auth=self.get_auth(),
                                            reauth_func=self.reauthenticate)
        self.collection_api = CollectionApi(self.validationtool_api_endpoint, auth=self.get_auth(),
                                            reauth_func=self.reauthenticate)

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

    def _set_nda_paths(self):
        nda_tools_settings_folder = os.path.join(os.path.expanduser('~'), '.NDATools')
        nda_tools_settings_cfg_file = os.path.join(nda_tools_settings_folder, 'settings.cfg')

        logger.info('Using configuration file from {}'.format(nda_tools_settings_cfg_file))
        self.config.read(nda_tools_settings_cfg_file)

        nda_org_root_dir = self._validate_folder_path(
            self.config.get("Paths", "nda_organization_root_dir"))
        if not nda_org_root_dir:
            nda_org_root_dir = os.path.join(os.path.expanduser('~'), 'NDA')

        nda_tools_root_folder = os.path.join(nda_org_root_dir, 'nda-tools')
        nda_tools_vtcmd_folder = os.path.join(nda_tools_root_folder, 'vtcmd')
        nda_tools_downloadcmd_folder = os.path.join(nda_tools_root_folder, 'downloadcmd')
        nda_tools_nda_folder = os.path.join(nda_tools_root_folder, 'nda')

        return {
            "nda_organization_root_folder": nda_org_root_dir,
            "nda_tools_root_folder": nda_tools_root_folder,
            "nda_tools_vtcmd_folder": nda_tools_vtcmd_folder,
            "nda_tools_nda_folder": nda_tools_nda_folder,
            "nda_tools_downloadcmd_folder": nda_tools_downloadcmd_folder,
            "nda_tools_downloads_folder": os.path.join(nda_tools_downloadcmd_folder, 'packages'),
            "nda_tools_downloadcmd_logs_folder": os.path.join(nda_tools_downloadcmd_folder, 'logs'),
            "nda_tools_vtcmd_logs_folder": os.path.join(nda_tools_vtcmd_folder, 'logs'),
            "nda_tools_val_folder": os.path.join(nda_tools_vtcmd_folder, 'validation_results'),
            "nda_tools_sub_pkg_folder": os.path.join(nda_tools_vtcmd_folder, 'submission_package'),
            "nda_tools_submissions_folder":  os.path.join(nda_tools_vtcmd_folder, 'submissions'),
            "nda_tools_nda_logs_folder": os.path.join(nda_tools_nda_folder, 'logs'),
            "nda_tools_settings_folder": nda_tools_settings_folder,
            "nda_tools_logging_yml_file": os.path.join(nda_tools_settings_folder, 'logging.yml'),
            "nda_tools_settings_cfg_file": nda_tools_settings_cfg_file
        }

    def _validate_folder_path(self, directory=None):
        if not directory or not directory.strip():
            return None
        else:
            directory = Path(os.path.expandvars(os.path.expanduser(directory.strip()))).resolve()
            if not os.path.isdir(directory):
                return None
            return directory
