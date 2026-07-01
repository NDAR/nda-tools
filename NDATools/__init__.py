from __future__ import print_function

import getpass
import importlib.resources
import json
import logging
import os
import pathlib
import shutil
import sys

__version__ = '0.8.dev7'

import threading
from importlib.resources import files

from typing import Tuple

pypi_version = None
initialization_complete = False
print("\n================================================================================")
print('\n                  Running NDA Tools Version {}'.format(__version__))
print("\n================================================================================")

logger = logging.getLogger(__name__)

try:
    import keyring
except Exception as e:
    logger.debug(f'Error while importing keyring module: {str(e)}')
    keyring = None
SERVICE_NAME = 'nda-tools'
_get_keyring = True if keyring is not None else False
_set_keyring = True if keyring is not None else False


def check_version():
    global pypi_version, initialization_complete
    import requests
    try:
        from packaging.version import parse
    except ImportError:
        from pip._vendor.packaging.version import parse
    # use https://test.pypi.org/pypi/{package}/json on test/release branches, use https://pypi.org on master

    if parse(__version__).is_devrelease:
        return
    url_pattern = 'https://pypi.org/pypi/{package}/json'
    package = 'nda-tools'
    """Return version of package on pypi.python.org using json."""
    req = requests.get(url_pattern.format(package=package))
    version = parse('0')
    if req.status_code == requests.codes.ok:
        j = json.loads(req.text)
        releases = j.get('releases', [])
        for release in releases:
            ver = parse(release)
            if not ver.is_prerelease:
                version = max(version, ver)
    pypi_version = str(version)

    if parse(__version__) < parse(pypi_version):
        print(
            "Your version of nda-tools is out of date. Please upgrade to the latest version ({}) from PyPi or GitHub and "
            "try again. \n\tTo upgrade using pip, run: \r\npip install nda-tools=={}".format(pypi_version,
                                                                                             pypi_version))
        sys.exit(1)

NDA_TOOLS_PACKAGE_FILE_METADATA_TEMPLATE = 'package_file_metadata_%s.txt'
NDA_TOOLS_DEFAULT_LOG_FORMAT = '%(asctime)s:%(levelname)s:%(message)s'

def create_nda_folders(nda_paths):
    # init folder structure for program runtime files
    def _create_if_not_exists(path):
        if not os.path.exists(path):
            os.mkdir(path)

    for path in nda_paths.values():
        _create_if_not_exists(path)

    if not pathlib.Path(nda_paths['nda_tools_logging_yml_file']).is_file():
        t = files('NDATools').joinpath('clientscripts/config/logging.yml')
        with importlib.resources.as_file(t) as f:
            shutil.copyfile(f, nda_paths['nda_tools_logging_yml_file'])

    if not pathlib.Path(nda_paths['nda_tools_settings_cfg_file']).is_file():
        t = files('NDATools').joinpath('clientscripts/config/settings.cfg')
        with importlib.resources.as_file(t) as f:
            shutil.copyfile(f, nda_paths['nda_tools_settings_cfg_file'])
    # MAC users sometimes see output from python warnings module. Suppress these msgs
    os.environ['PYTHONWARNINGS'] = 'ignore'


def _get_password(username) -> str:
    global _get_keyring
    try:
        if _get_keyring:
            password = keyring.get_password(SERVICE_NAME, username)
            if not password:
                logger.debug('no password found in keyring')
                _get_keyring = False
                return _get_password(username)
            logger.debug('retrieved password from keyring')
            return password
        else:
            return getpass.getpass('Enter your NDA account password:')
    except Exception as e:
        logger.warning(f'could not retrieve password from keyring: {str(e)}')
        _get_keyring = False
        return _get_password(username)


def _try_save_password_keyring(username, password):
    global _set_keyring
    try:
        if _set_keyring:
            keyring.set_password(SERVICE_NAME, username, password)
    except Exception as e:
        logger.warning(f'could not save password to keyring: {str(e)}')


def get_username():
    return str(input('Enter your NDA account username:')).lower().strip()


def _get_user_credentials(config) -> Tuple[str, str]:
    # Adding NDATools dependencies to the start of __init__ can cause errors during installation, so keep import here.
    from NDATools.upload.submission.api import RasAuthApi
    # username is fetched from settings.cfg, and it is not present at the first time use of nda-tools
    # display NDA account instructions
    global _get_keyring
    if not config.username:
        logger.info(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        logger.info(
            'You may already have an existing account (eRA Commons, Login.gov, or Smart Card/CAC), this is different from your NDA account')
        logger.info(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your RAS credentials (eRA Commons, Login.gov, or Smart Card/CAC)')
        logger.info(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')
    username = config.username
    while not username:
        username = get_username()

    password = config.password
    while not password:
        password = _get_password(username)

    # validate credentials and obtain token
    api = RasAuthApi(config.ras_login_api_endpoint)
    token = api.login(username, password)
    while not token:
        logger.info('Username/password combination is incorrect')
        _get_keyring = False
        username = get_username()
        password = _get_password(username)
        token = api.login(username, password)
    _try_save_password_keyring(username, password)
    return username, password, token


def auth_config_and_init_logging(command, args, config, auth_req=True):
    if auth_req:
        config.authenticate()
    check_version_and_create_folders(config.nda_paths)

    from NDATools.Configuration import LoggingConfiguration
    if command == 'vtcmd':
        LoggingConfiguration.load_config(config.nda_paths['nda_tools_logging_yml_file'], config.nda_paths['nda_tools_vtcmd_logs_folder'], args.verbose, args.log_dir)
    if command == 'downloadcmd':
        LoggingConfiguration.load_config(config.nda_paths['nda_tools_logging_yml_file'], config.nda_paths['nda_tools_downloadcmd_logs_folder'], args.verbose, args.log_dir)
    if command == 'nda':
        LoggingConfiguration.load_config(config.nda_paths['nda_tools_logging_yml_file'], config.nda_paths['nda_tools_nda_logs_folder'], args.verbose, args.log_dir)


def check_version_and_create_folders(nda_paths):
    check_version()
    create_nda_folders(nda_paths)


def _exit_client(message=None, status_code=1):
    for t in threading.enumerate():
        try:
            t.shutdown_flag.set()
        except AttributeError:
            continue
    if message:
        logger.info('\n\n{}'.format(message))
    os._exit(status_code)


def exit_error(message=None):
    _exit_client(message, status_code=1)


def exit_normal(message=None):
    _exit_client(message, status_code=0)
