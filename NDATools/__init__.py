from __future__ import print_function

import getpass
import json
import logging
import os
import pathlib
import shutil
import sys

__version__ = '0.5.0.dev9'

from typing import Tuple

from pkg_resources import resource_filename

from NDATools.Utils import exit_error, get_request, HttpErrorHandlingStrategy
from NDATools.upload.submission.api import UserApi

pypi_version = None
initialization_complete = False
print('Running NDATools Version {}'.format(__version__))

logger = logging.getLogger(__name__)

try:
    import keyring
except Exception as e:
    logger.debug(f'Error while importing keyring module: {str(e)}')
    keyring = None
SERVICE_NAME = 'nda-tools'
_use_keyring = True if keyring is not None else False


def check_version():
    global pypi_version, initialization_complete
    import requests
    try:
        from packaging.version import parse
    except ImportError:
        from pip._vendor.packaging.version import parse
    # use https://test.pypi.org/pypi/{package}/json on test/release branches, use https://pypi.org on master
    # Print Python 2 support dropping warning
    if sys.version_info < (3, 0):
        print()
        print('''WARNING - Detected Python version 2. Support for Python 2 is being removed from nda-tools. It is recommended to upgrade to the latest version of Python 
          before using the latest features in the downloadcmd''')

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


NDA_ORGINIZATION_ROOT_FOLDER = os.path.join(os.path.expanduser('~'), 'NDA')
NDA_TOOLS_ROOT_FOLDER = os.path.join(NDA_ORGINIZATION_ROOT_FOLDER, 'nda-tools')
NDA_TOOLS_VTCMD_FOLDER = os.path.join(NDA_TOOLS_ROOT_FOLDER, 'vtcmd')
NDA_TOOLS_DOWNLOADCMD_FOLDER = os.path.join(
    NDA_TOOLS_ROOT_FOLDER, 'downloadcmd')
NDA_TOOLS_DOWNLOADS_FOLDER = os.path.join(
    NDA_TOOLS_DOWNLOADCMD_FOLDER, 'packages')
NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = os.path.join(
    NDA_TOOLS_DOWNLOADCMD_FOLDER, 'logs')
NDA_TOOLS_VTCMD_LOGS_FOLDER = os.path.join(NDA_TOOLS_VTCMD_FOLDER, 'logs')
NDA_TOOLS_VAL_FOLDER = os.path.join(
    NDA_TOOLS_VTCMD_FOLDER, 'validation_results')
NDA_TOOLS_SUB_PACKAGE_FOLDER = os.path.join(
    NDA_TOOLS_VTCMD_FOLDER, 'submission_package')

NDA_TOOLS_PACKAGE_FILE_METADATA_TEMPLATE = 'package_file_metadata_%s.txt'
NDA_TOOLS_DEFAULT_LOG_FORMAT = '%(asctime)s:%(levelname)s:%(message)s'
NDA_TOOLS_SETTINGS_FOLDER = os.path.join(os.path.expanduser('~'), '.NDATools')
NDA_TOOLS_LOGGING_YML_FILE = os.path.join(NDA_TOOLS_SETTINGS_FOLDER, 'logging.yml')
NDA_TOOLS_SETTINGS_CFG_FILE = os.path.join(NDA_TOOLS_SETTINGS_FOLDER, 'settings.cfg')


def create_nda_folders():
    # init folder structure for program runtime files
    def _create_if_not_exists(path):
        if not os.path.exists(path):
            os.mkdir(path)

    _create_if_not_exists(NDA_ORGINIZATION_ROOT_FOLDER)
    _create_if_not_exists(NDA_TOOLS_ROOT_FOLDER)
    _create_if_not_exists(NDA_TOOLS_VTCMD_FOLDER)
    _create_if_not_exists(NDA_TOOLS_DOWNLOADCMD_FOLDER)
    _create_if_not_exists(NDA_TOOLS_DOWNLOADS_FOLDER)
    _create_if_not_exists(NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER)
    _create_if_not_exists(NDA_TOOLS_VTCMD_LOGS_FOLDER)
    _create_if_not_exists(NDA_TOOLS_VAL_FOLDER)
    _create_if_not_exists(NDA_TOOLS_SUB_PACKAGE_FOLDER)
    _create_if_not_exists(NDA_TOOLS_SETTINGS_FOLDER)

    if not pathlib.Path(NDA_TOOLS_LOGGING_YML_FILE).is_file():
        shutil.copyfile(resource_filename(__name__, 'clientscripts/config/logging.yml'),
                        NDA_TOOLS_LOGGING_YML_FILE)

    if not pathlib.Path(NDA_TOOLS_SETTINGS_CFG_FILE).is_file():
        shutil.copyfile(resource_filename(__name__, 'clientscripts/config/settings.cfg'),
                        NDA_TOOLS_SETTINGS_CFG_FILE)
    # MAC users sometimes see output from python warnings module. Suppress these msgs
    os.environ['PYTHONWARNINGS'] = 'ignore'


def prerun_checks_and_setup():
    check_version()
    create_nda_folders()


def _get_password(username) -> str:
    global _use_keyring
    try:
        if _use_keyring:
            password = keyring.get_password(SERVICE_NAME, username)
            if not password:
                logger.debug('no password found in keyring')
                _use_keyring = False
                return _get_password(username)
            logger.debug('retrieved password from keyring')
            return password
        else:
            return getpass.getpass('Enter your NDA account password:')
    except Exception as e:
        logger.warning(f'could not retrieve password from keyring: {str(e)}')
        _use_keyring = False
        return _get_password(username)


def _try_save_password_keyring(username, password):
    try:
        if _use_keyring:
            keyring.set_password(SERVICE_NAME, username, password)
    except Exception as e:
        logger.warning(f'could not save password to keyring: {str(e)}')


def _get_user_credentials(config) -> Tuple[str, str]:
    # username is fetched from settings.cfg, and it is not present at the first time use of nda-tools
    # display NDA account instructions
    global _use_keyring
    if not config.username:
        logger.info(
            '\nPlease use your NIMH Data Archive (NDA) account credentials to authenticate with nda-tools')
        logger.info(
            'You may already have an existing eRA commons account or a login.gov account, this is different from your NDA account')
        logger.info(
            'You may retrieve your NDA account info by logging into https://nda.nih.gov/user/dashboard/profile.html using your eRA commons account or login.gov account')
        logger.info(
            'Once you are logged into your profile page, you can find your NDA account username. For password retrieval, click UPDATE/RESET PASSWORD button')
    get_username = lambda: str(input('Enter your NDA account username:')).lower()
    username = config.username
    while not username:
        username = get_username()

    password = config.password
    while not password:
        password = _get_password(username)

    # validate credentials
    api = UserApi(config.user_api_endpoint)
    while not api.is_valid_nda_credentials(username, password):
        logger.info(
            'Unable to authenticate your NDA account credentials with nda-tools. Please check your NDA account credentials.\n')
        _use_keyring = False
        username = get_username()
        _get_password(username)
    _try_save_password_keyring(username, password)
    return username, password


def init_and_create_configuration(args, logs_folder, auth_req=True):
    from NDATools.Configuration import ClientConfiguration, LoggingConfiguration
    prerun_checks_and_setup()
    LoggingConfiguration.load_config(logs_folder, args.verbose, args.log_dir)
    config = ClientConfiguration(args)
    if auth_req:
        authenticate(config)
    return config


def authenticate(config):
    username, password = _get_user_credentials(config)
    config.update_with_auth(username, password)
    return config
