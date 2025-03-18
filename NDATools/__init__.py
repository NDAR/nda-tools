from __future__ import print_function

import json
import os
import pathlib
import shutil
import sys

__version__ = '0.4.0'

from pkg_resources import resource_filename

pypi_version = None
initialization_complete = False
print('Running NDATools Version {}'.format(__version__))


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
    if not os.path.exists(NDA_ORGINIZATION_ROOT_FOLDER):
        os.mkdir(NDA_ORGINIZATION_ROOT_FOLDER)

    if not os.path.exists(NDA_TOOLS_ROOT_FOLDER):
        os.mkdir(NDA_TOOLS_ROOT_FOLDER)

    if not os.path.exists(NDA_TOOLS_VTCMD_FOLDER):
        os.mkdir(NDA_TOOLS_VTCMD_FOLDER)

    if not os.path.exists(NDA_TOOLS_DOWNLOADCMD_FOLDER):
        os.mkdir(NDA_TOOLS_DOWNLOADCMD_FOLDER)

    if not os.path.exists(NDA_TOOLS_DOWNLOADS_FOLDER):
        os.mkdir(NDA_TOOLS_DOWNLOADS_FOLDER)

    if not os.path.exists(NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER):
        os.mkdir(NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER)

    if not os.path.exists(NDA_TOOLS_VTCMD_LOGS_FOLDER):
        os.mkdir(NDA_TOOLS_VTCMD_LOGS_FOLDER)

    if not os.path.exists(NDA_TOOLS_VAL_FOLDER):
        os.mkdir(NDA_TOOLS_VAL_FOLDER)

    if not os.path.exists(NDA_TOOLS_SUB_PACKAGE_FOLDER):
        os.mkdir(NDA_TOOLS_SUB_PACKAGE_FOLDER)

    if not os.path.exists(NDA_TOOLS_SETTINGS_FOLDER):
        os.mkdir(NDA_TOOLS_SETTINGS_FOLDER)

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


def init_and_create_configuration(args, logs_folder, auth_req=True):
    from NDATools.Configuration import ClientConfiguration, LoggingConfiguration
    prerun_checks_and_setup()
    LoggingConfiguration.load_config(logs_folder, args.verbose, args.log_dir)
    config = ClientConfiguration(args)
    config.read_user_credentials(auth_req)
    return config
