from __future__ import print_function

import json
import os
import sys

import requests

import NDATools

__version__ = '0.2.22'
pypi_version = None
version_checked = False


def check_version():
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
    print('Running NDATools Version {}'.format(NDATools.__version__))
    if parse(NDATools.__version__).is_devrelease:
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
    NDATools.pypi_version = str(version)

    if parse(NDATools.__version__) < parse(NDATools.pypi_version):
        print("Your version of nda-tools is out of date. Please upgrade to the latest version ({}) from PyPi or GitHub and "
              "try again. \n\tTo upgrade using pip, run: \r\npip install nda-tools=={}".format(NDATools.pypi_version, NDATools.pypi_version))
        sys.exit(1)

    NDATools.version_checked = True

if not NDATools.version_checked:
    check_version()

# init folder structure for program runtime files
NDA_ORGINIZATION_ROOT_FOLDER = os.path.join(os.path.expanduser('~'), 'NDA')
if not os.path.exists(NDA_ORGINIZATION_ROOT_FOLDER):
    os.mkdir(NDA_ORGINIZATION_ROOT_FOLDER)

NDA_TOOLS_ROOT_FOLDER = os.path.join(NDA_ORGINIZATION_ROOT_FOLDER, 'nda-tools')
if not os.path.exists(NDA_TOOLS_ROOT_FOLDER):
    os.mkdir(NDA_TOOLS_ROOT_FOLDER)

NDA_TOOLS_VTCMD_FOLDER = os.path.join(NDA_TOOLS_ROOT_FOLDER, 'vtcmd')
if not os.path.exists(NDA_TOOLS_VTCMD_FOLDER):
    os.mkdir(NDA_TOOLS_VTCMD_FOLDER)

NDA_TOOLS_DOWNLOADCMD_FOLDER = os.path.join(NDA_TOOLS_ROOT_FOLDER, 'downloadcmd')
if not os.path.exists(NDA_TOOLS_DOWNLOADCMD_FOLDER):
    os.mkdir(NDA_TOOLS_DOWNLOADCMD_FOLDER)


NDA_TOOLS_DOWNLOADS_FOLDER = os.path.join(NDA_TOOLS_DOWNLOADCMD_FOLDER, 'packages')
if not os.path.exists(NDA_TOOLS_DOWNLOADS_FOLDER):
    os.mkdir(NDA_TOOLS_DOWNLOADS_FOLDER)

NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER = os.path.join(NDA_TOOLS_DOWNLOADCMD_FOLDER, 'logs')
if not os.path.exists(NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER):
    os.mkdir(NDA_TOOLS_DOWNLOADCMD_LOGS_FOLDER)

NDA_TOOLS_VTCMD_LOGS_FOLDER = os.path.join(NDA_TOOLS_VTCMD_FOLDER, 'logs')
if not os.path.exists(NDA_TOOLS_VTCMD_LOGS_FOLDER):
    os.mkdir(NDA_TOOLS_VTCMD_LOGS_FOLDER)

NDA_TOOLS_VAL_FOLDER = os.path.join(NDA_TOOLS_VTCMD_FOLDER, 'validation_results')
if not os.path.exists(NDA_TOOLS_VAL_FOLDER):
    os.mkdir(NDA_TOOLS_VAL_FOLDER)

NDA_TOOLS_SUB_PACKAGE_FOLDER = os.path.join(NDA_TOOLS_VTCMD_FOLDER, 'submission_package')
if not os.path.exists(NDA_TOOLS_SUB_PACKAGE_FOLDER):
    os.mkdir(NDA_TOOLS_SUB_PACKAGE_FOLDER)

NDA_TOOLS_DEFAULT_LOG_FORMAT='%(asctime)s:%(levelname)s:%(message)s'

NDA_TOOLS_LOGGING_YML_FILE = os.path.join(os.path.expanduser('~'), '.NDATools/logging.yml')

os.environ['PYTHONWARNINGS']='ignore' # MAC users sometimes see output from python warnings module. Suppress these msgs
