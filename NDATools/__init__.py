import NDATools
import requests
import json
import sys

__version__ = '0.2.10'
pypi_version = None
version_checked = False

def check_version():
    try:
        from packaging.version import parse
    except ImportError:
        from pip._vendor.packaging.version import parse
    # use https://test.pypi.org/pypi/{package}/json on test/release branches, use https://pypi.org on master
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

