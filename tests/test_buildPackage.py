import pytest
from NDATools.Configuration import *
from NDATools.Validation import Validation
from NDATools.BuildPackage import SubmissionPackage

@pytest.fixture
def Package():
    config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
    return SubmissionPackage(uuid=['7d378acc-4d2f-4b22-9327-963145216030'], associated_files=None, config= config, title='test',
                 description='unit test', collection='1860', allow_exit=False)

    #3551f670-3d5a-44ba-9e95-d902ec65e4e7

def test_buildPackage(Package):

    #print(package.package_id, package.submission_package_uuid)
    Package.build_package()

    assert Package.submission_package_uuid is not None #package.submission_package_uuid == '7d378acc-4d2f-4b22-9327-963145216030'


# how to assign Package.associated_files -- this comes from Validation
"""
def test_file_search(Package):
    #Package.build_package()
    Package.file_search(directories=['/Users/ahmadus/Documents/Client/testdata/with_associated_files/sample_genomics_files'])

    assert len(Package.no_match) == 0
"""

def test_get_collections(Package):
    Package.get_collections()

    assert Package.collections is not {}

# def test_download_package(Package): ???