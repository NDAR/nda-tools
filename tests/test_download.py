import pytest
from NDATools.Download import Download
from NDATools.Configuration import *


@pytest.fixture
def s3Download():
    config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
    dir = os.path.join(os.path.expanduser('~'), 'AWS_downloads')
    return Download(dir, config)


def test_datastructure_download(s3Download):
	links = 'datastructure'
	paths = ['/Users/ahmadus/Desktop/image03.txt']
	s3Download.get_links(links, paths, filters=None)
	assert len(s3Download.path_list) != 0

def test_test_file_download(s3Download):
	links = 'text'
	paths = ['/Users/ahmadus/Desktop/sampleLinks.txt']
	s3Download.get_links(links, paths, filters=None)
	assert len(s3Download.path_list) != 0
