import pytest
from NDATools.Configuration import *


# create a new config file in user's home directory if one does not exist

def test_config_file():
    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', username='testusername', password='testpassword',
        access_key='testkey', secret_key='testsecretkey')

    config.make_config()


    assert os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
