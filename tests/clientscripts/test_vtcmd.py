from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.clientscripts.vtcmd import validate


@pytest.fixture
def args():
    return MagicMock()


@pytest.fixture
def config():
    tmp = MagicMock()
    tmp.is_authenticated = MagicMock(return_value=True)
    return tmp


@pytest.fixture
def pending_changes():
    return MagicMock()


@pytest.fixture
def original_uuids():
    return MagicMock()


@pytest.fixture
def validation_api_config():
    return {
        'v2Routing': {
            'percent': 100
        }
    }


def test_validate_routing(args, config, pending_changes, original_uuids, validation_api_config, monkeypatch):
    """Test that the mechanism for routing requests to v1 and v2 works as expected"""
    with monkeypatch.context() as m:
        m.setattr(NDATools.clientscripts.vtcmd, "get_request", MagicMock(return_value=validation_api_config))
        m.setattr(NDATools.clientscripts.vtcmd, "validate_v2", MagicMock(return_value=[]))
        m.setattr(NDATools.clientscripts.vtcmd, "validate_v1", MagicMock(return_value=[['uuid1', 'uuid2']]))
        validate(args, config, pending_changes, original_uuids)
        NDATools.clientscripts.vtcmd.validate_v2.assert_called_once()
        NDATools.clientscripts.vtcmd.validate_v1.assert_not_called()

        NDATools.clientscripts.vtcmd.validate_v2.reset_mock()
        NDATools.clientscripts.vtcmd.validate_v1.reset_mock()

        # update the percent to 0 to test v1 routing
        validation_api_config['v2Routing']['percent'] = 0
        validate(args, config, pending_changes, original_uuids)
        NDATools.clientscripts.vtcmd.validate_v1.assert_called_once()
        NDATools.clientscripts.vtcmd.validate_v2.assert_not_called()
