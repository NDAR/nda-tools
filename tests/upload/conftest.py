import pytest


@pytest.fixture
def config(top_level_datadir, validation_config_factory):
    file_path = (top_level_datadir / 'validation/file.csv')
    test_args = [str(file_path)]
    _, config = validation_config_factory(test_args)
    return config
