import pytest

from cognite import config
from tests.conftest import TEST_API_KEY, TEST_PROJECT

MOCK_URL = 'http://another.url'
NUM_OF_RETRIES = 5


@pytest.fixture
def unset_config_variables():
    config.configure_session('', '')
    yield
    config.configure_session(TEST_API_KEY, TEST_PROJECT)


@pytest.fixture
def change_url():
    config.set_base_url(MOCK_URL)
    yield
    config.set_base_url()


@pytest.fixture
def change_number_of_retries():
    config.set_number_of_retries(NUM_OF_RETRIES)
    yield
    config.set_number_of_retries()


@pytest.mark.usefixtures('unset_config_variables')
def test_get_config_variables_when_not_set():
    result = config.get_config_variables(None, None)
    assert result == ('', '')


def test_get_config_variables_when_set():
    result = config.get_config_variables(None, None)
    assert result == (TEST_API_KEY, TEST_PROJECT)


def test_get_config_variables_when_set_explicitly():
    result = config.get_config_variables('some_other_key', 'some_other_project')
    assert result == ('some_other_key', 'some_other_project')


@pytest.mark.usefixtures('change_url')
def test_set_base_url():
    assert config.get_base_url(api_version=0.5) == MOCK_URL


@pytest.mark.usefixtures('change_number_of_retries')
def test_set_number_of_retries():
    assert config.get_number_of_retries() == NUM_OF_RETRIES
