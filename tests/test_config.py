import pytest

from cognite import config
from tests.conftest import MOCK_API_KEY, MOCK_PROJECT


def test_get_config_variables_when_not_set():
    result = config.get_config_variables(None, None)
    assert result == ('', '')


@pytest.mark.usefixtures('configure_test_session')
def test_get_config_variables_when_set():
    result = config.get_config_variables(None, None)
    assert result == (MOCK_API_KEY, MOCK_PROJECT)


@pytest.mark.usefixtures('configure_test_session')
def test_get_config_variables_when_set_explicitly():
    result = config.get_config_variables('some_other_key', 'some_other_project')
    assert result == ('some_other_key', 'some_other_project')


def test_set_base_url():
    new_url = 'another_url'
    config.set_base_url(new_url)
    assert config.get_base_url() == new_url


def test_set_number_of_retries():
    retries = 5
    config.set_number_of_retries(retries)
    assert config.get_number_of_retries() == retries
