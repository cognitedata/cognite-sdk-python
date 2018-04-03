from cognite import config
from tests.conftest import TEST_API_KEY, TEST_PROJECT


def test_get_config_variables_when_not_set():
    config.configure_session('', '')
    result = config.get_config_variables(None, None)
    assert result == ('', '')
    config.configure_session(TEST_API_KEY, TEST_PROJECT)


def test_get_config_variables_when_set():
    result = config.get_config_variables(None, None)
    assert result == (TEST_API_KEY, TEST_PROJECT)


def test_get_config_variables_when_set_explicitly():
    result = config.get_config_variables('some_other_key', 'some_other_project')
    assert result == ('some_other_key', 'some_other_project')


def test_set_base_url():
    new_url = 'another_url'
    config.set_base_url(new_url)
    assert config.get_base_url() == new_url
    config.set_base_url(None)


def test_set_number_of_retries():
    retries = 5
    config.set_number_of_retries(retries)
    assert config.get_number_of_retries() == retries
