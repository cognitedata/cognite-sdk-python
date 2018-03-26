from cognite.config import configure_session, get_config_variables

MOCK_API_KEY = "AN_API_KEY"
MOCK_PROJECT = 'A_PROJECT'


def test_get_config_variables_when_not_set():
    result = get_config_variables(None, None)
    assert result == ('', '')


def test_get_config_variables_when_set():
    configure_session(MOCK_API_KEY, MOCK_PROJECT)
    result = get_config_variables(None, None)
    assert result == (MOCK_API_KEY, MOCK_PROJECT)


def test_get_config_variables_when_set_explicitly():
    configure_session(MOCK_API_KEY, MOCK_PROJECT)
    result = get_config_variables('some_key', 'some_project')
    assert result == ('some_key', 'some_project')
