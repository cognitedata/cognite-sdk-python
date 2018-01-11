import unittest
from cognite.config import configure_session, get_config_variables

MOCK_API_KEY = "AN_API_KEY"
MOCK_PROJECT = 'A_PROJECT'

class ConfigTestCase(unittest.TestCase):

    def test_get_config_variables_when_not_set(self):
        result = get_config_variables(None, None)
        self.assertEquals(result, ('', ''))

    def test_get_config_variables_when_set(self):
        configure_session(MOCK_API_KEY, MOCK_PROJECT)
        result = get_config_variables(None, None)
        self.assertEquals(result, (MOCK_API_KEY, MOCK_PROJECT))

    def test_get_config_variables_when_set_explicitly(self):
        configure_session(MOCK_API_KEY, MOCK_PROJECT)
        result = get_config_variables('some_key', 'some_project')
        self.assertEquals(result, ('some_key', 'some_project'))

    def tearDown(self):
        configure_session('', '')

def suites():
    return [unittest.TestLoader().loadTestsFromTestCase(ConfigTestCase)]
