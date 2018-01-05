import unittest

from cognite.config import configure_session, get_config_variables
from unit_tests.config import TEST_API_KEY, TEST_PROJECT

class ConfigTestCase(unittest.TestCase):

    def testget_config_variables_when_not_set(self):
        result = get_config_variables(None, None)
        self.assertEquals(result, ('', ''))

    def testget_config_variables_when_set(self):
        configure_session(TEST_API_KEY, TEST_PROJECT)
        result = get_config_variables(None, None)
        self.assertEquals(result, (TEST_API_KEY, TEST_PROJECT))

    def testget_config_variables_when_set_explicitly(self):
        configure_session(TEST_API_KEY, TEST_PROJECT)
        result = get_config_variables('some_key', 'some_project')
        self.assertEquals(result, ('some_key', 'some_project'))

    def tearDown(self):
        configure_session('', '')

def suites():
    return [unittest.TestLoader().loadTestsFromTestCase(ConfigTestCase)]

