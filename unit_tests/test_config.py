import unittest

from cognite.config import configure_session, _get_config_variables

class ConfigTestCase(unittest.TestCase):

    def test_get_config_variables_when_not_set(self):
        result = _get_config_variables(None, None)
        self.assertEquals(result, ('', ''))

    def test_get_config_variables_when_set(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        result = _get_config_variables(None, None)
        self.assertEquals(result, ('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp'))

    def test_get_config_variables_when_set_explicitly(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        result = _get_config_variables('some_key', 'some_project')
        self.assertEquals(result, ('some_key', 'some_project'))

    def tearDown(self):
        configure_session('', '')

def suites():
    return [unittest.TestLoader().loadTestsFromTestCase(ConfigTestCase)]

