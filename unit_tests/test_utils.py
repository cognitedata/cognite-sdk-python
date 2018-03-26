# -*- coding: utf-8 -*-
import unittest
import unittest.mock as mock

import cognite._utils as utils
import cognite.config as config
from unit_tests.test_config import configure_test_session, cleanup_test_session
from unit_tests.api_mock_responses import mock_response, assets_response


class UtilsTestCase(unittest.TestCase):
    ASSETS_URL = None

    def setUp(self):
        configure_test_session()
        api_key, project = config.get_config_variables(None, None)
        self.RANDOM_API_URL = config.get_base_url() + '/projects/{}/assets'.format(project)

    def test_get_request_ok(self):
        with mock.patch('cognite._utils.requests.get') as mock_request:
            mock_request.return_value = mock_response(json_data=assets_response)

            response = utils.get_request(self.ASSETS_URL)
            response_json = response.json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(response_json["data"]["items"]), len(assets_response))

    def test_get_request_failed(self):
        with mock.patch('cognite._utils.requests.get') as mock_request:
            mock_request.return_value = mock_response(status=400, json_data={"error": "Client error"})

            self.assertRaisesRegex(utils.APIError, "Client error[\n]X-Request_id", utils.get_request, self.ASSETS_URL)

            mock_request.return_value = mock_response(status=500, content="Server error")

            self.assertRaisesRegex(utils.APIError, "Server error[\n]X-Request_id", utils.get_request, self.ASSETS_URL)

            mock_request.return_value = mock_response(status=500, json_data={"error": "Server error"})

            self.assertRaisesRegex(utils.APIError, "Server error[\n]X-Request_id", utils.get_request, self.ASSETS_URL)

    def test_get_request_exception(self):
        with mock.patch('cognite._utils.requests.get') as mock_request:
            mock_request.return_value = mock_response(status=500)
            mock_request.side_effect = Exception("Custom error")

            self.assertRaisesRegex(utils.APIError, "Custom error", utils.get_request, self.ASSETS_URL)

    def tearDown(self):
        cleanup_test_session()
        self.RANDOM_API_URL = None


def suites():
    return [unittest.TestLoader().loadTestsFromTestCase(UtilsTestCase)]
