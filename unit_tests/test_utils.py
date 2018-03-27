# -*- coding: utf-8 -*-
import gzip
import json
import unittest
import unittest.mock as mock

import cognite._utils as utils
import cognite.config as config
from unit_tests.test_config import configure_test_session, cleanup_test_session
from unit_tests.api_mock_responses import mock_response, assets_response


class RequestsTestCase(unittest.TestCase):
    ASSETS_URL = None

    def setUp(self):
        configure_test_session()
        api_key, project = config.get_config_variables(None, None)
        self.ASSETS_URL = config.get_base_url() + '/projects/{}/assets'.format(project)

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

    def test_post_request_ok(self):
        with mock.patch('cognite._utils.requests.post') as mock_request:
            mock_request.return_value = mock_response(json_data=assets_response)

            response = utils.post_request(self.ASSETS_URL, assets_response)
            response_json = response.json()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(response_json["data"]["items"]), len(assets_response))

    def test_post_request_failed(self):
        with mock.patch('cognite._utils.requests.post') as mock_request:
            mock_request.return_value = mock_response(status=400, json_data={"error": "Client error"})

            self.assertRaisesRegex(utils.APIError, "Client error[\n]X-Request_id",
                                   utils.post_request, self.ASSETS_URL, assets_response)

            mock_request.return_value = mock_response(status=500, content="Server error")

            self.assertRaisesRegex(utils.APIError, "Server error[\n]X-Request_id",
                                   utils.post_request, self.ASSETS_URL, assets_response)

            mock_request.return_value = mock_response(status=500, json_data={"error": "Server error"})

            self.assertRaisesRegex(utils.APIError, "Server error[\n]X-Request_id",
                                   utils.post_request, self.ASSETS_URL, assets_response)

    def test_post_request_exception(self):
        with mock.patch('cognite._utils.requests.post') as mock_request:
            mock_request.return_value = mock_response(status=500)
            mock_request.side_effect = Exception("Custom error")

            self.assertRaisesRegex(utils.APIError, "Custom error",
                                   utils.post_request, self.ASSETS_URL, assets_response)

    def test_post_request_args(self):
        with mock.patch('cognite._utils.requests.post') as mock_request:

            def check_args_to_post_and_return_mock(url, data=None, headers=None, params=None, cookies=None):
                # URL is sent as is
                self.assertEqual(url, self.ASSETS_URL)
                # cookies should be the same
                self.assertEqual(cookies, {"a-cookie": "a-cookie-val"})
                # gzip is added as Content-Encoding header
                self.assertEqual(headers["Content-Encoding"], "gzip")
                # data is gzipped. Decompress and check if items size matches
                decompressed_assets = json.loads(gzip.decompress(data))
                self.assertEqual(len(decompressed_assets["data"]["items"]), len(assets_response))
                # Return the mock response
                return mock_response(json_data=assets_response)
            mock_request.side_effect = check_args_to_post_and_return_mock

            response = utils.post_request(
                self.ASSETS_URL, assets_response,
                headers={"Existing-Header": "SomeValue"},
                cookies={"a-cookie": "a-cookie-val"},
                use_gzip=True)

            self.assertEqual(response.status_code, 200)

    def test_post_request_gzip(self):
        with mock.patch('cognite._utils.requests.post') as mock_request:

            def check_gzip_enabled_and_return_mock(url, data=None, headers=None, params=None, cookies=None):
                # gzip is added as Content-Encoding header
                self.assertEqual(headers["Content-Encoding"], "gzip")
                # data is gzipped. Decompress and check if items size matches
                decompressed_assets = json.loads(gzip.decompress(data))
                self.assertEqual(len(decompressed_assets["data"]["items"]), len(assets_response))
                # Return the mock response
                return mock_response(json_data=assets_response)
            mock_request.side_effect = check_gzip_enabled_and_return_mock

            response = utils.post_request(
                self.ASSETS_URL, assets_response,
                headers={},
                use_gzip=True)
            self.assertEqual(response.status_code, 200)

            def check_gzip_disabled_and_return_mock(url, data=None, headers=None, params=None, cookies=None):
                # gzip is not added as Content-Encoding header
                self.assertTrue('Content-Encoding' not in headers)
                # data is not gzipped.
                self.assertEqual(len(json.loads(data)["data"]["items"]), len(assets_response))
                # Return the mock response
                return mock_response(json_data=assets_response)
            mock_request.side_effect = check_gzip_disabled_and_return_mock

            response = utils.post_request(
                self.ASSETS_URL, assets_response,
                headers={},
                use_gzip=False)
            self.assertEqual(response.status_code, 200)

    def tearDown(self):
        cleanup_test_session()
        self.RANDOM_API_URL = None


def suites():
    return [unittest.TestLoader().loadTestsFromTestCase(RequestsTestCase)]
