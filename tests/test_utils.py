# -*- coding: utf-8 -*-
import re
from unittest import mock

import pytest

import cognite._utils as utils
from cognite import config
from tests.conftest import MockReturnValue

RESPONSE = {
    'data': {
        'items': [
            {'id': 123456789,
             'name': 'a_name',
             'parentId': 234567890,
             'description': 'A piece of equipment',
             'metadata': {
                 'md1': 'some data'
             }
             }
        ]
    }
}


@pytest.fixture(autouse=True)
def url():
    api_key, project = config.get_config_variables(None, None)
    return config.get_base_url() + '/projects/{}/assets'.format(project)


class TestRequests:
    @mock.patch('cognite._utils.requests.get')
    def test_get_request(self, mock_request):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)
        response = utils.get_request(url)

        assert response.status_code == 200
        assert len(response.json()['data']['items']) == len(RESPONSE)

    @mock.patch('cognite._utils.requests.get')
    def test_get_request_failed(self, mock_request):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(utils.APIError) as e:
            utils.get_request(url)
        assert re.match("Client error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(utils.APIError) as e:
            utils.get_request(url)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(utils.APIError) as e:
            utils.get_request(url)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

    @mock.patch('cognite._utils.requests.get')
    def test_get_request_exception(self, mock_request):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(utils.APIError) as e:
            utils.get_request(url)
        assert re.match("Custom error", str(e.value))

    @mock.patch('cognite._utils.requests.post')
    def test_post_request_ok(self, mock_request):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)

        response = utils.post_request(url, RESPONSE)
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json['data']['items']) == len(RESPONSE)

    @mock.patch('cognite._utils.requests.post')
    def test_post_request_failed(self, mock_request):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(utils.APIError) as e:
            utils.post_request(url, RESPONSE)
        assert re.match("Client error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(utils.APIError) as e:
            utils.post_request(url, RESPONSE)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(utils.APIError) as e:
            utils.post_request(url, RESPONSE)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

    @mock.patch('cognite._utils.requests.post')
    def test_post_request_exception(self, mock_request):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(utils.APIError) as e:
            utils.post_request(url, RESPONSE)
        assert re.match("Custom error", str(e.value))

    @mock.patch('cognite._utils.requests.post')
    def test_post_request_args(self, mock_request):
        def check_args_to_post_and_return_mock(arg_url, data=None, headers=None, params=None, cookies=None):
            # URL is sent as is
            assert arg_url == url

            # cookies should be the same
            assert cookies == {"a-cookie": 'a-cookie-val'}

            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_args_to_post_and_return_mock

        response = utils.post_request(
            url, RESPONSE,
            headers={"Existing-Header": "SomeValue"},
            cookies={"a-cookie": "a-cookie-val"},
            use_gzip=True)

        assert response.status_code == 200

    @mock.patch('cognite._utils.requests.post')
    def test_post_request_gzip(self, mock_request):
        import json, gzip
        def check_gzip_enabled_and_return_mock(arg_url, data=None, headers=None, params=None, cookies=None):
            # URL is sent as is
            assert arg_url == url
            # gzip is added as Content-Encoding header
            assert headers["Content-Encoding"] == "gzip"
            # data is gzipped. Decompress and check if items size matches
            decompressed_assets = json.loads(gzip.decompress(data))
            assert len(decompressed_assets["data"]["items"]) == len(RESPONSE)
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_gzip_enabled_and_return_mock

        response = utils.post_request(
            url, RESPONSE,
            headers={},
            use_gzip=True)
        assert response.status_code == 200

        def check_gzip_disabled_and_return_mock(arg_url, data=None, headers=None, params=None, cookies=None):
            # URL is sent as is
            assert arg_url == url
            # gzip is not added as Content-Encoding header
            assert 'Content-Encoding' not in headers
            # data is not gzipped.
            assert len(json.loads(data)["data"]["items"]) == len(RESPONSE)
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_gzip_disabled_and_return_mock

        response = utils.post_request(
            url, RESPONSE,
            headers={},
            use_gzip=False)
        assert response.status_code == 200

    @mock.patch('cognite._utils.requests.put')
    def test_put_request_ok(self, mock_request):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)

        response = utils.put_request(url, body=RESPONSE)
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json["data"]["items"]) == len(RESPONSE)

    @mock.patch('cognite._utils.requests.put')
    def test_put_request_failed(self, mock_request):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(utils.APIError) as e:
            utils.put_request(url)
        assert re.match("Client error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(utils.APIError) as e:
            utils.put_request(url)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(utils.APIError) as e:
            utils.put_request(url)
        assert re.match("Server error[\n]X-Request_id", str(e.value))

    @mock.patch('cognite._utils.requests.put')
    def test_put_request_exception(self, mock_request):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(utils.APIError) as e:
            utils.put_request(url)
        assert re.match("Custom error", str(e.value))

    @mock.patch('cognite._utils.requests.put')
    def test_put_request_args(self, mock_request):
        import json
        def check_args_to_put_and_return_mock(arg_url, data=None, headers=None, params=None, cookies=None):
            # URL is sent as is
            assert arg_url == url
            # data is json encoded
            assert len(json.loads(data)["data"]["items"]) == len(RESPONSE)
            # cookies should be the same
            assert cookies == {"a-cookie": "a-cookie-val"}
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_args_to_put_and_return_mock

        response = utils.put_request(
            url, RESPONSE,
            headers={"Existing-Header": "SomeValue"},
            cookies={"a-cookie": "a-cookie-val"},
        )

        assert response.status_code == 200


class TestConversions:
    def test_datetime_to_ms(self):
        from datetime import datetime
        assert utils.datetime_to_ms(datetime(2018, 1, 31)) == 1517356800000
        assert utils.datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11)) == 1517397071000
        assert utils.datetime_to_ms(datetime(100, 1, 31)) == -59008867200000
        with pytest.raises(TypeError):
            utils.datetime_to_ms(None)

    def test_round_to_nearest(self):
        assert utils.round_to_nearest(12, 10) == 10
        assert utils.round_to_nearest(8, 10) == 10

    def test_granularity_to_ms(self):
        assert utils.granularity_to_ms('10s') == 10000
        assert utils.granularity_to_ms('10m') == 600000

    def test_interval_to_ms(self):
        assert isinstance(utils.interval_to_ms(None, None)[0], int)
        assert isinstance(utils.interval_to_ms(None, None)[1], int)

    def test_time_ago_to_ms(self):
        assert utils._time_ago_to_ms('3w-ago') == 1814400000
        assert utils._time_ago_to_ms('1d-ago') == 86400000
        assert utils._time_ago_to_ms('1s-ago') == 1000
        assert utils
        assert utils._time_ago_to_ms('not_correctly_formatted') is None
