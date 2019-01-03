# -*- coding: utf-8 -*-
import re
from unittest import mock
from unittest.mock import MagicMock

import pytest
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cognite import APIError
from cognite.client._api_client import APIClient
from cognite.client.cognite_client import STATUS_FORCELIST
from tests.conftest import MockReturnValue

RESPONSE = {
    "data": {
        "items": [
            {
                "id": 123456789,
                "name": "a_name",
                "parentId": 234567890,
                "description": "A piece of equipment",
                "metadata": {"md1": "some data"},
            }
        ]
    }
}


@pytest.fixture(autouse=True)
def api_client():
    session = Session()
    retry = Retry(total=0, read=0, connect=0, backoff_factor=1, status_forcelist=STATUS_FORCELIST)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    client = APIClient(
        request_session=session,
        project="test_proj",
        base_url="http://localtest.com/api/0.5/projects/test_proj",
        num_of_workers=1,
        cookies={"a-cookie": "a-cookie-val"},
        headers={},
        timeout=60,
    )
    yield client


@pytest.fixture(autouse=True)
def api_client_with_retries():
    session = Session()
    tries = 2
    retry = Retry(total=tries, read=tries, connect=tries, backoff_factor=1, status_forcelist=STATUS_FORCELIST)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    client = APIClient(
        request_session=session,
        project="test_proj",
        base_url="http://localtest.com/api/0.5/projects/test_proj",
        num_of_workers=1,
        cookies={"a-cookie": "a-cookie-val"},
        headers={},
        timeout=60,
    )
    yield client


@pytest.fixture(autouse=True)
def url():
    yield "/assets"


class TestRequests:
    @mock.patch("requests.sessions.Session.delete")
    def test_delete_request_ok(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)
        response = api_client._delete(url)
        assert response.status_code == 200
        assert len(response.json()["data"]["items"]) == len(RESPONSE)

    @mock.patch("requests.sessions.Session.delete")
    def test_delete_request_failed(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(APIError) as e:
            api_client._delete(url)
        assert re.match("Client error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(APIError) as e:
            api_client._delete(url)
        assert re.match("Server error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(APIError) as e:
            api_client._delete(url)
        assert re.match("Server error", str(e.value))

        mock_request.return_value = MockReturnValue(
            status=400, json_data={"error": {"code": 400, "message": "Client error"}}
        )

        with pytest.raises(APIError) as e:
            api_client._delete(url)
        assert re.match("Client error | code: 400 | X-Request-ID:", str(e.value))
        assert e.value.code == 400
        assert e.value.message == "Client error"

    @mock.patch("requests.sessions.Session.delete")
    def test_delete_request_exception(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(Exception) as e:
            api_client._delete(url)
        assert re.match("Custom error", str(e.value))

    @mock.patch("requests.sessions.Session.get")
    def test_get_request(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)
        response = api_client._get(url)

        assert response.status_code == 200
        assert len(response.json()["data"]["items"]) == len(RESPONSE)

    @mock.patch("requests.sessions.Session.get")
    def test_get_request_failed(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(APIError) as e:
            api_client._get(url)
        assert re.match("Client error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(APIError) as e:
            api_client._get(url)
        assert re.match("Server error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(APIError) as e:
            api_client._get(url)
        assert re.match("Server error", str(e.value))

    @mock.patch("requests.sessions.Session.get")
    def test_get_request_exception(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(Exception) as e:
            api_client._get(url)
        assert re.match("Custom error", str(e.value))

    @mock.patch("requests.sessions.Session.post")
    def test_post_request_ok(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)

        response = api_client._post(url, RESPONSE)
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json["data"]["items"]) == len(RESPONSE)

    @mock.patch("requests.sessions.Session.post")
    def test_post_request_failed(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(APIError) as e:
            api_client._post(url, RESPONSE)
        assert re.match("Client error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(APIError) as e:
            api_client._post(url, RESPONSE)
        assert re.match("Server error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(APIError) as e:
            api_client._post(url, RESPONSE)
        assert re.match("Server error", str(e.value))

    @mock.patch("requests.sessions.Session.post")
    def test_post_request_exception(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(Exception) as e:
            api_client._post(url, RESPONSE)
        assert re.match("Custom error", str(e.value))

    @mock.patch("requests.sessions.Session.post")
    def test_post_request_args(self, mock_request, api_client, url):
        def check_args_to_post_and_return_mock(
            arg_url, data=None, headers=None, params=None, cookies=None, timeout=None
        ):
            # URL is sent as is
            assert arg_url == api_client._base_url + url

            # cookies should be the same
            assert cookies == {"a-cookie": "a-cookie-val"}

            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_args_to_post_and_return_mock

        response = api_client._post(url, RESPONSE, headers={"Existing-Header": "SomeValue"}, use_gzip=True)

        assert response.status_code == 200

    @mock.patch("requests.sessions.Session.post")
    def test_post_request_gzip(self, mock_request, api_client, url):
        import json, gzip

        def check_gzip_enabled_and_return_mock(
            arg_url, data=None, headers=None, params=None, cookies=None, timeout=None
        ):
            # URL is sent as is
            assert arg_url == api_client._base_url + url
            # gzip is added as Content-Encoding header
            assert headers["Content-Encoding"] == "gzip"
            # data is gzipped. Decompress and check if items size matches
            decompressed_assets = json.loads(gzip.decompress(data))
            assert len(decompressed_assets["data"]["items"]) == len(RESPONSE)
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_gzip_enabled_and_return_mock

        response = api_client._post(url, RESPONSE, headers={}, use_gzip=True)
        assert response.status_code == 200

        def check_gzip_disabled_and_return_mock(
            arg_url, data=None, headers=None, params=None, cookies=None, timeout=None
        ):
            # URL is sent as is
            assert arg_url == api_client._base_url + url
            # gzip is not added as Content-Encoding header
            assert "Content-Encoding" not in headers
            # data is not gzipped.
            assert len(json.loads(data)["data"]["items"]) == len(RESPONSE)
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_gzip_disabled_and_return_mock

        response = api_client._post(url, RESPONSE, headers={}, use_gzip=False)
        assert response.status_code == 200

    @mock.patch("requests.sessions.Session.put")
    def test_put_request_ok(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(json_data=RESPONSE)

        response = api_client._put(url, body=RESPONSE)
        response_json = response.json()

        assert response.status_code == 200
        assert len(response_json["data"]["items"]) == len(RESPONSE)

    @mock.patch("requests.sessions.Session.put")
    def test_put_request_failed(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=400, json_data={"error": "Client error"})

        with pytest.raises(APIError) as e:
            api_client._put(url)
        assert re.match("Client error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, content="Server error")

        with pytest.raises(APIError) as e:
            api_client._put(url)
        assert re.match("Server error", str(e.value))

        mock_request.return_value = MockReturnValue(status=500, json_data={"error": "Server error"})

        with pytest.raises(APIError) as e:
            api_client._put(url)
        assert re.match("Server error", str(e.value))

    @mock.patch("requests.sessions.Session.put")
    def test_put_request_exception(self, mock_request, api_client, url):
        mock_request.return_value = MockReturnValue(status=500)
        mock_request.side_effect = Exception("Custom error")

        with pytest.raises(Exception) as e:
            api_client._put(url)
        assert re.match("Custom error", str(e.value))

    @mock.patch("requests.sessions.Session.put")
    def test_put_request_args(self, mock_request, api_client, url):
        import json

        def check_args_to_put_and_return_mock(
            arg_url, data=None, headers=None, params=None, cookies=None, timeout=None
        ):
            # URL is sent as is
            assert arg_url == api_client._base_url + url
            # data is json encoded
            assert len(json.loads(data)["data"]["items"]) == len(RESPONSE)
            # cookies should be the same
            assert cookies == {"a-cookie": "a-cookie-val"}
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_args_to_put_and_return_mock

        response = api_client._put(url, RESPONSE, headers={"Existing-Header": "SomeValue"})

        assert response.status_code == 200

    @pytest.mark.parametrize("status_code,expected_number_of_tries", [(400, 1), (500, 1)])
    @mock.patch("requests.sessions.Session.post")
    def test_retry_logic_post(self, mock_post, status_code, expected_number_of_tries, api_client_with_retries, url):
        response_object = MagicMock()
        response_object.configure_mock(status_code=status_code)
        mock_post.return_value = response_object

        with pytest.raises(APIError):
            api_client_with_retries._post(url, RESPONSE)

        assert mock_post.call_count == expected_number_of_tries, "incorrect number of tries for post"

    @pytest.mark.parametrize("status_code,expected_number_of_tries", [(400, 1), (500, 2)])
    @mock.patch("requests.sessions.Session.get")
    def test_retry_logic_get(self, mock_get, status_code, expected_number_of_tries, api_client_with_retries, url):
        import logging

        logging.basicConfig(level="DEBUG")

        response_object = MagicMock()
        response_object.configure_mock(status_code=status_code)
        mock_get.return_value = response_object

        with pytest.raises(APIError):
            api_client_with_retries._get(url, RESPONSE)

        assert mock_get.call_count == expected_number_of_tries, "incorrect number of tries for get"
