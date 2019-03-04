# -*- coding: utf-8 -*-
import gzip
import json
import re
from unittest import mock

import pytest

from cognite.client import APIError
from cognite.client._api_client import APIClient, _model_hosting_emulator_url_converter
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
    client = APIClient(
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

    @mock.patch("requests.sessions.Session.get")
    def test_get_request_with_autopaging(self, mock_request, api_client, url):
        mock_request.side_effect = [
            MockReturnValue(json_data={"data": {"items": [1, 2, 3], "nextCursor": "next"}}),
            MockReturnValue(json_data={"data": {"items": [4, 5, 6], "nextCursor": "next"}}),
            MockReturnValue(json_data={"data": {"items": [7, 8, 9], "nextCursor": None}}),
        ]

        res = api_client._get(url, params={}, autopaging=True)
        assert mock_request.call_count == 3
        assert {"data": {"items": [1, 2, 3, 4, 5, 6, 7, 8, 9]}} == res.json()

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

        response = api_client._post(url, RESPONSE, headers={"Existing-Header": "SomeValue"})

        assert response.status_code == 200

    @pytest.mark.usefixtures("disable_gzip")
    @mock.patch("requests.sessions.Session.post")
    def test_post_request_gzip_disabled(self, mock_request, api_client, url):
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

        response = api_client._post(url, RESPONSE, headers={})
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
            assert len(json.loads(gzip.decompress(data).decode("utf8"))["data"]["items"]) == len(RESPONSE)
            # cookies should be the same
            assert cookies == {"a-cookie": "a-cookie-val"}
            # Return the mock response
            return MockReturnValue(json_data=RESPONSE)

        mock_request.side_effect = check_args_to_put_and_return_mock

        response = api_client._put(url, RESPONSE, headers={"Existing-Header": "SomeValue"})

        assert response.status_code == 200

    @pytest.mark.parametrize(
        "input, expected",
        [
            (
                "https://api.cognitedata.com/api/0.6/projects/test-project/analytics/models",
                "http://localhost:8000/api/0.1/projects/test-project/models",
            ),
            (
                "https://api.cognitedata.com/api/0.6/projects/test-project/analytics/models/sourcepackages/1",
                "http://localhost:8000/api/0.1/projects/test-project/models/sourcepackages/1",
            ),
            (
                "https://api.cognitedata.com/api/0.6/projects/test-project/assets/update",
                "https://api.cognitedata.com/api/0.6/projects/test-project/assets/update",
            ),
        ],
    )
    def test_nostromo_emulator_url_converter(self, input, expected):
        assert expected == _model_hosting_emulator_url_converter(input)
