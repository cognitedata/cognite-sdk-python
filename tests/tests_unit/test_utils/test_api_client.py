# -*- coding: utf-8 -*-
import gzip
import json
from collections import namedtuple

import pytest

from cognite.client import APIError
from cognite.client._utils.api_client import APIClient, _model_hosting_emulator_url_converter
from cognite.client._utils.resource_base import CogniteResource

BASE_URL = "http://localtest.com/api/1.0/projects/test_proj"
URL_PATH = "/someurl"

RESPONSE = {"any": "ok"}

API_CLIENT = APIClient(
    project="test_proj",
    base_url=BASE_URL,
    num_of_workers=1,
    cookies={"a-cookie": "a-cookie-val"},
    headers={},
    timeout=60,
)


class TestBasicRequests:
    @pytest.fixture
    def mock_all_requests_ok(self, rsps):
        rsps.assert_all_requests_are_fired = False
        for method in [rsps.GET, rsps.PUT, rsps.POST, rsps.DELETE]:
            rsps.add(method, BASE_URL + URL_PATH, status=200, json=RESPONSE)

    @pytest.fixture
    def mock_all_requests_fail(self, rsps):
        rsps.assert_all_requests_are_fired = False
        for method in [rsps.GET, rsps.PUT, rsps.POST, rsps.DELETE]:
            rsps.add(method, BASE_URL + URL_PATH, status=400, json={"error": "Client error"})
            rsps.add(method, BASE_URL + URL_PATH, status=500, body="Server error")
            rsps.add(method, BASE_URL + URL_PATH, status=500, json={"error": "Server error"})
            rsps.add(method, BASE_URL + URL_PATH, status=400, json={"error": {"code": 400, "message": "Client error"}})

    RequestCase = namedtuple("RequestCase", ["name", "method", "kwargs"])

    request_cases = [
        RequestCase(name="post", method=API_CLIENT._post, kwargs={"url": URL_PATH, "body": {"any": "ok"}}),
        RequestCase(name="get", method=API_CLIENT._get, kwargs={"url": URL_PATH}),
        RequestCase(name="delete", method=API_CLIENT._delete, kwargs={"url": URL_PATH}),
        RequestCase(name="put", method=API_CLIENT._put, kwargs={"url": URL_PATH, "body": {"any": "ok"}}),
    ]

    @pytest.mark.usefixtures("mock_all_requests_ok")
    @pytest.mark.parametrize("name, method, kwargs", request_cases)
    def test_requests_ok(self, name, method, kwargs):
        response = method(**kwargs)
        assert response.status_code == 200
        assert response.json() == RESPONSE

    @pytest.mark.usefixtures("mock_all_requests_fail")
    @pytest.mark.parametrize("name, method, kwargs", request_cases)
    def test_requests_fail(self, name, method, kwargs):
        with pytest.raises(APIError, match="Client error") as e:
            method(**kwargs)
        assert e.value.code == 400

        with pytest.raises(APIError, match="Server error") as e:
            method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(APIError, match="Server error") as e:
            method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(APIError, match="Client error | code: 400 | X-Request-ID:") as e:
            method(**kwargs)
        assert e.value.code == 400
        assert e.value.message == "Client error"

    @pytest.fixture
    def mock_requests_auto_paging(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [1, 2, 3], "nextCursor": "next"}})
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [4, 5, 6], "nextCursor": "next"}})
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [7, 8, 9], "nextCursor": None}})

    @pytest.mark.usefixtures("mock_requests_auto_paging")
    def test_get_request_with_autopaging(self):
        res = API_CLIENT._get(URL_PATH, params={}, autopaging=True)
        assert {"data": {"items": [1, 2, 3, 4, 5, 6, 7, 8, 9]}} == res.json()

    @pytest.mark.usefixtures("disable_gzip")
    def test_request_gzip_disabled(self, rsps):
        def check_gzip_disabled(request):
            assert "Content-Encoding" not in request.headers
            assert {"any": "OK"} == json.loads(request.body)
            return 200, {}, json.dumps(RESPONSE)

        for method in [rsps.PUT, rsps.POST]:
            rsps.add_callback(method, BASE_URL + URL_PATH, check_gzip_disabled)

        API_CLIENT._post(URL_PATH, {"any": "OK"}, headers={})
        API_CLIENT._put(URL_PATH, {"any": "OK"}, headers={})

    def test_request_gzip_enabled(self, rsps):
        def check_gzip_enabled(request):
            assert "Content-Encoding" in request.headers
            assert {"any": "OK"} == json.loads(gzip.decompress(request.body))
            return 200, {}, json.dumps(RESPONSE)

        for method in [rsps.PUT, rsps.POST]:
            rsps.add_callback(method, BASE_URL + URL_PATH, check_gzip_enabled)

        API_CLIENT._post(URL_PATH, {"any": "OK"}, headers={})
        API_CLIENT._put(URL_PATH, {"any": "OK"}, headers={})


class TestStandardMethods:
    class SomeResource(CogniteResource):
        def __init__(self, x=None, y=None):
            self.x = x
            self.y = y

    @pytest.fixture
    def mock_get_for_retrieve_ok(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [{"x": 1, "y": 2}]}})

    @pytest.mark.usefixtures("mock_get_for_retrieve_ok")
    def test_standard_retrieve_OK(self):
        assert self.SomeResource(1, 2) == API_CLIENT._standard_retrieve(resource=self.SomeResource, url=URL_PATH)

    @pytest.fixture
    def mock_get_for_retrieve_fail(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=400, json={"error": {"message": "Client Error"}})

    @pytest.mark.usefixtures("mock_get_for_retrieve_fail")
    def test_standard_retrieve_fail(self):
        with pytest.raises(APIError, match="Client Error") as e:
            API_CLIENT._standard_retrieve(resource=self.SomeResource, url=URL_PATH)
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    @pytest.fixture
    def mock_get_for_retrieve_unknown_attribute(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [{"x": 1, "y": 2, "z": 3}]}})

    @pytest.mark.usefixtures("mock_get_for_retrieve_unknown_attribute")
    def test_standard_retrieve_unknown_attribute(self):
        with pytest.raises(AttributeError):
            API_CLIENT._standard_retrieve(resource=self.SomeResource, url=URL_PATH)


class TestMiscellaneous:
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
