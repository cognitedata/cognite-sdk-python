# -*- coding: utf-8 -*-
import gzip
import json
from collections import namedtuple

import pytest

from cognite.client import APIError
from cognite.client._utils.api_client import APIClient, _model_hosting_emulator_url_converter
from cognite.client._utils.bases import CogniteResource, CogniteResourceList

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
        RequestCase(name="post", method=API_CLIENT._post, kwargs={"url_path": URL_PATH, "body": {"any": "ok"}}),
        RequestCase(name="get", method=API_CLIENT._get, kwargs={"url_path": URL_PATH}),
        RequestCase(name="delete", method=API_CLIENT._delete, kwargs={"url_path": URL_PATH}),
        RequestCase(name="put", method=API_CLIENT._put, kwargs={"url_path": URL_PATH, "body": {"any": "ok"}}),
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


class SomeResource(CogniteResource):
    def __init__(self, x=None, y=None):
        self.x = x
        self.y = y


class SomeResourceList(CogniteResourceList):
    _RESOURCE = SomeResource


class TestStandardMethods:
    @pytest.fixture
    def mock_get_for_retrieve_ok(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [{"x": 1, "y": 2}]}})

    @pytest.mark.usefixtures("mock_get_for_retrieve_ok")
    def test_standard_retrieve_OK(self):
        assert SomeResource(1, 2) == API_CLIENT._standard_retrieve(resource_class=SomeResource, url_path=URL_PATH)

    @pytest.fixture
    def mock_get_fail(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=400, json={"error": {"message": "Client Error"}})

    @pytest.mark.usefixtures("mock_get_fail")
    def test_standard_retrieve_fail(self):
        with pytest.raises(APIError, match="Client Error") as e:
            API_CLIENT._standard_retrieve(resource_class=SomeResource, url_path=URL_PATH)
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    @pytest.fixture
    def mock_get_unknown_attribute(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [{"x": 1, "y": 2, "z": 3}]}})

    @pytest.mark.usefixtures("mock_get_unknown_attribute")
    def test_standard_retrieve_unknown_attribute(self):
        with pytest.raises(AttributeError):
            API_CLIENT._standard_retrieve(resource_class=SomeResource, url_path=URL_PATH)

    @pytest.fixture
    def mock_get_for_list_ok(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"data": {"items": [{"x": 1, "y": 2}, {"x": 1}]}})

    @pytest.mark.usefixtures("mock_get_for_list_ok")
    def test_standard_list_ok(self):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._standard_list(
            resource_list_class=SomeResourceList, url_path=URL_PATH
        )

    @pytest.mark.usefixtures("mock_get_fail")
    def test_standard_list_fail(self):
        with pytest.raises(APIError, match="Client Error") as e:
            API_CLIENT._standard_list(resource_list_class=SomeResourceList, url_path=URL_PATH)
        assert 400 == e.value.code
        assert "Client Error" == e.value.message

    @pytest.mark.usefixtures("mock_get_unknown_attribute")
    def test_standard_list_unkown_attribute(self):
        with pytest.raises(AttributeError):
            API_CLIENT._standard_list(resource_list_class=SomeResourceList, url_path=URL_PATH)

    NUMBER_OF_ITEMS_FOR_AUTOPAGING = 11500
    ITEMS_TO_GET_WHILE_AUTOPAGING = [{"x": 1, "y": 1} for _ in range(NUMBER_OF_ITEMS_FOR_AUTOPAGING)]

    @pytest.fixture
    def mock_get_for_autopaging(self, rsps):
        def callback(request):
            params = {elem.split("=")[0]: elem.split("=")[1] for elem in request.path_url.split("?")[-1].split("&")}
            limit = int(params["limit"])
            cursor = int(params.get("cursor") or 0)
            items = self.ITEMS_TO_GET_WHILE_AUTOPAGING[cursor : cursor + limit]
            if cursor + limit >= self.NUMBER_OF_ITEMS_FOR_AUTOPAGING:
                next_cursor = None
            else:
                next_cursor = cursor + limit
            response = json.dumps({"data": {"nextCursor": next_cursor, "items": items}})
            return 200, {}, response

        rsps.add_callback(rsps.GET, BASE_URL + URL_PATH, callback)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator(self):
        total_resources = 0
        for resources in API_CLIENT._standard_list_generator(
            resource_list=SomeResourceList, url_path=URL_PATH, limit=10000
        ):
            assert 1000 == len(resources)
            total_resources += len(resources)
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging(self):
        res = API_CLIENT._standard_list(resource_list_class=SomeResourceList, url_path=URL_PATH)
        assert self.NUMBER_OF_ITEMS_FOR_AUTOPAGING == len(res)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging_with_limit(self):
        res = API_CLIENT._standard_list(resource_list_class=SomeResourceList, url_path=URL_PATH, limit=5333)
        assert 5333 == len(res)

    @pytest.fixture
    def mock_post_for_create_ok(self, rsps):
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH,
            status=200,
            json={"data": {"items": [{"x": 1, "y": 1}, {"x": 1, "y": None}]}},
        )

    @pytest.mark.usefixtures("mock_post_for_create_ok")
    def test_standard_create_ok(self):
        res = API_CLIENT._standard_create(
            resource_list_class=SomeResourceList, url_path=URL_PATH, items=[SomeResource(1, 1), SomeResource(1)]
        )
        assert SomeResource(1, 1) == res[0]
        assert SomeResource(1) == res[1]

    @pytest.fixture
    def mock_post_for_create_fail(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=400, json={"error": {"message": "Client Error"}})

    @pytest.mark.usefixtures("mock_post_for_create_fail")
    def test_standard_create_fail(self):
        with pytest.raises(APIError, match="Client Error") as e:
            API_CLIENT._standard_create(
                resource_list_class=SomeResourceList, url_path=URL_PATH, items=[SomeResource(1, 1), SomeResource(1)]
            )
        assert 400 == e.value.code
        assert "Client Error" == e.value.message


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
