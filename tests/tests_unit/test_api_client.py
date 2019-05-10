import os
from collections import namedtuple

import pytest

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client._base import *
from cognite.client.exceptions import CogniteAPIError, CogniteCompoundAPIError
from tests.utils import jsgz_load, set_request_limit

BASE_URL = "http://localtest.com/api/1.0/projects/test-project"
URL_PATH = "/someurl"

RESPONSE = {"any": "ok"}
COGNITE_CLIENT = CogniteClient()
API_CLIENT = APIClient(
    project="test-project",
    api_key="abc",
    base_url=BASE_URL,
    max_workers=1,
    headers={},
    timeout=60,
    cognite_client=COGNITE_CLIENT,
)


class TestBasicRequests:
    @pytest.fixture
    def mock_all_requests_ok(self, rsps):
        rsps.assert_all_requests_are_fired = False
        for method in [rsps.GET, rsps.PUT, rsps.POST, rsps.DELETE]:
            rsps.add(method, BASE_URL + URL_PATH, status=200, json=RESPONSE)
        yield rsps

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
        RequestCase(name="post", method=API_CLIENT._post, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}),
        RequestCase(name="get", method=API_CLIENT._get, kwargs={"url_path": URL_PATH}),
        RequestCase(name="delete", method=API_CLIENT._delete, kwargs={"url_path": URL_PATH}),
        RequestCase(name="put", method=API_CLIENT._put, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}),
    ]

    @pytest.mark.parametrize("name, method, kwargs", request_cases)
    def test_requests_ok(self, name, method, kwargs, mock_all_requests_ok):
        response = method(**kwargs)
        assert response.status_code == 200
        assert response.json() == RESPONSE

        request_headers = mock_all_requests_ok.calls[0].request.headers
        assert "application/json" == request_headers["content-type"]
        assert "application/json" == request_headers["accept"]
        assert API_CLIENT._api_key == request_headers["api-key"]
        assert "User-Agent" in request_headers

    @pytest.mark.usefixtures("mock_all_requests_fail")
    @pytest.mark.parametrize("name, method, kwargs", request_cases)
    def test_requests_fail(self, name, method, kwargs):
        with pytest.raises(CogniteAPIError, match="Client error") as e:
            method(**kwargs)
        assert e.value.code == 400

        with pytest.raises(CogniteAPIError, match="Server error") as e:
            method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(CogniteAPIError, match="Server error") as e:
            method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(CogniteAPIError, match="Client error | code: 400 | X-Request-ID:") as e:
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
            assert {"any": "OK"} == jsgz_load(request.body)
            return 200, {}, json.dumps(RESPONSE)

        for method in [rsps.PUT, rsps.POST]:
            rsps.add_callback(method, BASE_URL + URL_PATH, check_gzip_enabled)

        API_CLIENT._post(URL_PATH, {"any": "OK"}, headers={})
        API_CLIENT._put(URL_PATH, {"any": "OK"}, headers={})

    def test_headers_correct(self, mock_all_requests_ok):
        API_CLIENT._post(URL_PATH, {"any": "OK"}, headers={"additional": "stuff"})
        headers = mock_all_requests_ok.calls[0].request.headers

        assert "gzip, deflate" == headers["accept-encoding"]
        assert "gzip" == headers["content-encoding"]
        assert "CognitePythonSDK:{}".format(utils.get_current_sdk_version()) == headers["x-cdp-sdk"]
        assert "abc" == headers["api-key"]
        assert "stuff" == headers["additional"]


class SomeUpdate(CogniteUpdate):
    @property
    def y(self):
        return PrimitiveUpdate(self, "y")


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> SomeUpdate:
        return self._set(value)


class SomeResource(CogniteResource):
    def __init__(self, x=None, y=None, id=None, external_id=None, cognite_client=None):
        self.x = x
        self.y = y
        self.id = id
        self.external_id = external_id


class SomeResourceList(CogniteResourceList):
    _RESOURCE = SomeResource
    _UPDATE = SomeUpdate


class SomeFilter(CogniteFilter):
    def __init__(self, var_x, var_y):
        self.var_x = var_x
        self.var_y = var_y


class TestStandardRetrieve:
    def test_standard_retrieve_OK(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=200, json={"x": 1, "y": 2})
        assert SomeResource(1, 2) == API_CLIENT._retrieve(cls=SomeResource, resource_path=URL_PATH, id=1)

    def test_standard_retrieve_fail(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            API_CLIENT._retrieve(cls=SomeResource, resource_path=URL_PATH, id=1)
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_cognite_client_is_set(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=200, json={"x": 1, "y": 2})
        assert COGNITE_CLIENT == API_CLIENT._retrieve(cls=SomeResource, resource_path=URL_PATH, id=1)._cognite_client


class TestStandardRetrieveMultiple:
    @pytest.fixture
    def mock_by_ids(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        yield rsps

    def test_by_id_no_wrap_OK(self, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=False, ids=[1, 2]
        )
        assert {"items": [1, 2]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_single_id_no_wrap_OK(self, mock_by_ids):
        assert SomeResource(1, 2) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=False, ids=1
        )
        assert {"items": [1]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_id_wrap_OK(self, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, ids=[1, 2]
        )
        assert {"items": [{"id": 1}, {"id": 2}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_single_id_wrap_OK(self, mock_by_ids):
        assert SomeResource(1, 2) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, ids=1
        )
        assert {"items": [{"id": 1}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_external_id_wrap_OK(self, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, external_ids=["1", "2"]
        )
        assert {"items": [{"externalId": "1"}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_single_external_id_wrap_OK(self, mock_by_ids):
        assert SomeResource(1, 2) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, external_ids="1"
        )
        assert {"items": [{"externalId": "1"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_external_id_no_wrap(self):
        with pytest.raises(ValueError, match="must be wrapped"):
            API_CLIENT._retrieve_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=False, external_ids=["1", "2"]
            )

    def test_id_and_external_id_mixed(self, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._retrieve_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, ids=1, external_ids=["2"]
        )
        assert {"items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_standard_retrieve_multiple_fail(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            API_CLIENT._retrieve_multiple(cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, ids=[1, 2])
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_ids_all_None(self):
        with pytest.raises(ValueError, match="No ids specified"):
            API_CLIENT._retrieve_multiple(cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=False)

    def test_cognite_client_is_set(self, mock_by_ids):
        assert (
            COGNITE_CLIENT
            == API_CLIENT._retrieve_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, wrap_ids=True, ids=[1, 2]
            )._cognite_client
        )

    def test_over_limit_concurrent(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 3, "y": 4}]})

        with set_request_limit(API_CLIENT, 1):
            API_CLIENT._retrieve_multiple(cls=SomeResourceList, resource_path=URL_PATH, ids=[1, 2], wrap_ids=False)

        assert {"items": [1]} == jsgz_load(rsps.calls[0].request.body)
        assert {"items": [2]} == jsgz_load(rsps.calls[1].request.body)


class TestStandardList:
    def test_standard_list_ok(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert (
            SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump()
            == API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="GET").dump()
        )

    def test_standard_list_with_filter_GET_ok(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert (
            SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump()
            == API_CLIENT._list(
                cls=SomeResourceList, resource_path=URL_PATH, method="GET", filter={"filter": "bla"}
            ).dump()
        )
        assert "filter=bla" in rsps.calls[0].request.path_url

    def test_standard_list_with_filter_POST_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/list", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == API_CLIENT._list(
            cls=SomeResourceList, resource_path=URL_PATH, method="POST", filter={"filter": "bla"}
        )
        assert {"filter": {"filter": "bla"}, "limit": 1000, "cursor": None} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_list_fail(self, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="GET")
        assert 400 == e.value.code
        assert "Client Error" == e.value.message

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
            response = json.dumps({"nextCursor": next_cursor, "items": items})
            return 200, {}, response

        rsps.add_callback(rsps.GET, BASE_URL + URL_PATH, callback)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator(self):
        total_resources = 0
        for resource in API_CLIENT._list_generator(cls=SomeResourceList, resource_path=URL_PATH, method="GET"):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 11500 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_limit(self):
        total_resources = 0
        for resource in API_CLIENT._list_generator(
            cls=SomeResourceList, resource_path=URL_PATH, method="GET", limit=10000
        ):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_chunk_size(self):
        total_resources = 0
        for resource_chunk in API_CLIENT._list_generator(
            cls=SomeResourceList, resource_path=URL_PATH, method="GET", chunk_size=1000
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            if len(resource_chunk) == 1000:
                total_resources += 1000
            elif len(resource_chunk) == 500:
                total_resources += 500
            else:
                raise AssertionError("resource chunk length was not 1000 or 500")
        assert 11500 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_chunk_size_with_limit(self):
        total_resources = 0
        for resource_chunk in API_CLIENT._list_generator(
            cls=SomeResourceList, resource_path=URL_PATH, method="GET", limit=10000, chunk_size=1000
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            assert 1000 == len(resource_chunk)
            total_resources += 1000
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator__chunk_size_exceeds_max(self):
        total_resources = 0
        for resource_chunk in API_CLIENT._list_generator(
            cls=SomeResourceList, resource_path=URL_PATH, method="GET", limit=2002, chunk_size=1001
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            assert 1001 == len(resource_chunk)
            total_resources += 1001
        assert 2002 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging(self):
        res = API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="GET")
        assert self.NUMBER_OF_ITEMS_FOR_AUTOPAGING == len(res)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging_with_limit(self):
        res = API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="GET", limit=5333)
        assert 5333 == len(res)

    def test_cognite_client_is_set(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/list", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert (
            COGNITE_CLIENT
            == API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="POST")._cognite_client
        )
        assert (
            COGNITE_CLIENT
            == API_CLIENT._list(cls=SomeResourceList, resource_path=URL_PATH, method="GET")._cognite_client
        )


class TestStandardCreate:
    def test_standard_create_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        res = API_CLIENT._create_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(1, 1), SomeResource(1)]
        )
        assert {"items": [{"x": 1, "y": 1}, {"x": 1}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResource(1, 2) == res[0]
        assert SomeResource(1) == res[1]

    def test_standard_create_single_item_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        res = API_CLIENT._create_multiple(cls=SomeResourceList, resource_path=URL_PATH, items=SomeResource(1, 2))
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResource(1, 2) == res

    def test_standard_create_single_item_in_list_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        res = API_CLIENT._create_multiple(cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(1, 2)])
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResourceList([SomeResource(1, 2)]) == res

    def test_standard_create_fail(self, rsps):
        def callback(request):
            item = jsgz_load(request.body)["items"][0]
            return int(item["externalId"]), {}, json.dumps({})

        rsps.add_callback(rsps.POST, BASE_URL + URL_PATH, callback=callback, content_type="application/json")
        with set_request_limit(API_CLIENT, 1):
            with pytest.raises(CogniteCompoundAPIError) as e:
                API_CLIENT._create_multiple(
                    cls=SomeResourceList,
                    resource_path=URL_PATH,
                    items=[
                        SomeResource(1, 1, external_id="200"),
                        SomeResource(1, external_id="400"),
                        SomeResource(external_id="500"),
                    ],
                )
        assert 400 == e.value.code
        assert [SomeResource(1, external_id="400")] == e.value.failed
        assert [SomeResource(1, 1, external_id="200")] == e.value.successful
        assert [SomeResource(external_id="500")] == e.value.unknown

    def test_standard_create_concurrent(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 3, "y": 4}]})

        res = API_CLIENT._create_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(1, 2), SomeResource(3, 4)], limit=1
        )
        assert SomeResourceList([SomeResource(1, 2), SomeResource(3, 4)]) == res

        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(rsps.calls[0].request.body)
        assert {"items": [{"x": 3, "y": 4}]} == jsgz_load(rsps.calls[1].request.body)

    def test_cognite_client_is_set(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        assert (
            COGNITE_CLIENT
            == API_CLIENT._create_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=SomeResource()
            )._cognite_client
        )
        assert (
            COGNITE_CLIENT
            == API_CLIENT._create_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource()]
            )._cognite_client
        )


class TestStandardDelete:
    def test_standard_delete_multiple_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        API_CLIENT._delete_multiple(resource_path=URL_PATH, wrap_ids=False, ids=[1, 2])
        assert {"items": [1, 2]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_ok__single_id(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        API_CLIENT._delete_multiple(resource_path=URL_PATH, wrap_ids=False, ids=1)
        assert {"items": [1]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_ok__single_id_in_list(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        API_CLIENT._delete_multiple(resource_path=URL_PATH, wrap_ids=False, ids=[1])
        assert {"items": [1]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_fail_4xx(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteCompoundAPIError) as e:
            API_CLIENT._delete_multiple(resource_path=URL_PATH, wrap_ids=False, ids=[1, 2])
        assert 400 == e.value.code
        assert "Client Error" == e.value.message
        assert e.value.failed == [1, 2]

    def test_standard_delete_multiple_fail_5xx(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=500, json={"error": {"message": "Server Error"}})
        with pytest.raises(CogniteCompoundAPIError) as e:
            API_CLIENT._delete_multiple(resource_path=URL_PATH, wrap_ids=False, ids=[1, 2])
        assert 500 == e.value.code
        assert "Server Error" == e.value.message
        assert e.value.unknown == [1, 2]
        assert e.value.failed == []

    def test_over_limit_concurrent(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})

        with set_request_limit(API_CLIENT, 2):
            API_CLIENT._delete_multiple(resource_path=URL_PATH, ids=[1, 2, 3, 4], wrap_ids=False)
        assert {"items": [1, 2]} == jsgz_load(rsps.calls[0].request.body)
        assert {"items": [3, 4]} == jsgz_load(rsps.calls[1].request.body)


class TestStandardUpdate:
    @pytest.fixture
    def mock_update(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"id": 1, "x": 1, "y": 100}]})
        yield rsps

    def test_standard_update_with_cognite_resource_OK(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(id=1, y=100)]
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_resource__non_update_attributes(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(id=1, y=100, x=1)]
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_resource__id_and_external_id_set(self):
        with pytest.raises(AssertionError, match="Exactly one of id and external id"):
            API_CLIENT._update_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(id=1, external_id="1", y=100, x=1)]
            )

    def test_standard_update_with_cognite_resource_and_external_id_OK(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(external_id="1", y=100)]
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_with_cognite_resource__id_error(self):
        with pytest.raises(AssertionError, match="one of id and external id"):
            API_CLIENT._update_multiple(cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(y=100)])

        with pytest.raises(AssertionError, match="one of id and external id"):
            API_CLIENT._update_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(id=1, external_id="1", y=100)]
            )

    def test_standard_update_with_cognite_update_object_OK(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeUpdate(id=1).y.set(100)]
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_single_object(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=SomeUpdate(id=1).y.set(100)
        )
        assert SomeResource(id=1, x=1, y=100) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_update_object_and_external_id_OK(self, mock_update):
        res = API_CLIENT._update_multiple(
            cls=SomeResourceList, resource_path=URL_PATH, items=[SomeUpdate(external_id="1").y.set(100)]
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_fail_4xx(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteCompoundAPIError) as e:
            API_CLIENT._update_multiple(
                cls=SomeResourceList,
                resource_path=URL_PATH,
                items=[SomeResource(id=0), SomeResource(external_id="abc")],
            )
        assert e.value.message == "Client Error"
        assert e.value.code == 400
        assert e.value.failed == [0, "abc"]

    def test_standard_update_fail_5xx(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=500, json={"error": {"message": "Server Error"}})
        with pytest.raises(CogniteCompoundAPIError) as e:
            API_CLIENT._update_multiple(
                cls=SomeResourceList,
                resource_path=URL_PATH,
                items=[SomeResource(id=0), SomeResource(external_id="abc")],
            )
        assert e.value.message == "Server Error"
        assert e.value.code == 500
        assert e.value.failed == []
        assert e.value.unknown == [0, "abc"]

    def test_cognite_client_is_set(self, mock_update):
        assert (
            COGNITE_CLIENT
            == API_CLIENT._update_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=SomeResource(id=0)
            )._cognite_client
        )
        assert (
            COGNITE_CLIENT
            == API_CLIENT._update_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(id=0)]
            )._cognite_client
        )

    def test_over_limit_concurrent(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"x": 3, "y": 4}]})

        with set_request_limit(API_CLIENT, 1):
            API_CLIENT._update_multiple(
                cls=SomeResourceList, resource_path=URL_PATH, items=[SomeResource(1, 2, id=1), SomeResource(3, 4, id=2)]
            )

        assert {"items": [{"id": 1, "update": {"y": {"set": 2}}}]} == jsgz_load(rsps.calls[0].request.body)
        assert {"items": [{"id": 2, "update": {"y": {"set": 4}}}]} == jsgz_load(rsps.calls[1].request.body)


class TestStandardSearch:
    def test_standard_search_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        res = API_CLIENT._search(
            cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter=SomeFilter(var_x=1, var_y=1),
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            rsps.calls[0].request.body
        )

    def test_standard_search_dict_filter_ok(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        res = API_CLIENT._search(
            cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter={"var_x": 1, "varY": 1},
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            rsps.calls[0].request.body
        )

    def test_standard_search_fail(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=400, json={"error": {"message": "Client Error"}})

        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            API_CLIENT._search(cls=SomeResourceList, resource_path=URL_PATH, search=None, filter=None, limit=None)
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_cognite_client_is_set(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        assert (
            COGNITE_CLIENT
            == API_CLIENT._search(
                cls=SomeResourceList, resource_path=URL_PATH, search={"name": "bla"}, filter={"name": "bla"}, limit=1000
            )._cognite_client
        )


class TestHelpers:
    @pytest.mark.parametrize(
        "input, emulator_url, expected",
        [
            (
                "http://localtest.com/api/1.0/projects/test-project/analytics/models",
                "http://localhost:8000/api/0.1",
                "http://localhost:8000/api/0.1/projects/test-project/models",
            ),
            (
                "http://localtest.com/api/1.0/projects/test-project/analytics/models/sourcepackages/1",
                "http://localhost:1234/api/0.5",
                "http://localhost:1234/api/0.5/projects/test-project/models/sourcepackages/1",
            ),
            (
                "https://api.cognitedata.com/api/0.6/projects/test-project/assets/update",
                "http://localhost:8000/api/0.1",
                "https://api.cognitedata.com/api/0.6/projects/test-project/assets/update",
            ),
            (
                "https://api.cognitedata.com/login/status",
                "http://localhost:8000/api/0.1",
                "https://api.cognitedata.com/login/status",
            ),
        ],
    )
    def test_nostromo_emulator_url_filter(self, input, emulator_url, expected):
        os.environ["MODEL_HOSTING_EMULATOR_URL"] = emulator_url
        assert expected == API_CLIENT._apply_model_hosting_emulator_url_filter(input)
        del os.environ["MODEL_HOSTING_EMULATOR_URL"]

    @pytest.fixture
    def mlh_emulator_mock(self, rsps):
        os.environ["MODEL_HOSTING_EMULATOR_URL"] = "http://localhost:8888/api/0.1"
        rsps.add(rsps.POST, "http://localhost:8888/api/0.1/projects/test-project/models/versions", status=200, json={})
        yield rsps
        del os.environ["MODEL_HOSTING_EMULATOR_URL"]

    @pytest.mark.usefixtures("mlh_emulator_mock")
    def test_do_request_with_mlh_emulator_activated(self):
        API_CLIENT._do_request(method="POST", url_path="/analytics/models/versions")

    @pytest.mark.parametrize(
        "ids, external_ids, wrap_ids, expected",
        [
            (1, None, False, [1]),
            ([1, 2], None, False, [1, 2]),
            (1, None, True, [{"id": 1}]),
            ([1, 2], None, True, [{"id": 1}, {"id": 2}]),
            (1, "1", True, [{"id": 1}, {"externalId": "1"}]),
            (1, ["1"], True, [{"id": 1}, {"externalId": "1"}]),
            ([1, 2], ["1"], True, [{"id": 1}, {"id": 2}, {"externalId": "1"}]),
            (None, "1", True, [{"externalId": "1"}]),
            (None, ["1", "2"], True, [{"externalId": "1"}, {"externalId": "2"}]),
        ],
    )
    def test_process_ids(self, ids, external_ids, wrap_ids, expected):
        assert expected == API_CLIENT._process_ids(ids, external_ids, wrap_ids)

    @pytest.mark.parametrize(
        "ids, external_ids, wrap_ids, exception, match",
        [
            (None, None, False, ValueError, "No ids specified"),
            (None, ["1", "2"], False, ValueError, "externalIds must be wrapped"),
            ([1], ["1"], False, ValueError, "externalIds must be wrapped"),
            ("1", None, False, TypeError, "must be int or list of int"),
            (1, 1, True, TypeError, "must be str or list of str"),
        ],
    )
    def test_process_ids_fail(self, ids, external_ids, wrap_ids, exception, match):
        with pytest.raises(exception, match=match):
            API_CLIENT._process_ids(ids, external_ids, wrap_ids)

    @pytest.mark.parametrize(
        "id, external_id, expected",
        [(1, None, True), (None, "1", True), (None, None, False), ([1], None, False), (None, ["1"], False)],
    )
    def test_is_single_identifier(self, id, external_id, expected):
        assert expected == API_CLIENT._is_single_identifier(id, external_id)
