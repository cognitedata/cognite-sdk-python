from __future__ import annotations

import json
import math
import random
import time
import unittest
from collections import namedtuple
from typing import Any, ClassVar, Literal, cast

import pytest
from more_itertools import flatten
from requests import Response
from responses import matchers

from cognite.client import CogniteClient, utils
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import Token
from cognite.client.data_classes import TimeSeries, TimeSeriesUpdate
from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from tests.utils import jsgz_load, set_request_limit

BASE_URL = "http://localtest.com/api/1.0/projects/test-project"
URL_PATH = "/someurl"

RESPONSE = {"any": "ok"}


@pytest.fixture(scope="class")
def api_client_with_token_factory(cognite_client):
    return APIClient(
        ClientConfig(
            client_name="any",
            project="test-project",
            base_url=BASE_URL,
            max_workers=1,
            headers={"x-cdp-app": "python-sdk-integration-tests"},
            credentials=Token(lambda: "abc"),
        ),
        api_version=None,
        cognite_client=cognite_client,
    )


@pytest.fixture(scope="class")
def api_client_with_token(cognite_client):
    return APIClient(
        ClientConfig(
            client_name="any",
            project="test-project",
            base_url=BASE_URL,
            max_workers=1,
            headers={"x-cdp-app": "python-sdk-integration-tests"},
            credentials=Token("abc"),
        ),
        api_version=None,
        cognite_client=cognite_client,
    )


RequestCase = namedtuple("RequestCase", ["name", "method", "kwargs"])


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

    request_cases: ClassVar = [
        lambda api_client: RequestCase(
            name="post", method=api_client._post, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}
        ),
        lambda api_client: RequestCase(name="get", method=api_client._get, kwargs={"url_path": URL_PATH}),
        lambda api_client: RequestCase(name="delete", method=api_client._delete, kwargs={"url_path": URL_PATH}),
        lambda api_client: RequestCase(
            name="put", method=api_client._put, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}
        ),
    ]

    @pytest.mark.parametrize("fn", request_cases)
    def test_requests_ok(self, fn, mock_all_requests_ok, api_client_with_token):
        name, method, kwargs = fn(api_client_with_token)
        response = method(**kwargs)
        assert response.status_code == 200
        assert response.json() == RESPONSE

        request_headers = mock_all_requests_ok.calls[0].request.headers
        assert "application/json" == request_headers["content-type"]
        assert "application/json" == request_headers["accept"]
        assert api_client_with_token._config.credentials.authorization_header()[1] == request_headers["Authorization"]
        assert "python-sdk-integration-tests" == request_headers["x-cdp-app"]
        assert "User-Agent" in request_headers

    @pytest.mark.usefixtures("mock_all_requests_fail")
    @pytest.mark.parametrize("fn", request_cases)
    def test_requests_fail(self, fn, api_client_with_token):
        name, method, kwargs = fn(api_client_with_token)
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
    def test_request_gzip_disabled(self, rsps, api_client_with_token):
        def check_gzip_disabled(request):
            assert "Content-Encoding" not in request.headers
            assert {"any": "OK"} == json.loads(request.body)
            return 200, {}, json.dumps(RESPONSE)

        for method in [rsps.PUT, rsps.POST]:
            rsps.add_callback(method, BASE_URL + URL_PATH, check_gzip_disabled)

        api_client_with_token._post(URL_PATH, {"any": "OK"}, headers={})
        api_client_with_token._put(URL_PATH, {"any": "OK"}, headers={})

    def test_request_gzip_enabled(self, rsps, api_client_with_token):
        def check_gzip_enabled(request):
            assert "Content-Encoding" in request.headers
            assert {"any": "OK"} == jsgz_load(request.body)
            return 200, {}, json.dumps(RESPONSE)

        for method in [rsps.PUT, rsps.POST]:
            rsps.add_callback(method, BASE_URL + URL_PATH, check_gzip_enabled)

        api_client_with_token._post(URL_PATH, {"any": "OK"}, headers={})
        api_client_with_token._put(URL_PATH, {"any": "OK"}, headers={})

    def test_headers_correct(self, mock_all_requests_ok, api_client_with_token):
        api_client_with_token._post(URL_PATH, {"any": "OK"}, headers={"additional": "stuff"})
        headers = mock_all_requests_ok.calls[0].request.headers

        assert "gzip, deflate" == headers["accept-encoding"]
        assert "gzip" == headers["content-encoding"]
        assert f"CognitePythonSDK:{utils._auxiliary.get_current_sdk_version()}" == headers["x-cdp-sdk"]
        assert "Bearer abc" == headers["Authorization"]
        assert "stuff" == headers["additional"]

    def test_headers_correct_with_token_factory(self, mock_all_requests_ok, api_client_with_token_factory):
        api_client_with_token_factory._post(URL_PATH, {"any": "OK"})
        headers = mock_all_requests_ok.calls[0].request.headers

        assert "api-key" not in headers
        assert api_client_with_token_factory._config.credentials.authorization_header()[1] == headers["Authorization"]

    def test_headers_correct_with_token(self, mock_all_requests_ok, api_client_with_token):
        api_client_with_token._post(URL_PATH, {"any": "OK"})
        headers = mock_all_requests_ok.calls[0].request.headers

        assert "api-key" not in headers
        assert api_client_with_token._config.credentials.authorization_header()[1] == headers["Authorization"]

    @pytest.mark.parametrize("payload", [math.nan, math.inf, -math.inf, {"foo": {"bar": {"baz": [[[math.nan]]]}}}])
    def test__do_request_raises_more_verbose_exception(self, api_client_with_token, payload):
        with pytest.raises(ValueError, match=r"contain NaN\(s\) or \+/\- Inf\!"):
            api_client_with_token._do_request("POST", URL_PATH, json=payload)

    def test__do_request_raises_unmodified_exception(self, api_client_with_token):
        # Create circular ref in payload to raise an arbitrary ValueError
        # we want to make sure we _don't_ modify:
        payload = []
        payload.append(payload)
        with pytest.raises(ValueError) as exc_info:
            api_client_with_token._do_request("POST", URL_PATH, json=payload)
        exc_msg = exc_info.value.args[0]
        assert "contain NaN(s) or +/- Inf!" not in exc_msg


class SomeUpdate(CogniteUpdate):
    @property
    def y(self):
        return PrimitiveUpdate(self, "y")

    @property
    def external_id(self):
        return PrimitiveUpdate(self, "externalId")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [PropertySpec("y", is_nullable=False), PropertySpec("external_id", is_nullable=False)]


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> SomeUpdate:
        return self._set(value)


class SomeResource(CogniteResource):
    def __init__(self, x=None, y=None, id=None, external_id=None, cognite_client=None):
        self.x = x
        self.y = y
        self.id = id
        self.external_id = external_id
        self._cognite_client = cast("CogniteClient", cognite_client)


class SomeResourceList(CogniteResourceList):
    _RESOURCE = SomeResource


class SomeFilter(CogniteFilter):
    def __init__(self, var_x, var_y):
        self.var_x = var_x
        self.var_y = var_y


class SomeAggregation(CogniteResource):
    def __init__(self, count):
        self.count = count


class TestStandardRetrieve:
    def test_standard_retrieve_OK(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=200, json={"x": 1, "y": 2})
        assert SomeResource(1, 2) == api_client_with_token._retrieve(
            cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1)
        )

    def test_standard_retrieve_not_found(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=404, json={"error": {"message": "Not Found."}})
        assert (
            api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1)) is None
        )

    def test_standard_retrieve_fail(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1))
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH + "/1", status=200, json={"x": 1, "y": 2})
        res = api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1))
        assert cognite_client == res._cognite_client


class TestStandardRetrieveMultiple:
    @pytest.fixture
    def mock_by_ids(self, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        yield rsps

    def test_by_id_wrap_OK(self, api_client_with_token, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, 2),
        )
        assert {"items": [{"id": 1}, {"id": 2}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_single_id_wrap_OK(self, api_client_with_token, mock_by_ids):
        assert SomeResource(1, 2) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1),
        )
        assert {"items": [{"id": 1}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_external_id_wrap_OK(self, api_client_with_token, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of("1", "2"),
        )
        assert {"items": [{"externalId": "1"}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_by_single_external_id_wrap_OK(self, api_client_with_token, mock_by_ids):
        assert SomeResource(1, 2) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of("1"),
        )
        assert {"items": [{"externalId": "1"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_retrieve_multiple_ignore_unknown(self, api_client_with_token, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, "2"),
            ignore_unknown_ids=True,
        )
        assert {"items": [{"id": 1}, {"externalId": "2"}], "ignoreUnknownIds": True} == jsgz_load(
            mock_by_ids.calls[0].request.body
        )

    def test_id_and_external_id_mixed(self, api_client_with_token, mock_by_ids):
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.load(ids=1, external_ids="2"),
        )
        assert {"items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.calls[0].request.body)

    def test_standard_retrieve_multiple_fail(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            api_client_with_token._retrieve_multiple(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                identifiers=IdentifierSequence.of(1, 2),
            )
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_ids_all_None(self, api_client_with_token):
        result = api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(),
        )

        assert isinstance(result, SomeResourceList)
        assert len(result) == 0

    def test_single_id_not_found(self, api_client_with_token, rsps):
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/byids",
            status=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 1}]}},
        )
        res = api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1),
        )
        assert res is None

    def test_multiple_ids_not_found(self, api_client_with_token, rsps):
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/byids",
            status=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 1}]}},
        )
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/byids",
            status=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 2}]}},
        )
        # Second request may be skipped intentionally depending on which thread runs when:
        rsps.assert_all_requests_are_fired = False

        with set_request_limit(api_client_with_token, 1):
            with pytest.raises(CogniteNotFoundError) as e:
                api_client_with_token._retrieve_multiple(
                    list_cls=SomeResourceList,
                    resource_cls=SomeResource,
                    resource_path=URL_PATH,
                    identifiers=IdentifierSequence.of(1, 2),
                )
        assert {"id": 1} in e.value.not_found
        assert {"id": 2} in e.value.not_found + e.value.skipped

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, mock_by_ids):
        res = api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, 2),
        )
        assert cognite_client == res._cognite_client

    def test_over_limit_concurrent(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/byids", status=200, json={"items": [{"x": 3, "y": 4}]})

        with set_request_limit(api_client_with_token, 1):
            api_client_with_token._retrieve_multiple(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                identifiers=IdentifierSequence.of(1, 2),
            )
        unittest.TestCase().assertCountEqual(
            [{"items": [{"id": 1}]}, {"items": [{"id": 2}]}],
            [jsgz_load(rsps.calls[0].request.body), jsgz_load(rsps.calls[1].request.body)],
        )


class TestStandardList:
    def test_standard_list_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert (
            SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump()
            == api_client_with_token._list(
                list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
            ).dump()
        )

    def test_standard_list_with_filter_GET_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert (
            SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump()
            == api_client_with_token._list(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                method="GET",
                filter={"filter": "bla"},
            ).dump()
        )
        assert "filter=bla" in rsps.calls[0].request.path_url

    def test_standard_list_with_filter_POST_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/list", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == api_client_with_token._list(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="POST",
            filter={"filter": "bla"},
        )
        assert {"filter": {"filter": "bla"}, "limit": 1000, "cursor": None} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_list_fail(self, api_client_with_token, rsps):
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            api_client_with_token._list(
                list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
            )
        assert 400 == e.value.code
        assert "Client Error" == e.value.message

    NUMBER_OF_ITEMS_FOR_AUTOPAGING = 11500
    ITEMS_TO_GET_WHILE_AUTOPAGING: ClassVar = [{"x": 1, "y": 1} for _ in range(NUMBER_OF_ITEMS_FOR_AUTOPAGING)]

    def test_list_partitions(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/list", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        res = api_client_with_token._list(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="POST",
            partitions=3,
            limit=None,
            headers={"X-Test": "foo"},
        )
        assert 6 == len(res)
        assert isinstance(res, SomeResourceList)
        assert isinstance(res[0], SomeResource)
        assert 3 == len(rsps.calls)
        assert {"1/3", "2/3", "3/3"} == {jsgz_load(c.request.body)["partition"] for c in rsps.calls}
        for call in rsps.calls:
            request = jsgz_load(call.request.body)
            assert "X-Test" in call.request.headers.keys()
            del request["partition"]
            assert {"cursor": None, "filter": {}, "limit": 1000} == request
            assert call.response.json()["items"] == [{"x": 1, "y": 2}, {"x": 1}]

    def test_list_partitions_with_failure(self, api_client_with_token, rsps):
        def request_callback(request):
            payload = jsgz_load(request.body)
            np, total = payload["partition"].split("/")
            if int(np) == 3:
                return 503, {}, json.dumps({"message": "Service Unavailable"})
            else:
                time.sleep(0.001)  # ensures bad luck race condition where 503 above executes last
                return 200, {}, json.dumps({"items": [{"x": 42, "y": 13}]})

        rsps.add_callback(
            rsps.POST, BASE_URL + URL_PATH + "/list", callback=request_callback, content_type="application/json"
        )
        with pytest.raises(CogniteAPIError) as exc:
            api_client_with_token._list(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                method="POST",
                partitions=10,
                limit=None,
            )
        assert 503 == exc.value.code
        assert exc.value.unknown == [("3/10",)]
        assert exc.value.skipped
        assert exc.value.successful
        assert 9 == len(exc.value.successful) + len(exc.value.skipped)
        assert 1 < len(rsps.calls)

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

    @pytest.fixture
    def mock_get_for_autopaging_2589(self, rsps):
        NUM_ITEMS = 2589
        ITEMS_EDGECASE = [{"x": 1, "y": 1} for _ in range(NUM_ITEMS)]

        def callback(request):
            params = {elem.split("=")[0]: elem.split("=")[1] for elem in request.path_url.split("?")[-1].split("&")}
            limit = int(params["limit"])
            cursor = int(params.get("cursor") or 0)
            items = ITEMS_EDGECASE[cursor : cursor + limit]
            if cursor + limit >= NUM_ITEMS:
                next_cursor = None
            else:
                next_cursor = cursor + limit
            response = json.dumps({"nextCursor": next_cursor, "items": items})
            return 200, {}, response

        rsps.add_callback(rsps.GET, BASE_URL + URL_PATH, callback)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator(self, api_client_with_token):
        total_resources = 0
        for resource in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        ):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 11500 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_limit(self, api_client_with_token):
        total_resources = 0
        for resource in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", limit=10000
        ):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_chunk_size(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", chunk_size=1000
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            if len(resource_chunk) == 1000:
                total_resources += 1000
            elif len(resource_chunk) == 500:
                total_resources += 500
            else:
                raise ValueError("resource chunk length was not 1000 or 500")
        assert 11500 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging_2589")
    def test_standard_list_generator_with_chunk_size_chunk_edge_case(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", chunk_size=2500
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            total_resources += len(resource_chunk)
            assert len(resource_chunk) in [89, 2500]
        assert 2589 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging_2589")
    def test_standard_list_generator_with_chunk_size_chunk_limit(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            chunk_size=2500,
            limit=2563,
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            total_resources += len(resource_chunk)
            assert len(resource_chunk) in [63, 2500]
        assert 2563 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_chunk_size_with_limit(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            limit=10000,
            chunk_size=1000,
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            assert 1000 == len(resource_chunk)
            total_resources += 1000
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_with_chunk_size_below_default_limit_and_global_limit(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            limit=1000,
            chunk_size=100,
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            assert 100 == len(resource_chunk)
            total_resources += 100
        assert 1000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator__chunk_size_exceeds_max(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            limit=2002,
            chunk_size=1001,
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            assert 1001 == len(resource_chunk)
            total_resources += 1001
        assert 2002 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_generator_vs_partitions(self, api_client_with_token):
        total_resources = 0
        for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            partitions=1,
            limit=2000,
            chunk_size=1001,
        ):
            # TODO: chunk_size is ignored when partitions is set, fix in next major version
            assert isinstance(resource_chunk, SomeResource)
            total_resources += 1

        assert 2000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging(self, api_client_with_token):
        res = api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        )
        assert self.NUMBER_OF_ITEMS_FOR_AUTOPAGING == len(res)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    def test_standard_list_autopaging_with_limit(self, api_client_with_token):
        res = api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", limit=5333
        )
        assert 5333 == len(res)

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/list", status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        rsps.add(rsps.GET, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        res = api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="POST"
        )
        assert cognite_client == res._cognite_client

        res = api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        )
        assert cognite_client == res._cognite_client


class TestStandardAggregate:
    def test_standard_aggregate_OK(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/aggregate", status=200, json={"items": [{"count": 1}]})
        assert [SomeAggregation(1)] == api_client_with_token._aggregate(
            resource_path=URL_PATH, filter={"x": 1}, cls=SomeAggregation
        )

    def test_standard_aggregate_fail(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/aggregate", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            api_client_with_token._aggregate(resource_path=URL_PATH, filter={"x": 1}, cls=SomeAggregation)
        assert "Client Error" == e.value.message
        assert 400 == e.value.code


class TestStandardCreate:
    def test_standard_create_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        res = api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 1), SomeResource(1)],
        )
        assert {"items": [{"x": 1, "y": 1}, {"x": 1}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResource(1, 2) == res[0]
        assert SomeResource(1) == res[1]

    def test_standard_create_extra_body_fields(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]})
        api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 1), SomeResource(1)],
            extra_body_fields={"foo": "bar"},
        )
        assert {"items": [{"x": 1, "y": 1}, {"x": 1}], "foo": "bar"} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_create_single_item_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        res = api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=SomeResource(1, 2)
        )
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResource(1, 2) == res

    def test_standard_create_single_item_in_list_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        res = api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=[SomeResource(1, 2)]
        )
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(rsps.calls[0].request.body)
        assert SomeResourceList([SomeResource(1, 2)]) == res

    def test_standard_create_fail(self, api_client_with_token, rsps):
        def callback(request):
            item = jsgz_load(request.body)["items"][0]
            return int(item["externalId"]), {}, json.dumps({})

        rsps.add_callback(rsps.POST, BASE_URL + URL_PATH, callback=callback, content_type="application/json")
        with set_request_limit(api_client_with_token, 1):
            with pytest.raises(CogniteAPIError) as e:
                api_client_with_token._create_multiple(
                    list_cls=SomeResourceList,
                    resource_cls=SomeResource,
                    resource_path=URL_PATH,
                    items=[
                        # The external id here is also used as the fake api response status code:
                        SomeResource(1, external_id="400"),  # i.e, this will raise CogniteAPIError(..., code=400)
                        SomeResource(external_id="500"),
                        SomeResource(1, 1, external_id="200"),
                    ],
                )
        assert e.value.code in (400, 500)  # race condition, don't know which failing is -last-
        assert [SomeResource(1, external_id="400")] == e.value.failed
        assert [SomeResource(1, 1, external_id="200")] == e.value.successful
        assert [SomeResource(external_id="500")] == e.value.unknown

    def test_standard_create_concurrent(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 3, "y": 4}]})

        res = api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 2), SomeResource(3, 4)],
            limit=1,
        )
        expected_res_lst = SomeResourceList([SomeResource(1, 2), SomeResource(3, 4)])
        unittest.TestCase().assertCountEqual(expected_res_lst, res)

        expected_item_bodies = [{"items": [{"x": 1, "y": 2}]}, {"items": [{"x": 3, "y": 4}]}]
        gotten_item_bodies = [jsgz_load(rsps.calls[0].request.body), jsgz_load(rsps.calls[1].request.body)]
        unittest.TestCase().assertCountEqual(expected_item_bodies, gotten_item_bodies)

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH, status=200, json={"items": [{"x": 1, "y": 2}]})
        assert (
            cognite_client
            == api_client_with_token._create_multiple(
                list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=SomeResource()
            )._cognite_client
        )
        assert (
            cognite_client
            == api_client_with_token._create_multiple(
                list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=[SomeResource()]
            )._cognite_client
        )


class TestStandardDelete:
    def test_standard_delete_multiple_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
        )
        assert {"items": [1, 2]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_ok__single_id(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of(1)
        )
        assert {"items": [1]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_ok__single_id_in_list(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1])
        )
        assert {"items": [1]} == jsgz_load(rsps.calls[0].request.body)

    def test_standard_delete_multiple_fail_4xx(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError) as e:
            api_client_with_token._delete_multiple(
                resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
            )
        assert 400 == e.value.code
        assert "Client Error" == e.value.message
        assert e.value.failed == [1, 2]

    def test_standard_delete_multiple_fail_5xx(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=500, json={"error": {"message": "Server Error"}})
        with pytest.raises(CogniteAPIError) as e:
            api_client_with_token._delete_multiple(
                resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
            )
        assert 500 == e.value.code
        assert "Server Error" == e.value.message
        assert e.value.unknown == [1, 2]
        assert e.value.failed == []

    def test_standard_delete_multiple_fail_missing_ids(self, api_client_with_token, rsps):
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/delete",
            status=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 1}]}},
        )
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/delete",
            status=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 3}]}},
        )
        with set_request_limit(api_client_with_token, 2):
            with pytest.raises(CogniteNotFoundError) as e:
                api_client_with_token._delete_multiple(
                    resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2, 3])
                )

        unittest.TestCase().assertCountEqual([{"id": 1}, {"id": 3}], e.value.not_found)
        assert [1, 2, 3] == sorted(e.value.failed)

    def test_over_limit_concurrent(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/delete", status=200, json={})

        with set_request_limit(api_client_with_token, 2):
            api_client_with_token._delete_multiple(
                resource_path=URL_PATH, identifiers=IdentifierSequence.of([1, 2, 3, 4]), wrap_ids=False
            )
        unittest.TestCase().assertCountEqual(
            [{"items": [1, 2]}, {"items": [3, 4]}],
            [jsgz_load(rsps.calls[0].request.body), jsgz_load(rsps.calls[1].request.body)],
        )


class TestStandardUpdate:
    @pytest.fixture
    def mock_update(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"id": 1, "x": 1, "y": 100}]})
        yield rsps

    def test_standard_update_with_cognite_resource_OK(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, y=100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_resource__subject_to_camel_case_issue(
        self, api_client_with_token, mock_update
    ):
        api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, external_id="abc", y=100)],
        )
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}, "externalId": {"set": "abc"}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_with_cognite_resource__non_update_attributes(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, y=100, x=1)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_resource__id_and_external_id_set(self, api_client_with_token, mock_update):
        api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, external_id="1", y=100, x=1)],
        )
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}, "externalId": {"set": "1"}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_with_cognite_resource_and_external_id_OK(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(external_id="1", y=100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_with_cognite_update_object_OK(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeUpdate(id=1).y.set(100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_single_object(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=SomeUpdate(id=1).y.set(100),
        )
        assert SomeResource(id=1, x=1, y=100) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.calls[0].request.body)

    def test_standard_update_with_cognite_update_object_and_external_id_OK(self, api_client_with_token, mock_update):
        res = api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeUpdate(external_id="1").y.set(100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.calls[0].request.body
        )

    def test_standard_update_fail_4xx(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=400, json={"error": {"message": "Client Error"}})
        with pytest.raises(CogniteAPIError) as e:
            api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=[SomeResource(id=0), SomeResource(external_id="abc")],
            )
        assert e.value.message == "Client Error"
        assert e.value.code == 400
        assert e.value.failed == [0, "abc"]

    def test_standard_update_fail_5xx(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=500, json={"error": {"message": "Server Error"}})
        with pytest.raises(CogniteAPIError) as e:
            api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=[SomeResource(id=0), SomeResource(external_id="abc")],
            )
        assert e.value.message == "Server Error"
        assert e.value.code == 500
        assert e.value.failed == []
        assert e.value.unknown == [0, "abc"]

    def test_standard_update_fail_missing_and_5xx(self, api_client_with_token, rsps, monkeypatch):
        # Note 1: We have two tasks being added to an executor, but that doesn't mean we know the
        # execution order. Depending on whether the 400 or 500 hits the first or second task,
        # the following asserts fail (ordering issue). Thus, we use 'matchers.json_params_matcher'
        # to make sure the responses match the two tasks.

        # Note 2: The matcher function expects request.body to not be gzipped (it just does .decode("utf-8")
        # which fails, making the matching functions useless.. so we temporarily turn off gzip for this test
        monkeypatch.setattr(global_config, "disable_gzip", True)

        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/update",
            status=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 0}]}},
            match=[matchers.json_params_matcher({"items": [{"update": {}, "id": 0}]})],
        )
        rsps.add(
            rsps.POST,
            BASE_URL + URL_PATH + "/update",
            status=500,
            json={"error": {"message": "Server Error"}},
            match=[matchers.json_params_matcher({"items": [{"update": {}, "externalId": "abc"}]})],
        )
        items = [SomeResource(external_id="abc"), SomeResource(id=0)]
        random.shuffle(items)

        with set_request_limit(api_client_with_token, 1):
            with pytest.raises(CogniteAPIError) as e:
                api_client_with_token._update_multiple(
                    update_cls=SomeUpdate,
                    list_cls=SomeResourceList,
                    resource_cls=SomeResource,
                    resource_path=URL_PATH,
                    items=items,
                )
        assert ["abc"] == e.value.unknown
        assert [0] == e.value.failed
        assert [{"id": 0}] == e.value.missing

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, mock_update):
        assert (
            cognite_client
            == api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=SomeResource(id=0),
            )._cognite_client
        )
        assert (
            cognite_client
            == api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=[SomeResource(id=0)],
            )._cognite_client
        )

    def test_over_limit_concurrent(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"x": 1, "y": 2}]})
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/update", status=200, json={"items": [{"x": 3, "y": 4}]})

        with set_request_limit(api_client_with_token, 1):
            api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=[SomeResource(1, 2, id=1), SomeResource(3, 4, id=2)],
            )
        unittest.TestCase().assertCountEqual(
            [{"items": [{"id": 1, "update": {"y": {"set": 2}}}]}, {"items": [{"id": 2, "update": {"y": {"set": 4}}}]}],
            [jsgz_load(rsps.calls[0].request.body), jsgz_load(rsps.calls[1].request.body)],
        )


class TestStandardSearch:
    def test_standard_search_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        res = api_client_with_token._search(
            list_cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter=SomeFilter(var_x=1, var_y=1),
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            rsps.calls[0].request.body
        )

    def test_standard_search_dict_filter_ok(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        res = api_client_with_token._search(
            list_cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter={"var_x": 1, "varY": 1},
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            rsps.calls[0].request.body
        )

    def test_standard_search_fail(self, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=400, json={"error": {"message": "Client Error"}})

        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            api_client_with_token._search(
                list_cls=SomeResourceList, resource_path=URL_PATH, search=None, filter=None, limit=None
            )
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    def test_cognite_client_is_set(self, cognite_client, api_client_with_token, rsps):
        rsps.add(rsps.POST, BASE_URL + URL_PATH + "/search", status=200, json={"items": [{"x": 1, "y": 2}]})

        assert (
            cognite_client
            == api_client_with_token._search(
                list_cls=SomeResourceList,
                resource_path=URL_PATH,
                search={"name": "bla"},
                filter={"name": "bla"},
                limit=1000,
            )._cognite_client
        )


def convert_resource_to_patch_object_test_cases():
    yield pytest.param(
        # Is String is ignored as it cannot be updated.
        TimeSeries(id=123, name="bla", is_string=False),
        TimeSeriesUpdate._get_update_properties(),
        "patch",
        {"id": 123, "update": {"name": {"set": "bla"}}},
        id="Patch TimeSeries",
    )
    yield pytest.param(
        TimeSeries(id=42, description="updated"),
        TimeSeriesUpdate._get_update_properties(),
        "replace_ignore_null",
        {"id": 42, "update": {"description": {"set": "updated"}}},
        id="Replace TimeSeries with ignore null",
    )
    yield pytest.param(
        TimeSeries(id=42, metadata={"myNew": "metadataValue"}),
        TimeSeriesUpdate._get_update_properties(),
        "patch",
        {"id": 42, "update": {"metadata": {"add": {"myNew": "metadataValue"}}}},
        id="Patch TimeSeries with container property",
    )


class TestRetryableEndpoints:
    @pytest.mark.parametrize(
        "method, path, expected",
        flatten(
            [
                # Should retry POST on all _read_ endpoints
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/list", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/byids", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/search", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/list", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/byids", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/aggregate", True),
                # Should not retry POST /create and /update as they are not idempotent
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}", False),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/update", False),
            ]
            for resource in [
                "assets",
                "events",
                "files",
                "timeseries",
                "sequences",
                "datasets",
                "relationships",
                "labels",
            ]
        ),
    )
    def test_is_retryable_resource_api_endpoints(self, api_client_with_token, method, path, expected):
        assert expected == api_client_with_token._is_retryable(method, path)

    @pytest.mark.parametrize(
        "method, path, expected",
        sorted(
            [
                ### Versions
                *(
                    # Should work on all api version
                    ("POST", f"https://api.cognitedata.com/api/{version}/projects/bla/assets/list", True)
                    for version in ["v1", "playground"]
                ),
                ### Hosts
                *(
                    # Should work on all hosts
                    ("POST", f"https://{host}/api/v1/projects/bla/assets/list", True)
                    for host in ["api.cognitedata.com", "greenfield.cognitedata.com", "localhost:8000"]
                ),
                ### Methods
                *(
                    # Should by default retry GET, PUT, and PATCH
                    (method, "https://api.cognitedata.com/api/v1/projects/bla", True)
                    for method in {"GET", "PUT", "PATCH"}
                ),
                ### Annotations
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/suggest", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/reverselookup", True),
                ### Functions
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/status", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/delete", False),
                ### Function calls
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/123/call", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/123/calls/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/xyz/calls/byids", False),
                ### Function schedules
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules/delete", False),
                ### User Profiles
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles/search", True),
                ### Documents
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/aggregate", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/search", True),
                ### 3D models
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/delete", False),
                ### 3D model revisions
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/12/revisions/34/nodes/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/12/revisions/ab/nodes/list", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions/56/nodes/byids", True),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions/56/nodes/byXids",
                    False,
                ),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions/cd/nodes/byids",
                    False,
                ),
                ### 3D asset mappings
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/56/revisions/78/mappings", False),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/bla/3d/models/56/revisions/78/mappings/list",
                    True,
                ),
                #### Files
                ("POST", "https://api.c.com/api/v1/projects/bla/files/downloadlink?extendedExpiration=true", True),
                #### Timeseries
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data/delete", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data/latest", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/synthetic/query", True),
                #### Sequences
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/sequences/data", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/sequences/data/delete", True),
                #### Data modeling
                *flatten(
                    # should retry _all_ data modeling schema endpoints as they are idempotent.
                    [
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/list", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/byids", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/delete", True),
                    ]
                    for resource in ("spaces", "containers", "views", "datamodels")
                ),
                # Retry all data modeling instances endpoints as they are idempotent
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/models/instances", True),
                *(
                    ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/instances/{endpoint}", True)
                    for endpoint in ("list", "byids", "delete", "aggregate", "search")
                ),
                # Retry for RAW on rows but not on dbs or tables as only the rows endpoints are idempotent
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db/tables/t", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db/tables/t/rows", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db/tables/t/rows/delete", True),
            ]
        ),
    )
    def test_is_retryable(self, api_client_with_token, method, path, expected):
        assert expected == api_client_with_token._is_retryable(method, path)

    @pytest.mark.parametrize(
        "method, path", [("POST", "htt://bla/bla"), ("BLOP", "http://localhost:8000/token/inspect")]
    )
    def test_is_retryable_should_fail(self, api_client_with_token, method, path):
        with pytest.raises(ValueError, match="is not valid"):
            api_client_with_token._is_retryable(method, path)

    def test_is_retryable_add(self, api_client_with_token, monkeypatch: pytest.MonkeyPatch):
        rperp = APIClient._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS | {"/assets/bloop"}
        monkeypatch.setattr(APIClient, "_RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS", rperp)

        test_url = "https://greenfield.cognitedata.com/api/v1/projects/blabla/assets/bloop"
        assert api_client_with_token._is_retryable("POST", test_url) is True


class TestHelpers:
    @pytest.mark.parametrize(
        "before, after",
        [
            ({"api-key": "bla", "key": "bla"}, {"api-key": "***", "key": "bla"}),
            ({"Authorization": "bla", "key": "bla"}, {"Authorization": "***", "key": "bla"}),
        ],
    )
    def test_sanitize_headers(self, before, after):
        assert before != after
        APIClient._sanitize_headers(before)
        assert before == after

    @pytest.mark.parametrize(
        "content, expected",
        [(b'{"foo": 42}', '{"foo": 42}'), (b"foobar", "foobar"), (b"\xed\xbc\xad", "<binary>")],
    )
    def test_get_response_content_safe(self, content, expected):
        res = Response()
        res._content = content
        assert APIClient._get_response_content_safe(res) == expected

    @pytest.mark.parametrize(
        "resource, update_attributes, mode, expected_object", list(convert_resource_to_patch_object_test_cases())
    )
    def test_convert_resource_to_patch_object(
        self,
        resource: CogniteResource,
        update_attributes: list[PropertySpec],
        mode: Literal["replace_ignore_null", "patch", "replace"],
        expected_object: dict[str, dict[str, dict]],
    ):
        actual = APIClient._convert_resource_to_patch_object(resource, update_attributes, mode)

        assert actual == expected_object


class TestConnectionPooling:
    def test_connection_pool_is_shared_between_clients(self):
        cnf = ClientConfig(client_name="bla", credentials=Token("bla"), project="bla")
        c1 = CogniteClient(cnf)
        c2 = CogniteClient(cnf)
        assert (
            c1._api_client._http_client.session
            == c2._api_client._http_client.session
            == c1._api_client._http_client_with_retry.session
            == c2._api_client._http_client_with_retry.session
        )


def test_worker_in_backoff_loop_gets_new_token(rsps):
    url = "https://api.cognitedata.com/api/v1/projects/c/assets/byids"
    rsps.add(rsps.POST, url, status=429, json={"error": "Backoff plz"})
    rsps.add(rsps.POST, url, status=200, json={"items": [{"id": 123}]})

    call_count = 0

    def token_callable():
        nonlocal call_count
        if call_count < 1:
            call_count += 1
            return "outdated-token"
        return "valid-token"

    client = CogniteClient(ClientConfig(client_name="a", credentials=Token(token_callable), project="c"))
    assert client.assets.retrieve(id=1).id == 123
    assert call_count > 0
    assert rsps.calls[0].request.headers["Authorization"] == "Bearer outdated-token"
    assert rsps.calls[1].request.headers["Authorization"] == "Bearer valid-token"


@pytest.mark.parametrize("limit, expected_error", ((-2, ValueError), (0, ValueError), ("10", TypeError)))
def test_list_and_search__bad_limit_value_raises(limit, expected_error, cognite_client):
    with pytest.raises(expected_error):
        cognite_client.assets.list(limit=limit)
    with pytest.raises(expected_error):
        cognite_client.assets.search(limit=limit)
