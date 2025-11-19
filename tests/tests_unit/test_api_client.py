from __future__ import annotations

import asyncio
import json
import math
import random
import re
import unittest
from collections import namedtuple
from collections.abc import Callable, Iterator
from typing import Any, ClassVar, Literal, cast

import pytest
from httpx import Headers, Request, Response
from pytest_httpx import HTTPXMock
from typing_extensions import Self

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import Token
from cognite.client.data_classes import TimeSeriesUpdate
from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
)
from cognite.client.data_classes.hosted_extractors import MQTT5SourceUpdate, MQTT5SourceWrite
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._url import validate_url_and_return_retryability
from tests.tests_unit.conftest import DefaultResourceGenerator
from tests.utils import get_or_raise, get_wrapped_async_client, jsgz_load

BASE_URL = "http://localtest.com/api/v1/projects/test-project"
URL_PATH = "/someurl"

RESPONSE = {"any": "ok"}


@pytest.fixture(scope="class")
def api_client_with_token_factory(async_client: AsyncCogniteClient) -> APIClient:
    return APIClient(
        ClientConfig(
            client_name="any",
            project="test-project",
            base_url=BASE_URL,
            headers={"x-cdp-app": "python-sdk-integration-tests"},
            credentials=Token(lambda: "abc"),
        ),
        api_version=None,
        cognite_client=async_client,
    )


@pytest.fixture(scope="class")
def api_client_with_token(async_client: AsyncCogniteClient) -> APIClient:
    return APIClient(
        ClientConfig(
            client_name="any",
            project="test-project",
            base_url=BASE_URL,
            headers={"x-cdp-app": "python-sdk-integration-tests"},
            credentials=Token("abc"),
        ),
        api_version=None,
        cognite_client=async_client,
    )


RequestCase = namedtuple("RequestCase", ["name", "method", "kwargs"])


class TestBasicRequests:
    @pytest.fixture
    def mock_all_requests_ok(self, httpx_mock: HTTPXMock) -> Iterator[HTTPXMock]:
        for method in ["GET", "PUT", "POST", "DELETE"]:
            httpx_mock.add_response(
                method=method, url=BASE_URL + URL_PATH, status_code=200, json=RESPONSE, is_optional=True
            )
        yield httpx_mock

    @pytest.fixture
    def mock_all_requests_fail(self, httpx_mock: HTTPXMock) -> None:
        for method in ["GET", "PUT", "POST", "DELETE"]:
            httpx_mock.add_response(
                method=method,
                url=BASE_URL + URL_PATH,
                status_code=400,
                json={"error": "Client error"},
                is_optional=True,
            )
            httpx_mock.add_response(
                method=method, url=BASE_URL + URL_PATH, status_code=500, text="Server error", is_optional=True
            )
            httpx_mock.add_response(
                method=method,
                url=BASE_URL + URL_PATH,
                status_code=500,
                json={"error": "Server error"},
                is_optional=True,
            )
            httpx_mock.add_response(
                method=method,
                url=BASE_URL + URL_PATH,
                status_code=400,
                json={"error": {"code": 400, "message": "Client error"}},
                is_optional=True,
            )

    request_cases: ClassVar = [
        lambda api_client: RequestCase(
            name="post", method=api_client._post, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}
        ),
        lambda api_client: RequestCase(name="get", method=api_client._get, kwargs={"url_path": URL_PATH}),
        lambda api_client: RequestCase(
            name="put", method=api_client._put, kwargs={"url_path": URL_PATH, "json": {"any": "ok"}}
        ),
    ]

    @pytest.mark.parametrize("fn", request_cases)
    async def test_requests_ok(
        self, fn: Any, mock_all_requests_ok: HTTPXMock, api_client_with_token: APIClient
    ) -> None:
        name, method, kwargs = fn(api_client_with_token)
        response = await method(**kwargs)
        assert response.status_code == 200
        assert response.json() == RESPONSE

        request_headers = mock_all_requests_ok.get_requests()[0].headers
        assert "application/json" == request_headers["content-type"]
        assert "application/json" == request_headers["accept"]
        assert api_client_with_token._config.credentials.authorization_header()[1] == request_headers["Authorization"]
        assert "python-sdk-integration-tests" == request_headers["x-cdp-app"]
        assert "User-Agent" in request_headers

    @pytest.mark.usefixtures("mock_all_requests_fail")
    @pytest.mark.parametrize("fn", request_cases)
    async def test_requests_fail(self, fn: Any, api_client_with_token: APIClient) -> None:
        name, method, kwargs = fn(api_client_with_token)
        with pytest.raises(CogniteAPIError, match="Client error") as e:
            await method(**kwargs)
        assert e.value.code == 400

        with pytest.raises(CogniteAPIError, match="Server error") as e:
            await method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(CogniteAPIError, match="Server error") as e:
            await method(**kwargs)
        assert e.value.code == 500

        with pytest.raises(CogniteAPIError, match=re.escape("Client error | code: 400 | X-Request-ID:")) as e:
            await method(**kwargs)
        assert e.value.code == 400
        assert e.value.message == "Client error"

    @pytest.mark.usefixtures("disable_gzip")
    async def test_request_gzip_disabled(self, httpx_mock: HTTPXMock, api_client_with_token: APIClient) -> None:
        def check_gzip_disabled(request: Any) -> Response:
            assert "Content-Encoding" not in request.headers
            assert {"any": "OK"} == json.loads(request.content)
            return Response(200, headers={}, json=RESPONSE)

        for method in ["PUT", "POST"]:
            httpx_mock.add_callback(check_gzip_disabled, method=method, url=BASE_URL + URL_PATH)

        await api_client_with_token._post(URL_PATH, json={"any": "OK"}, headers={})
        await api_client_with_token._put(URL_PATH, json={"any": "OK"}, headers={})

    async def test_request_gzip_enabled(self, httpx_mock: HTTPXMock, api_client_with_token: APIClient) -> None:
        def check_gzip_enabled(request: Any) -> Response:
            assert "Content-Encoding" in request.headers
            assert {"any": "OK"} == jsgz_load(request.content)
            return Response(200, headers={}, json=RESPONSE)

        for method in ["PUT", "POST"]:
            httpx_mock.add_callback(check_gzip_enabled, method=method, url=BASE_URL + URL_PATH)

        await api_client_with_token._post(URL_PATH, json={"any": "OK"}, headers={})
        await api_client_with_token._put(URL_PATH, json={"any": "OK"}, headers={})

    async def test_headers_correct(self, mock_all_requests_ok: HTTPXMock, api_client_with_token: APIClient) -> None:
        from cognite.client import __version__

        await api_client_with_token._post(URL_PATH, {"any": "OK"}, headers={"additional": "stuff"})
        headers = mock_all_requests_ok.get_requests()[0].headers

        assert {"gzip", "deflate"} <= set(headers["accept-encoding"].split(", "))
        assert "gzip" == headers["content-encoding"]
        assert f"CognitePythonSDK:{__version__}" == headers["x-cdp-sdk"]
        assert "Bearer abc" == headers["Authorization"]
        assert "stuff" == headers["additional"]

    async def test_headers_correct_with_token_factory(
        self, mock_all_requests_ok: HTTPXMock, api_client_with_token_factory: APIClient
    ) -> None:
        await api_client_with_token_factory._post(URL_PATH, {"any": "OK"})
        headers = mock_all_requests_ok.get_requests()[0].headers

        assert "api-key" not in headers
        assert api_client_with_token_factory._config.credentials.authorization_header()[1] == headers["Authorization"]

    async def test_headers_correct_with_token(
        self, mock_all_requests_ok: HTTPXMock, api_client_with_token: APIClient
    ) -> None:
        await api_client_with_token._post(URL_PATH, {"any": "OK"})
        headers = mock_all_requests_ok.get_requests()[0].headers

        assert "api-key" not in headers
        assert api_client_with_token._config.credentials.authorization_header()[1] == headers["Authorization"]

    @pytest.mark.parametrize("payload", [math.nan, math.inf, -math.inf, {"foo": {"bar": {"baz": [[[math.nan]]]}}}])
    async def test__request_raises_more_verbose_exception(self, api_client_with_token: APIClient, payload: Any) -> None:
        with pytest.raises(ValueError, match=r"contain NaN\(s\) or \+/\- Inf\!"):
            await api_client_with_token._post(URL_PATH, json=payload)

    async def test__request_raises_unmodified_exception(self, api_client_with_token: APIClient) -> None:
        # Create circular ref in payload to raise an arbitrary ValueError
        # we want to make sure we _don't_ modify:
        payload: list = []
        payload.append(payload)
        with pytest.raises(ValueError) as exc_info:
            await api_client_with_token._post(URL_PATH, json=payload)  # type: ignore[arg-type]
        exc_msg = exc_info.value.args[0]
        assert "contain NaN(s) or +/- Inf!" not in exc_msg


class SomeUpdate(CogniteUpdate):
    @property
    def y(self) -> PrimitiveUpdate:
        return PrimitiveUpdate(self, "y")

    @property
    def external_id(self) -> PrimitiveUpdate:
        return PrimitiveUpdate(self, "externalId")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [PropertySpec("y", is_nullable=False), PropertySpec("external_id", is_nullable=False)]


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> SomeUpdate:
        return self._set(value)


class SomeResource(WriteableCogniteResource):
    def __init__(
        self,
        x: int | None = None,
        y: int | None = None,
        id: int | None = None,
        external_id: str | None = None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.x = x
        self.y = y
        self.id = id
        self.external_id = external_id
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            x=resource.get("x"),
            y=resource.get("y"),
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            cognite_client=cognite_client,
        )

    def as_write(self) -> SomeResource:
        return self


class SomeResourceList(CogniteResourceList):
    _RESOURCE = SomeResource


class SomeFilter(CogniteFilter):
    def __init__(self, var_x: Any, var_y: Any) -> None:
        self.var_x = var_x
        self.var_y = var_y


class TestStandardRetrieve:
    async def test_standard_retrieve_ok(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="GET", url=BASE_URL + URL_PATH + "/1", status_code=200, json={"x": 1, "y": 2})
        assert SomeResource(1, 2) == await api_client_with_token._retrieve(
            cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1)
        )

    async def test_standard_retrieve_not_found(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="GET", url=BASE_URL + URL_PATH + "/1", status_code=404, json={"error": {"message": "Not Found."}}
        )
        assert (
            await api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1))
            is None
        )

    async def test_standard_retrieve_fail(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="GET", url=BASE_URL + URL_PATH + "/1", status_code=400, json={"error": {"message": "Client Error"}}
        )
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            await api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1))
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="GET", url=BASE_URL + URL_PATH + "/1", status_code=200, json={"x": 1, "y": 2})
        res = await api_client_with_token._retrieve(cls=SomeResource, resource_path=URL_PATH, identifier=Identifier(1))
        assert res
        assert async_client is res._cognite_client


class TestStandardRetrieveMultiple:
    @pytest.fixture
    def mock_by_ids(self, httpx_mock: HTTPXMock) -> HTTPXMock:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/byids",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        return httpx_mock

    async def test_by_id_wrap_OK(self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock) -> None:
        assert SomeResourceList(
            [SomeResource(1, 2), SomeResource(1)]
        ) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, 2),
        )
        assert {"items": [{"id": 1}, {"id": 2}]} == jsgz_load(mock_by_ids.get_requests()[0].content)

    async def test_by_single_id_wrap_OK(self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock) -> None:
        assert SomeResource(1, 2) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1),
        )
        assert {"items": [{"id": 1}]} == jsgz_load(mock_by_ids.get_requests()[0].content)

    async def test_by_external_id_wrap_OK(self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock) -> None:
        assert SomeResourceList(
            [SomeResource(1, 2), SomeResource(1)]
        ) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of("1", "2"),
        )
        assert {"items": [{"externalId": "1"}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.get_requests()[0].content)

    async def test_by_single_external_id_wrap_OK(
        self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock
    ) -> None:
        assert SomeResource(1, 2) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of("1"),
        )
        assert {"items": [{"externalId": "1"}]} == jsgz_load(mock_by_ids.get_requests()[0].content)

    async def test_retrieve_multiple_ignore_unknown(
        self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock
    ) -> None:
        assert SomeResourceList(
            [SomeResource(1, 2), SomeResource(1)]
        ) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, "2"),
            ignore_unknown_ids=True,
        )
        assert {"items": [{"id": 1}, {"externalId": "2"}], "ignoreUnknownIds": True} == jsgz_load(
            mock_by_ids.get_requests()[0].content
        )

    async def test_id_and_external_id_mixed(self, api_client_with_token: APIClient, mock_by_ids: HTTPXMock) -> None:
        assert SomeResourceList(
            [SomeResource(1, 2), SomeResource(1)]
        ) == await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.load(ids=1, external_ids="2"),
        )
        assert {"items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(mock_by_ids.get_requests()[0].content)

    async def test_standard_retrieve_multiple_fail(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/byids",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            await api_client_with_token._retrieve_multiple(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                identifiers=IdentifierSequence.of(1, 2),
            )
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    async def test_ids_all_None(self, api_client_with_token: APIClient) -> None:
        result = await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(),
        )

        assert isinstance(result, SomeResourceList)
        assert len(result) == 0

    async def test_single_id_not_found(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/byids",
            status_code=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 1}]}},
        )
        res = await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1),
        )
        assert res is None

    async def test_multiple_ids_not_found(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/byids",
            status_code=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 1}]}},
        )
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/byids",
            status_code=400,
            json={"error": {"message": "Not Found", "missing": [{"id": 2}]}},
        )
        # Second request may be skipped intentionally depending on which thread runs when:
        # ....assert_all_requests_are_fired = False  # TODO

        set_request_limit(api_client_with_token, 1)
        with pytest.raises(CogniteNotFoundError) as e:
            await api_client_with_token._retrieve_multiple(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                identifiers=IdentifierSequence.of(1, 2),
            )
        assert {"id": 1} in e.value.not_found
        assert {"id": 2} in e.value.not_found + e.value.skipped

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, mock_by_ids: HTTPXMock
    ) -> None:
        res = await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, 2),
        )
        assert async_client is res._cognite_client

    async def test_over_limit_concurrent(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/byids", status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/byids", status_code=200, json={"items": [{"x": 3, "y": 4}]}
        )

        set_request_limit(api_client_with_token, 1)
        await api_client_with_token._retrieve_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            identifiers=IdentifierSequence.of(1, 2),
        )
        unittest.TestCase().assertCountEqual(
            [{"items": [{"id": 1}]}, {"items": [{"id": 2}]}],
            [
                jsgz_load(httpx_mock.get_requests()[0].content),
                jsgz_load(httpx_mock.get_requests()[1].content),
            ],
        )


class TestStandardList:
    async def test_standard_list_ok(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="GET",
            url=BASE_URL + URL_PATH + "?limit=1000",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        res = await api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        )
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump() == res.dump()

    async def test_standard_list_with_filter_get_ok(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=BASE_URL + URL_PATH + "?filter=bla&limit=1000",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        res = await api_client_with_token._list(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            filter={"filter": "bla"},
        )
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]).dump() == res.dump()

    async def test_standard_list_with_filter_post_ok(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/list",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        res = await api_client_with_token._list(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="POST",
            filter={"filter": "bla"},
        )
        assert SomeResourceList([SomeResource(1, 2), SomeResource(1)]) == res
        assert {"filter": {"filter": "bla"}, "limit": 1000} == jsgz_load(httpx_mock.get_requests()[0].content)

    async def test_standard_list_fail(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="GET",
            url=BASE_URL + URL_PATH + "?limit=1000",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            await api_client_with_token._list(
                list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
            )
        assert 400 == e.value.code
        assert "Client Error" == e.value.message

    NUMBER_OF_ITEMS_FOR_AUTOPAGING = 11500
    ITEMS_TO_GET_WHILE_AUTOPAGING: ClassVar = [{"x": 1, "y": 1} for _ in range(NUMBER_OF_ITEMS_FOR_AUTOPAGING)]

    async def test_list_partitions(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        for _ in range(3):
            httpx_mock.add_response(
                method="POST",
                url=BASE_URL + URL_PATH + "/list",
                status_code=200,
                json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
            )
        res = await api_client_with_token._list(
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
        assert 3 == len(httpx_mock.get_requests())
        assert {"1/3", "2/3", "3/3"} == {jsgz_load(c.content)["partition"] for c in httpx_mock.get_requests()}
        for request in httpx_mock.get_requests():
            payload = jsgz_load(request.content)
            assert "x-test" in request.headers
            del payload["partition"]
            assert {"cursor": None, "filter": {}, "limit": 1000} == payload

    async def test_list_partitions_with_failure(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        async def request_callback(request: Any) -> Response:
            payload = jsgz_load(request.content)
            partition = int(payload["partition"].split("/")[0])
            if partition == 3:
                return Response(503, headers={}, json={"message": "Service Unavailable"})
            elif partition < 5:
                # Ensure we don't hit the bad luck race condition when 503 above executes last:
                await asyncio.sleep(0.05)
            return Response(200, headers={}, json={"items": [{"x": 42, "y": 13}]})

        httpx_mock.add_callback(
            request_callback,
            method="POST",
            url=BASE_URL + URL_PATH + "/list",
            match_headers={"content-type": "application/json"},
            is_reusable=True,
        )
        with pytest.raises(CogniteAPIError) as exc:
            await api_client_with_token._list(
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                method="POST",
                partitions=15,
                limit=None,
            )
        assert 503 == exc.value.code
        assert [task["partition"] for task in exc.value.unknown] == ["3/15"]
        assert exc.value.skipped
        assert exc.value.successful
        assert 14 == len(exc.value.successful) + len(exc.value.skipped)
        assert 1 < len(httpx_mock.get_requests())

    @pytest.fixture
    def mock_get_for_autopaging(self, httpx_mock: HTTPXMock) -> None:
        def callback(request: Request) -> Response:
            params = {
                elem.split("=")[0]: elem.split("=")[1] for elem in request.url.query.decode().split("?")[-1].split("&")
            }
            limit = int(params["limit"])
            cursor = int(params.get("cursor") or 0)
            items = self.ITEMS_TO_GET_WHILE_AUTOPAGING[cursor : cursor + limit]
            if cursor + limit >= self.NUMBER_OF_ITEMS_FOR_AUTOPAGING:
                next_cursor = None
            else:
                next_cursor = cursor + limit
            response = json.dumps({"nextCursor": next_cursor, "items": items})
            return Response(200, headers={}, content=response)

        httpx_mock.add_callback(
            callback,
            method="GET",
            url=re.compile(re.escape(BASE_URL + URL_PATH) + r"\?limit=\d+(?:$|&cursor=\d+)"),
            is_reusable=True,
        )

    @pytest.fixture
    def mock_get_for_autopaging_2589(self, httpx_mock: HTTPXMock) -> None:
        NUM_ITEMS = 2589
        ITEMS_EDGECASE = [{"x": 1, "y": 1} for _ in range(NUM_ITEMS)]

        def callback(request: Any) -> Response:
            params = {
                elem.split("=")[0]: elem.split("=")[1] for elem in request.url.query.decode().split("?")[-1].split("&")
            }
            limit = int(params["limit"])
            cursor = int(params.get("cursor") or 0)
            items = ITEMS_EDGECASE[cursor : cursor + limit]
            if cursor + limit >= NUM_ITEMS:
                next_cursor = None
            else:
                next_cursor = cursor + limit
            response = json.dumps({"nextCursor": next_cursor, "items": items})
            return Response(200, headers={}, content=response)

        httpx_mock.add_callback(
            callback,
            method="GET",
            url=re.compile(re.escape(BASE_URL + URL_PATH) + r"\?limit=\d+(?:$|&cursor=\d+)"),
            is_reusable=True,
        )

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    async def test_standard_list_generator(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        ):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 11500 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    async def test_standard_list_generator_with_limit(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", limit=10000
        ):
            assert isinstance(resource, SomeResource)
            total_resources += 1
        assert 10000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    async def test_standard_list_generator_with_chunk_size(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
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
    async def test_standard_list_generator_with_chunk_size_chunk_edge_case(
        self, api_client_with_token: APIClient
    ) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", chunk_size=2500
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            total_resources += len(resource_chunk)
            assert len(resource_chunk) in [89, 2500]
        assert 2589 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging_2589")
    async def test_standard_list_generator_with_chunk_size_chunk_limit(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
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
    async def test_standard_list_generator_with_chunk_size_with_limit(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
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
    async def test_standard_list_generator_with_chunk_size_below_default_limit_and_global_limit(
        self, api_client_with_token: APIClient
    ) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
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
    async def test_standard_list_generator__chunk_size_exceeds_max(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
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
    async def test_standard_list_generator_vs_partitions(self, api_client_with_token: APIClient) -> None:
        total_resources = 0
        async for resource_chunk in api_client_with_token._list_generator(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            method="GET",
            limit=2000,
            chunk_size=1001,
        ):
            assert isinstance(resource_chunk, SomeResourceList)
            total_resources += len(resource_chunk)

        assert 2000 == total_resources

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    async def test_standard_list_autopaging(self, api_client_with_token: APIClient) -> None:
        res = await api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        )
        assert self.NUMBER_OF_ITEMS_FOR_AUTOPAGING == len(res)

    @pytest.mark.usefixtures("mock_get_for_autopaging")
    async def test_standard_list_autopaging_with_limit(self, api_client_with_token: APIClient) -> None:
        res = await api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET", limit=5333
        )
        assert 5333 == len(res)

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/list",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        httpx_mock.add_response(
            method="GET",
            url=BASE_URL + URL_PATH + "?limit=1000",
            status_code=200,
            json={"items": [{"x": 1, "y": 2}, {"x": 1}]},
        )
        res = await api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="POST"
        )
        assert async_client is res._cognite_client

        res = await api_client_with_token._list(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, method="GET"
        )
        assert async_client is res._cognite_client


class TestStandardAggregate:
    async def test_standard_aggregate_OK(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/aggregate", status_code=200, json={"items": [{"count": 1}]}
        )
        assert 1 == await api_client_with_token._aggregate_count(resource_path=URL_PATH, filter={"x": 1})

    async def test_standard_aggregate_fail(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/aggregate",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )
        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            await api_client_with_token._aggregate_count(resource_path=URL_PATH, filter={"x": 1})
        assert "Client Error" == e.value.message
        assert 400 == e.value.code


class TestStandardCreate:
    async def test_standard_create_ok(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]}
        )
        res = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 1), SomeResource(1)],
        )
        assert {"items": [{"x": 1, "y": 1}, {"x": 1}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert SomeResource(1, 2) == res[0]
        assert SomeResource(1) == res[1]

    async def test_standard_create_extra_body_fields(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 1, "y": 2}, {"x": 1}]}
        )
        await api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 1), SomeResource(1)],
            extra_body_fields={"foo": "bar"},
        )
        assert {"items": [{"x": 1, "y": 1}, {"x": 1}], "foo": "bar"} == jsgz_load(httpx_mock.get_requests()[0].content)

    async def test_standard_create_single_item_ok(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        res = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=SomeResource(1, 2)
        )
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert SomeResource(1, 2) == res

    async def test_standard_create_single_item_in_list_ok(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        res = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=[SomeResource(1, 2)]
        )
        assert {"items": [{"x": 1, "y": 2}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert SomeResourceList([SomeResource(1, 2)]) == res

    async def test_standard_create_fail(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        def callback(request: Any) -> Response:
            item = jsgz_load(request.content)["items"][0]
            return Response(status_code=int(item["externalId"]), headers={}, json={})

        httpx_mock.add_callback(
            callback,
            method="POST",
            url=BASE_URL + URL_PATH,
            match_headers={"content-type": "application/json"},
            is_reusable=True,
        )
        set_request_limit(api_client_with_token, 1)
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._create_multiple(
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

    async def test_standard_create_concurrent(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH, status_code=200, json={"items": [{"x": 3, "y": 4}]}
        )

        res = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 2), SomeResource(3, 4)],
            limit=1,
        )
        expected_res_lst = SomeResourceList([SomeResource(1, 2), SomeResource(3, 4)])
        unittest.TestCase().assertCountEqual(expected_res_lst, res)

        expected_item_bodies = [{"items": [{"x": 1, "y": 2}]}, {"items": [{"x": 3, "y": 4}]}]
        gotten_item_bodies = [
            jsgz_load(httpx_mock.get_requests()[0].content),
            jsgz_load(httpx_mock.get_requests()[1].content),
        ]
        unittest.TestCase().assertCountEqual(expected_item_bodies, gotten_item_bodies)

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH,
            status_code=200,
            json={"items": [{"x": 1, "y": 2}]},
            is_reusable=True,
        )
        res1 = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=SomeResource()
        )
        assert async_client is res1._cognite_client

        res2 = await api_client_with_token._create_multiple(
            list_cls=SomeResourceList, resource_cls=SomeResource, resource_path=URL_PATH, items=[SomeResource()]
        )
        assert async_client is res2._cognite_client


class TestStandardDelete:
    async def test_standard_delete_multiple_ok(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=BASE_URL + URL_PATH + "/delete", status_code=200, json={})
        await api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
        )
        assert {"items": [1, 2]} == jsgz_load(httpx_mock.get_requests()[0].content)

    async def test_standard_delete_multiple_ok__single_id(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="POST", url=BASE_URL + URL_PATH + "/delete", status_code=200, json={})
        await api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of(1)
        )
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)

    async def test_standard_delete_multiple_ok__single_id_in_list(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="POST", url=BASE_URL + URL_PATH + "/delete", status_code=200, json={})
        await api_client_with_token._delete_multiple(
            resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1])
        )
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)

    async def test_standard_delete_multiple_fail_4xx(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/delete",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._delete_multiple(
                resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
            )
        assert 400 == e.value.code
        assert "Client Error" == e.value.message
        assert e.value.failed == [1, 2]

    async def test_standard_delete_multiple_fail_5xx(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/delete",
            status_code=500,
            json={"error": {"message": "Server Error"}},
        )
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._delete_multiple(
                resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2])
            )
        assert 500 == e.value.code
        assert "Server Error" == e.value.message
        assert e.value.unknown == [1, 2]
        assert e.value.failed == []

    async def test_standard_delete_multiple_fail_missing_ids(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/delete",
            status_code=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 1}]}},
        )
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/delete",
            status_code=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 3}]}},
        )
        set_request_limit(api_client_with_token, 2)
        with pytest.raises(CogniteNotFoundError) as e:
            await api_client_with_token._delete_multiple(
                resource_path=URL_PATH, wrap_ids=False, identifiers=IdentifierSequence.of([1, 2, 3])
            )

        unittest.TestCase().assertCountEqual([{"id": 1}, {"id": 3}], e.value.not_found)
        assert [1, 2, 3] == sorted(e.value.failed)

    async def test_over_limit_concurrent(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        httpx_mock.add_response(method="POST", url=BASE_URL + URL_PATH + "/delete", status_code=200, json={})
        httpx_mock.add_response(method="POST", url=BASE_URL + URL_PATH + "/delete", status_code=200, json={})

        set_request_limit(api_client_with_token, 2)
        await api_client_with_token._delete_multiple(
            resource_path=URL_PATH, identifiers=IdentifierSequence.of([1, 2, 3, 4]), wrap_ids=False
        )
        unittest.TestCase().assertCountEqual(
            [{"items": [1, 2]}, {"items": [3, 4]}],
            [
                jsgz_load(httpx_mock.get_requests()[0].content),
                jsgz_load(httpx_mock.get_requests()[1].content),
            ],
        )


class TestStandardUpdate:
    @pytest.fixture
    def mock_update(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> HTTPXMock:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/update",
            status_code=200,
            json={"items": [{"id": 1, "x": 1, "y": 100}]},
            is_reusable=True,
        )
        return httpx_mock

    async def test_standard_update_with_cognite_resource_OK(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, y=100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.get_requests()[0].content)

    async def test_standard_update_with_cognite_resource__subject_to_camel_case_issue(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, external_id="abc", y=100)],
        )
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}, "externalId": {"set": "abc"}}}]} == jsgz_load(
            mock_update.get_requests()[0].content
        )

    async def test_standard_update_with_cognite_resource__non_update_attributes(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, y=100, x=1)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.get_requests()[0].content)

    async def test_standard_update_with_cognite_resource__id_and_external_id_set(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=1, external_id="1", y=100, x=1)],
        )
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}, "externalId": {"set": "1"}}}]} == jsgz_load(
            mock_update.get_requests()[0].content
        )

    async def test_standard_update_with_cognite_resource_and_external_id_OK(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(external_id="1", y=100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.get_requests()[0].content
        )

    async def test_standard_update_with_cognite_update_object_OK(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeUpdate(id=1).y.set(100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.get_requests()[0].content)

    async def test_standard_update_single_object(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=SomeUpdate(id=1).y.set(100),
        )
        assert SomeResource(id=1, x=1, y=100) == res
        assert {"items": [{"id": 1, "update": {"y": {"set": 100}}}]} == jsgz_load(mock_update.get_requests()[0].content)

    async def test_standard_update_with_cognite_update_object_and_external_id_OK(
        self, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeUpdate(external_id="1").y.set(100)],
        )
        assert SomeResourceList([SomeResource(id=1, x=1, y=100)]) == res
        assert {"items": [{"externalId": "1", "update": {"y": {"set": 100}}}]} == jsgz_load(
            mock_update.get_requests()[0].content
        )

    async def test_standard_update_fail_4xx(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/update",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=[SomeResource(id=0), SomeResource(external_id="abc")],
            )
        assert e.value.message == "Client Error"
        assert e.value.code == 400
        assert e.value.failed == [0, "abc"]

    async def test_standard_update_fail_5xx(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/update",
            status_code=500,
            json={"error": {"message": "Server Error"}},
        )
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._update_multiple(
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

    @pytest.mark.usefixtures("disable_gzip")  # -> because match_json doesn't work with gzip
    async def test_standard_update_fail_missing_and_5xx(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        # Note 1: We have two tasks being added to an executor, but that doesn't mean we know the
        # execution order. Depending on whether the 400 or 500 hits the first or second task,
        # the following asserts fail (ordering issue). Thus, we use 'matchers.json_params_matcher'
        # to make sure the responses match the two tasks.
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/update",
            status_code=400,
            json={"error": {"message": "Missing ids", "missing": [{"id": 0}]}},
            match_json={"items": [{"update": {}, "id": 0}]},
        )
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/update",
            status_code=500,
            json={"error": {"message": "Server Error"}},
            match_json={"items": [{"update": {}, "externalId": "abc"}]},
        )
        items = [SomeResource(external_id="abc"), SomeResource(id=0)]
        random.shuffle(items)

        set_request_limit(api_client_with_token, 1)
        with pytest.raises(CogniteAPIError) as e:
            await api_client_with_token._update_multiple(
                update_cls=SomeUpdate,
                list_cls=SomeResourceList,
                resource_cls=SomeResource,
                resource_path=URL_PATH,
                items=items,
            )
        assert ["abc"] == e.value.unknown
        assert [0] == e.value.failed
        assert [{"id": 0}] == e.value.missing

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, mock_update: HTTPXMock
    ) -> None:
        res1 = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=SomeResource(id=0),
        )
        assert async_client is res1._cognite_client

        res2 = await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(id=0)],
        )
        assert async_client is res2._cognite_client

    async def test_over_limit_concurrent(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock, set_request_limit: Callable
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/update", status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/update", status_code=200, json={"items": [{"x": 3, "y": 4}]}
        )

        set_request_limit(api_client_with_token, 1)
        await api_client_with_token._update_multiple(
            update_cls=SomeUpdate,
            list_cls=SomeResourceList,
            resource_cls=SomeResource,
            resource_path=URL_PATH,
            items=[SomeResource(1, 2, id=1), SomeResource(3, 4, id=2)],
        )
        unittest.TestCase().assertCountEqual(
            [{"items": [{"id": 1, "update": {"y": {"set": 2}}}]}, {"items": [{"id": 2, "update": {"y": {"set": 4}}}]}],
            [
                jsgz_load(httpx_mock.get_requests()[0].content),
                jsgz_load(httpx_mock.get_requests()[1].content),
            ],
        )


class TestStandardSearch:
    async def test_standard_search_ok(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/search", status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )

        res = await api_client_with_token._search(
            list_cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter=SomeFilter(var_x=1, var_y=1),
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    async def test_standard_search_dict_filter_ok(
        self, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/search", status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )

        res = await api_client_with_token._search(
            list_cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter={"var_x": 1, "varY": 1},
            limit=1000,
        )
        assert SomeResourceList([SomeResource(1, 2)]) == res
        assert {"search": {"name": "bla"}, "limit": 1000, "filter": {"varX": 1, "varY": 1}} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    async def test_standard_search_fail(self, api_client_with_token: APIClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=BASE_URL + URL_PATH + "/search",
            status_code=400,
            json={"error": {"message": "Client Error"}},
        )

        with pytest.raises(CogniteAPIError, match="Client Error") as e:
            await api_client_with_token._search(
                list_cls=SomeResourceList, resource_path=URL_PATH, search={}, filter={}, limit=1
            )
        assert "Client Error" == e.value.message
        assert 400 == e.value.code

    async def test_cognite_client_is_set(
        self, async_client: AsyncCogniteClient, api_client_with_token: APIClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=BASE_URL + URL_PATH + "/search", status_code=200, json={"items": [{"x": 1, "y": 2}]}
        )
        res = await api_client_with_token._search(
            list_cls=SomeResourceList,
            resource_path=URL_PATH,
            search={"name": "bla"},
            filter={"name": "bla"},
            limit=1000,
        )
        assert async_client == res._cognite_client


class TestRetryableEndpoints:
    @pytest.mark.parametrize(
        "method, path, expected",
        [
            test_case
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
            for test_case in [
                # Should retry POST on all _read_ endpoints
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/list", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/byids", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/search", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/list", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/byids", True),
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}/aggregate", True),
                # Should not retry CREATE endpoints as they are not idempotent
                ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/{resource}", False),
            ]
        ],
    )
    async def test_is_retryable_resource_api_endpoints(self, method: str, path: str, expected: bool) -> None:
        assert expected is validate_url_and_return_retryability(method, path)

    @pytest.mark.parametrize(
        "method, path, expected",
        sorted(
            [
                # Versions, only v1 is currently recognized:
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/assets/list", True),
                # 'playground' is no longer a valid version, but since we use a deny-list, we don't match,
                # and thus say it is retryable:
                ("POST", "https://api.cognitedata.com/api/playground/projects/bla/assets/list", True),
                # Hosts
                *(
                    # Should work on all hosts
                    ("POST", f"https://{host}/api/v1/projects/bla/assets/list", True)
                    for host in ["api.cognitedata.com", "greenfield.cognitedata.com", "localhost:8000"]
                ),
                # Methods
                *(
                    # Should by default retry GET, PUT, and PATCH
                    (method, "https://api.cognitedata.com/api/v1/projects/bla", True)
                    for method in {"GET", "PUT", "PATCH"}
                ),
                # Annotations
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/suggest", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/annotations/reverselookup", True),
                # Functions
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/status", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/delete", False),
                # Function calls
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/123/call", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/123/calls/byids", True),
                # Function schedules
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/functions/schedules/delete", False),
                # User Profiles
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/profiles/search", True),
                # Documents
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/aggregate", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/documents/search", True),
                # Extraction Pipelines
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/delete", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/runs", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/runs/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/config", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/extpipes/config/revert", False),
                # Transformations
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/filter", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/run", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/cancel", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/notifications", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/schedules", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/schedules/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/schedules/delete", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/jobs/byids", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/transformations/query/run", True),
                # 3D models
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/delete", False),
                # 3D model revisions
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions", False),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/12/revisions/34/nodes/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions/56/nodes/byids", True),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/bla/3d/models/34/revisions/cd/nodes/byids",
                    True,
                ),
                # 3D asset mappings
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/3d/models/56/revisions/78/mappings", False),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/bla/3d/models/56/revisions/78/mappings/list",
                    True,
                ),
                # Geospatial
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial", False),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/compute", True),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/crs", False),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/crs/byids", True),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/featuretypes", False),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/featuretypes/list", True),
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/featuretypes/delete", False),
                *[
                    (
                        "POST",
                        f"https://api.c.com/api/v1/projects/bla/geospatial/featuretypes/abc_123/features/{endpoint}",
                        True,
                    )
                    for endpoint in ("aggregate", "list", "byids", "search-streaming", "search")
                ],
                ("POST", "https://api.c.com/api/v1/projects/bla/geospatial/featuretypes/a_1/features/delete", False),
                (
                    "POST",
                    "https://api.c.com/api/v1/projects/bla/geospatial/featuretypes/a_1/features/b_2/rasters/c_3",
                    True,
                ),
                # Files
                ("POST", "https://api.c.com/api/v1/projects/bla/files/downloadlink?extendedExpiration=true", True),
                # Timeseries
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data/delete", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/data/latest", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/timeseries/synthetic/query", True),
                # Sequences
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/sequences/data", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/sequences/data/delete", True),
                # Data modeling
                *[
                    # should retry _all_ data modeling schema endpoints as they are idempotent.
                    test_case
                    for resource in ("spaces", "containers", "views", "datamodels")
                    for test_case in [
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/list", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/byids", True),
                        ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/{resource}/delete", True),
                    ]
                ],
                # Retry all data modeling instances endpoints as they are idempotent
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/models/instances", True),
                *(
                    ("POST", f"https://api.cognitedata.com/api/v1/projects/bla/models/instances/{endpoint}", True)
                    for endpoint in ("list", "byids", "delete", "aggregate", "search")
                ),
                # Retry all data modeling graphql endpoints
                ("POST", "https://api.cognitedata.com/api/v1/projects/any/dml/graphql", True),
                (
                    "POST",
                    "https://api.cognitedata.com/api/v1/projects/any/userapis/spaces/bla/datamodels/bla/versions/v1/graphql",
                    True,
                ),
                # Retry for RAW on rows but not on dbs or tables as only the rows endpoints are idempotent
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db/tables/t/rows", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/raw/dbs/db/tables/t/rows/delete", True),
                # Engineering diagrams
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/context/diagram/convert", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/context/diagram/detect", True),
                ("GET", "https://api.cognitedata.com/api/v1/projects/bla/context/diagram/convert/123", True),
                ("GET", "https://api.cognitedata.com/api/v1/projects/bla/context/diagram/detect/456", True),
                # Simulators
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/simulators/list", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/simulators/delete", False),
                # AI API
                # "ai/tools/documents/(summarize|ask)",
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/ai/tools/documents/summarize", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/ai/tools/documents/ask", True),
                ("POST", "https://api.cognitedata.com/api/v1/projects/bla/ai/tools/documents/task", False),
            ]
        ),
    )
    async def test_is_retryable(self, method: str, path: str, expected: bool) -> None:
        assert expected is validate_url_and_return_retryability(method, path)

    @pytest.mark.parametrize(
        "method, path, expected_error",
        [
            ("POST", "htt://bla/bla", r"is not valid\. Cannot resolve whether or not it is retryable"),
            ("BLOP", "http://localhost:8000/token/inspect", r"^Method BLOP is not valid\. Must be one of"),
        ],
    )
    async def test_is_retryable_should_fail(self, method: str, path: str, expected_error: str) -> None:
        with pytest.raises(ValueError, match=expected_error):
            validate_url_and_return_retryability(method, path)


class TestHelpers:
    @pytest.mark.parametrize("header_type", [Headers, dict])
    async def test_sanitize_headers(self, header_type: Any) -> None:
        before = header_type({"Authorization": "bla", "key": "bla"})
        after = header_type({"Authorization": "***", "key": "bla"})

        assert before != after
        assert after == APIClient._sanitize_headers(before)

    @pytest.mark.parametrize(
        "resource, update_obj, mode, expected_update_object",
        [
            pytest.param(
                DefaultResourceGenerator.time_series(id=42, name="bla", metadata={"myNew": "metadataValue"}),
                TimeSeriesUpdate,
                "replace_ignore_null",
                {
                    "isStep": {"set": False},
                    "name": {"set": "bla"},
                    "metadata": {"set": {"myNew": "metadataValue"}},
                },
                id="replace_ignore_null",
            ),
            pytest.param(
                # is_string is ignored as it cannot be updated.
                DefaultResourceGenerator.time_series(id=42, name="bla", metadata={"myNew": "metadataValue"}),
                TimeSeriesUpdate,
                "patch",
                {
                    "isStep": {"set": False},
                    "name": {"set": "bla"},
                    "metadata": {"add": {"myNew": "metadataValue"}},
                },
                id="patch",
            ),
            pytest.param(
                DefaultResourceGenerator.time_series(id=42, name="bla"),
                TimeSeriesUpdate,
                "replace",
                {
                    "isStep": {"set": False},
                    "assetId": {"setNull": True},
                    "dataSetId": {"setNull": True},
                    "description": {"setNull": True},
                    "name": {"set": "bla"},
                    "metadata": {"set": {}},
                    "securityCategories": {"set": []},
                    "unit": {"setNull": True},
                    "unitExternalId": {"setNull": True},
                },
                id="replace",
            ),
            pytest.param(
                MQTT5SourceWrite(external_id="my-source-mqtt", host="mqtt.hsl.fi", port=1883),
                MQTT5SourceUpdate,
                "replace",
                {
                    "host": {"set": "mqtt.hsl.fi"},
                    "port": {"set": 1883},
                    "useTls": {"set": False},
                    "authentication": {"setNull": True},
                    "caCertificate": {"setNull": True},
                    "authCertificate": {"setNull": True},
                },
                id="replace with setNull key",
            ),
        ],
    )
    async def test_convert_resource_to_patch_object(
        self,
        resource: CogniteResource,
        update_obj: type[CogniteUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"],
        expected_update_object: dict[str, dict[str, dict]],
    ) -> None:
        update_attributes = update_obj._get_update_properties()
        actual = APIClient._convert_resource_to_patch_object(resource, update_attributes, mode)
        assert actual["update"] == expected_update_object


class TestConnectionPooling:
    async def test_connection_pool_is_shared_between_clients(self) -> None:
        cnf = ClientConfig(client_name="bla", credentials=Token("bla"), project="bla")
        c1 = get_wrapped_async_client(CogniteClient(cnf))
        c2 = AsyncCogniteClient(cnf)
        assert c1 is not c2
        assert (
            c1._api_client._http_client.httpx_async_client
            is c2._api_client._http_client.httpx_async_client
            is c1._api_client._http_client_with_retry.httpx_async_client
            is c2._api_client._http_client_with_retry.httpx_async_client
        )


async def test_worker_in_backoff_loop_gets_new_token(httpx_mock: HTTPXMock) -> None:
    url = "https://api.cognitedata.com/api/v1/projects/c/assets/byids"
    httpx_mock.add_response(method="POST", url=url, status_code=429, json={"error": "Backoff plz"})
    httpx_mock.add_response(
        method="POST",
        url=url,
        status_code=200,
        json={"items": [{"id": 123, "createdTime": 123, "lastUpdatedTime": 123}]},
    )

    call_count = 0

    def token_callable() -> str:
        nonlocal call_count
        if call_count < 1:
            call_count += 1
            return "outdated-token"
        return "valid-token"

    client = CogniteClient(ClientConfig(client_name="a", credentials=Token(token_callable), project="c"))
    assert get_or_raise(client.assets.retrieve(id=1)).id == 123
    assert call_count > 0
    assert httpx_mock.get_requests()[0].headers["Authorization"] == "Bearer outdated-token"
    assert httpx_mock.get_requests()[1].headers["Authorization"] == "Bearer valid-token"


@pytest.mark.parametrize("limit, expected_error", ((-2, ValueError), (0, ValueError), ("10", TypeError)))
async def test_list_and_search__bad_limit_value_raises(
    limit: Any, expected_error: Any, cognite_client: CogniteClient
) -> None:
    with pytest.raises(expected_error):
        cognite_client.assets.list(limit=limit)
    with pytest.raises(expected_error):
        cognite_client.assets.search(limit=limit)
