"""Verifies that each top-level concurrency category routes to the correct semaphore.

The strict-semaphore check in tests/conftest.py provides suite-wide coverage of "every
internal API call passes a semaphore". This file adds the narrower contract: that each
sub-config (general/raw/datapoints/data_modeling) and operation type (read/write/delete
/search/read_schema/write_schema) is wired to the API methods that should use it.
One representative test per (sub_config, operation) combination — adding more is redundant.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.data_modeling.records import RecordId, RecordWrite
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._concurrency import HierarchicalBoundedSemaphore
from tests.utils import fresh_concurrency_state

SemCall = tuple[str, str, str]  # (sub_config_name (eg 'general'), operation, project)
ApiCall = Callable[[AsyncCogniteClient], Awaitable[Any]]


@pytest.fixture
def semaphore_spy(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[SemCall]]:
    """Records every _semaphore_factory call across all sub-configs (on a freshly-reset state)."""
    calls: list[SemCall] = []
    with fresh_concurrency_state() as settings:
        for sub_config in settings._all_concurrency_configs:
            original = sub_config._semaphore_factory

            def make_spy(_orig: Any, _name: str) -> Any:
                def spy(operation: str, project: str) -> Any:
                    calls.append((_name, operation, project))
                    return _orig(operation, project)

                return spy

            monkeypatch.setattr(sub_config, "_semaphore_factory", make_spy(original, sub_config.api_name))

        yield calls


@pytest.fixture
def mock_any_request(httpx_mock: HTTPXMock) -> HTTPXMock:
    """Catch-all 200 response for any HTTP method/URL — tests only care about the semaphore."""
    any_url = re.compile(r".*")
    for method in ("GET", "POST", "PUT", "DELETE", "PATCH"):
        httpx_mock.add_response(method=method, url=any_url, status_code=200, json={"items": []}, is_optional=True)
    return httpx_mock


def assert_routed(calls: list[SemCall], sub_config: str, operation: str) -> None:
    matches = [(s, o) for s, o, _ in calls if s == sub_config and o == operation]
    assert matches, f"Expected a ({sub_config!r}, {operation!r}) semaphore call, got: {[(s, o) for s, o, _ in calls]}"


def _raw_write_call(c: AsyncCogniteClient) -> Awaitable[None]:
    import pandas as pd

    df = pd.DataFrame({"col-a": [1, 2]}, index=["r1", "r2"])
    return c.raw.rows.insert_dataframe("db1", "t1", df)


@pytest.mark.usefixtures("mock_any_request")
class TestSemaphoreRouting:
    @pytest.mark.parametrize(
        "api_call, sub_config, operation",
        [
            pytest.param(lambda c: c.assets.retrieve(id=1), "general", "read", id="general_read"),
            pytest.param(lambda c: c.assets.delete(id=1), "general", "delete", id="general_delete"),
            pytest.param(
                _raw_write_call,
                "raw",
                "write",
                id="raw_write",
                marks=pytest.mark.dsl,
            ),
            pytest.param(
                lambda c: c.raw.rows.delete("db1", "t1", key=["k1", "k2"]),
                "raw",
                "delete",
                id="raw_delete",
            ),
            pytest.param(
                lambda c: c.time_series.data.insert(datapoints=[(1000, 1.0)], id=1),
                "datapoints",
                "write",
                id="datapoints_write",
                marks=pytest.mark.allow_no_semaphore(
                    "DatapointsAPI._insert_datapoints holds the semaphore via outer 'async with' "
                    "for memory backpressure, then passes None to _post to avoid double-acquiring."
                ),
            ),
            pytest.param(
                lambda c: c.time_series.data.delete_range(start=0, end=1000, id=1),
                "datapoints",
                "delete",
                id="datapoints_delete",
            ),
            pytest.param(
                lambda c: c.data_modeling.instances.retrieve(nodes=NodeId("space1", "node1")),
                "data_modeling",
                "read",
                id="data_modeling_read",
            ),
            pytest.param(
                lambda c: c.data_modeling.spaces.retrieve(spaces="my-space"),
                "data_modeling",
                "read_schema",
                id="data_modeling_read_schema",
            ),
            pytest.param(
                lambda c: c.data_modeling.spaces.delete(spaces="my-space"),
                "data_modeling",
                "write_schema",
                id="data_modeling_write_schema",
            ),
        ],
    )
    async def test_routing(
        self,
        async_client: AsyncCogniteClient,
        semaphore_spy: list[SemCall],
        api_call: ApiCall,
        sub_config: str,
        operation: str,
    ) -> None:
        await api_call(async_client)
        assert_routed(semaphore_spy, sub_config, operation)
        # Project is part of the (op, project, loop) cache key — verify it's threaded through:
        assert async_client.config.project in {proj for _, _, proj in semaphore_spy}


class TestSemaphoreRoutingSpecialResponses:
    """Tests where the catch-all ``mock_any_request`` shape doesn't fit the response parser."""

    async def test_raw_read(
        self, async_client: AsyncCogniteClient, semaphore_spy: list[SemCall], httpx_mock: HTTPXMock
    ) -> None:
        # raw.rows.retrieve parses a single Row directly (not {items: [...]}); supply a row shape:
        httpx_mock.add_response(
            method="GET",
            url=re.compile(r".*/raw/dbs/.*"),
            status_code=200,
            json={"key": "k1", "columns": {"col1": "v1"}, "lastUpdatedTime": 0},
        )
        await async_client.raw.rows.retrieve("db1", "t1", "k1")
        assert_routed(semaphore_spy, "raw", "read")


class TestRecordsSemaphoreRouting:
    """Records API uses RecordsConcurrencyOperation enums (not plain strings),
    so we spy on the enum values directly."""

    @pytest.fixture
    def records_spy(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[str, str]]]:
        calls: list[tuple[str, str]] = []
        with fresh_concurrency_state() as settings:
            original = settings.records._semaphore_factory

            def spy(operation: Any, project: str) -> Any:
                calls.append((operation.value, project))
                return original(operation, project)

            monkeypatch.setattr(settings.records, "_semaphore_factory", spy)
            yield calls

    @pytest.mark.usefixtures("mock_any_request")
    @pytest.mark.parametrize(
        "api_call, expected_operation",
        [
            pytest.param(
                lambda c: c.data_modeling.records.ingest(
                    RecordWrite(space="sp", external_id="r1", sources=[]),
                    stream_id="s1",
                ),
                "write",
                id="ingest_write",
            ),
            pytest.param(
                lambda c: c.data_modeling.records.upsert(
                    RecordWrite(space="sp", external_id="r1", sources=[]),
                    stream_id="s1",
                ),
                "write",
                id="upsert_write",
            ),
            pytest.param(
                lambda c: c.data_modeling.records.delete(
                    RecordId(space="sp", external_id="r1"),
                    stream_id="s1",
                ),
                "write",
                id="delete_write",
            ),
        ],
    )
    async def test_write_routing(
        self,
        async_client: AsyncCogniteClient,
        records_spy: list[tuple[str, str]],
        api_call: ApiCall,
        expected_operation: str,
    ) -> None:
        await api_call(async_client)
        ops = [op for op, _ in records_spy]
        assert expected_operation in ops, f"Expected {expected_operation!r} in {ops}"
        assert async_client.config.project in {proj for _, proj in records_spy}


class TestHierarchicalSemaphoreThroughHTTPClient:
    """Verify that HierarchicalBoundedSemaphore works through the real SDK HTTP chain.

    The type hints say asyncio.BoundedSemaphore, but at runtime the HTTP client
    just does ``async with semaphore``. These tests confirm that a
    HierarchicalBoundedSemaphore actually acquires and releases both inner
    semaphores when passed through _post.
    """

    async def test_post_with_hierarchical_semaphore_succeeds(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        dedicated = asyncio.BoundedSemaphore(1)
        query = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(dedicated, query)

        await async_client.data_modeling.records._post(
            url_path="/streams/test/records/filter",
            json={"limit": 10},
            semaphore=h,
        )
        assert dedicated._value == 1, "dedicated semaphore should be released after request"
        assert query._value == 1, "query semaphore should be released after request"

    async def test_post_with_hierarchical_semaphore_releases_on_http_error(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=500, json={"error": {"message": "fail", "code": 500}})
        dedicated = asyncio.BoundedSemaphore(1)
        query = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(dedicated, query)

        with pytest.raises(CogniteAPIError):
            await async_client.data_modeling.records._post(
                url_path="/streams/test/records/filter",
                json={"limit": 10},
                semaphore=h,
            )
        assert dedicated._value == 1, "dedicated semaphore should be released after error"
        assert query._value == 1, "query semaphore should be released after error"

    async def test_hierarchical_semaphore_limits_concurrent_requests(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        hold = asyncio.Event()

        async def slow_response(request: Any) -> Any:
            await hold.wait()
            return httpx.Response(200, json={"items": []})

        httpx_mock.add_callback(slow_response, method="POST", url=re.compile(r".*"), is_optional=True)
        httpx_mock.add_callback(slow_response, method="POST", url=re.compile(r".*"), is_optional=True)
        httpx_mock.add_callback(slow_response, method="POST", url=re.compile(r".*"), is_optional=True)

        dedicated = asyncio.BoundedSemaphore(1)
        query = asyncio.BoundedSemaphore(2)

        async def make_request() -> None:
            h = HierarchicalBoundedSemaphore(dedicated, query)
            await async_client.data_modeling.records._post(
                url_path="/streams/test/records/filter",
                json={"limit": 10},
                semaphore=h,
            )

        t1 = asyncio.create_task(make_request())
        t2 = asyncio.create_task(make_request())
        await asyncio.sleep(0.05)

        assert dedicated._value == 0, "dedicated(1) should be fully consumed by first request"
        assert query._value == 1, "query(2) should have 1 slot consumed"

        hold.set()
        await asyncio.gather(t1, t2)
        assert dedicated._value == 1
        assert query._value == 2


class TestRecordsSemaphoreEndpointPatterns:
    """Simulate the exact patterns that records endpoints will use:
    - list/sync: _get_semaphore("query", stream_type) → override_semaphore in _list
    - retrieve: _get_semaphore("retrieve", stream_type) → override_semaphore in _post (hierarchical)
    - aggregate: _get_semaphore("aggregate", stream_type) → override_semaphore in _post (hierarchical)
    """

    @pytest.fixture(autouse=True)
    def _fresh_state(self) -> Iterator[None]:
        with fresh_concurrency_state():
            yield

    async def test_list_pattern_mutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("query", "mutable")
        assert isinstance(sem, asyncio.BoundedSemaphore)
        assert sem._value == 30

        await records_api._post(url_path="/streams/s1/records/filter", json={"limit": 10}, semaphore=sem)
        assert sem._value == 30

    async def test_list_pattern_immutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("query", "immutable")
        assert isinstance(sem, asyncio.BoundedSemaphore)
        assert sem._value == 10

        await records_api._post(url_path="/streams/s1/records/filter", json={"limit": 10}, semaphore=sem)
        assert sem._value == 10

    async def test_retrieve_pattern_mutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("retrieve", "mutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

        await records_api._post(url_path="/streams/s1/records/retrieve", json={"items": []}, semaphore=sem)
        dedicated, query = sem._semaphores
        assert dedicated._value == 20
        assert query._value == 30

    async def test_retrieve_pattern_immutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("retrieve", "immutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

        await records_api._post(url_path="/streams/s1/records/retrieve", json={"items": []}, semaphore=sem)
        dedicated, query = sem._semaphores
        assert dedicated._value == 10
        assert query._value == 10

    async def test_aggregate_pattern_mutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("aggregate", "mutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

        await records_api._post(url_path="/streams/s1/records/aggregate", json={}, semaphore=sem)
        dedicated, query = sem._semaphores
        assert dedicated._value == 10
        assert query._value == 30

    async def test_aggregate_pattern_immutable(self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []})
        records_api = async_client.data_modeling.records
        sem = records_api._get_semaphore("aggregate", "immutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

        await records_api._post(url_path="/streams/s1/records/aggregate", json={}, semaphore=sem)
        dedicated, query = sem._semaphores
        assert dedicated._value == 5
        assert query._value == 10

    async def test_retrieve_and_list_share_query_semaphore_through_post(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        """A retrieve request and a list request against the same stream type
        must compete for the same query semaphore."""
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []}, is_optional=True)
        httpx_mock.add_response(method="POST", url=re.compile(r".*"), status_code=200, json={"items": []}, is_optional=True)
        records_api = async_client.data_modeling.records

        retrieve_sem = records_api._get_semaphore("retrieve", "mutable")
        list_sem = records_api._get_semaphore("query", "mutable")

        assert isinstance(retrieve_sem, HierarchicalBoundedSemaphore)
        assert retrieve_sem._semaphores[1] is list_sem

        await records_api._post(url_path="/streams/s1/records/retrieve", json={"items": []}, semaphore=retrieve_sem)
        await records_api._post(url_path="/streams/s1/records/filter", json={"limit": 10}, semaphore=list_sem)


class TestStrictFixtureCatchesMissingSemaphore:
    """Sanity check that the suite-wide strict fixture in tests/conftest.py still works.

    Without this, a regression in the strict fixture would silently pass every test.
    """

    async def test_top_level_post_without_semaphore_raises(self, async_client: AsyncCogniteClient) -> None:
        with pytest.raises(pytest.fail.Exception, match="did not pass a semaphore"):
            await async_client.post("/assets/byids", json={"items": [{"id": 1}]})
