from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling.data_types import UnitReference
from cognite.client.data_classes.data_modeling.instances import InstanceSort, TypeInformation
from cognite.client.data_classes.data_modeling.records import (
    Avg,
    Count,
    FilterAggregateResult,
    Filters,
    Max,
    MetricAggregateResult,
    MovingFunction,
    MovingFunctionAggregateResult,
    NumberHistogram,
    NumberHistogramAggregateResult,
    Record,
    RecordContainerId,
    RecordId,
    RecordList,
    RecordsAggregation,
    RecordSource,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordWrite,
    Sum,
    SyncRecord,
    SyncRecordList,
    TimeHistogram,
    TimeHistogramAggregateResult,
    TimeRange,
    UniqueValues,
    UniqueValuesAggregateResult,
)
from tests.utils import jsgz_load


@pytest.fixture
def stream_id() -> str:
    return "my-stream"


@pytest.fixture
def records_base_url(async_client: AsyncCogniteClient, stream_id: str) -> str:
    return async_client.data_modeling.records._base_url_with_base_path + f"/streams/{stream_id}/records"


@pytest.fixture
def delete_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/delete$")


@pytest.fixture
def mock_delete(httpx_mock: HTTPXMock, delete_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)


@pytest.fixture
def ingest_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"$")


@pytest.fixture
def mock_ingest(httpx_mock: HTTPXMock, ingest_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)


@pytest.fixture
def upsert_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/upsert$")


@pytest.fixture
def mock_upsert(httpx_mock: HTTPXMock, upsert_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)


@pytest.fixture
def filter_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/filter$")


@pytest.fixture
def record_response() -> dict:
    return {
        "space": "sp",
        "externalId": "rec-1",
        "createdTime": 100,
        "lastUpdatedTime": 200,
        "properties": {"sp": {"container-x": {"temp": 22.5}}},
    }


@pytest.fixture
def mock_filter(httpx_mock: HTTPXMock, filter_url_pattern: re.Pattern, record_response: dict) -> None:
    httpx_mock.add_response(method="POST", url=filter_url_pattern, status_code=200, json={"items": [record_response]})


@pytest.fixture
def sync_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/sync$")


@pytest.fixture
def write_item() -> RecordWrite:
    return RecordWrite(
        space="sp",
        external_id="rec-1",
        sources=[
            RecordSource(
                source=RecordContainerId(space="sp", external_id="container-x"),
                properties={"temp": 22.5},
            )
        ],
    )


class TestRecordsAPIDelete:
    def test_delete_posts_space_external_id_pairs(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.delete(RecordId(space="sp", external_id="rec-1"), stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
        stream_id: str,
    ) -> None:
        items = [RecordId(space="sp", external_id="rec-1"), RecordId(space="sp", external_id="rec-2")]
        cognite_client.data_modeling.records.delete(items, stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {
            "items": [
                {"space": "sp", "externalId": "rec-1"},
                {"space": "sp", "externalId": "rec-2"},
            ]
        }

    def test_delete_chunks(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        delete_url_pattern: re.Pattern,
        stream_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(async_client.data_modeling.records, "_DELETE_LIMIT", 42)
        httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)
        httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)
        items = [RecordId(space="sp", external_id=f"r-{i}") for i in range(43)]
        cognite_client.data_modeling.records.delete(items, stream_id=stream_id)
        assert len(httpx_mock.get_requests()) == 2


class TestRecordsAPIIngest:
    def test_ingest_single_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_ingest: None,
        stream_id: str,
        write_item: RecordWrite,
    ) -> None:
        cognite_client.data_modeling.records.ingest(write_item, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {
            "items": [
                {
                    "space": "sp",
                    "externalId": "rec-1",
                    "sources": [
                        {
                            "source": {"type": "container", "space": "sp", "externalId": "container-x"},
                            "properties": {"temp": 22.5},
                        }
                    ],
                }
            ]
        }

    def test_ingest_chunks_over_1000(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        ingest_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)
        httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(1001)]
        cognite_client.data_modeling.records.ingest(items, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 1000
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordsAPIUpsert:
    def test_upsert_single_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_upsert: None,
        stream_id: str,
        write_item: RecordWrite,
    ) -> None:
        cognite_client.data_modeling.records.upsert(write_item, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.path.endswith(f"/streams/{stream_id}/records/upsert")
        body = jsgz_load(requests[0].content)
        assert body == {
            "items": [
                {
                    "space": "sp",
                    "externalId": "rec-1",
                    "sources": [
                        {
                            "source": {"type": "container", "space": "sp", "externalId": "container-x"},
                            "properties": {"temp": 22.5},
                        }
                    ],
                }
            ]
        }

    def test_upsert_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_upsert: None,
        stream_id: str,
    ) -> None:
        items = [
            RecordWrite(space="sp", external_id="rec-1", sources=[]),
            RecordWrite(space="sp", external_id="rec-2", sources=[]),
        ]
        cognite_client.data_modeling.records.upsert(items, stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert [item["externalId"] for item in body["items"]] == ["rec-1", "rec-2"]

    def test_upsert_chunks(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        upsert_url_pattern: re.Pattern,
        stream_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(async_client.data_modeling.records, "_CREATE_LIMIT", 10)
        httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)
        httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(11)]
        cognite_client.data_modeling.records.upsert(items, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 10
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordsAPIAggregate:
    def test_aggregate_posts_request_and_returns_wrapper(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(records_base_url) + r"/aggregate$"),
            json={"aggregates": {"avg_temp": {"avg": 22.5}}},
        )
        out = cognite_client.data_modeling.records.aggregate(
            stream_id=stream_id,
            aggregates={"avg_temp": {"avg": {"property": ["sp", "container-x", "temp"]}}},
            last_updated_time=TimeRange(gte=1_000_000),
            filter=filters.Equals(["space"], "sp"),
            target_units=RecordTargetUnits(unit_system_name="SI"),
            include_typing=True,
        )

        assert isinstance(out, RecordsAggregation)
        assert out.aggregates == {"avg_temp": {"avg": 22.5}}
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {
            "aggregates": {"avg_temp": {"avg": {"property": ["sp", "container-x", "temp"]}}},
            "lastUpdatedTime": {"gte": 1_000_000},
            "filter": {"equals": {"property": ["space"], "value": "sp"}},
            "targetUnits": {"unitSystemName": "SI"},
            "includeTyping": True,
        }

    def test_aggregate_accepts_dict_filter(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(records_base_url) + r"/aggregate$"),
            json={"aggregates": {"total": {"count": 7}}},
        )
        cognite_client.data_modeling.records.aggregate(
            stream_id=stream_id,
            aggregates={"total": {"count": {}}},
            filter={"matchAll": {}},
        )

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["filter"] == {"matchAll": {}}

    def test_aggregate_accepts_mixed_typed_and_dict_aggregates(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(records_base_url) + r"/aggregate$"),
            json={"aggregates": {"total": {"count": 7}}},
        )

        cognite_client.data_modeling.records.aggregate(
            stream_id=stream_id,
            aggregates={
                "by_day": TimeHistogram(
                    ["sp", "container-x", "timestamp"],
                    calendar_interval="1d",
                    aggregates={
                        "avg_temp": Avg(["sp", "container-x", "temp"]),
                        "moving_count": MovingFunction(
                            buckets_path="_count",
                            window=3,
                            function="MovingFunctions.unweightedAvg",
                        ),
                        "raw_total": {"count": {}},
                    },
                ),
                "by_region": UniqueValues(
                    ["sp", "container-x", "region"],
                    aggregates={"max_temp": Max(["sp", "container-x", "temp"])},
                    size=5,
                ),
                "salary_histogram": NumberHistogram(
                    ["sp", "container-x", "salary"],
                    interval=1000,
                    aggregates={"sum_salary": Sum(["sp", "container-x", "salary"])},
                    hard_bounds={"min": 0, "max": 10000},
                ),
                "by_filters": Filters(
                    filters=[
                        filters.Range(["createdTime"], gte=1),
                        {"matchAll": {}},
                    ],
                    aggregates={"total": Count()},
                ),
            },
        )

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["aggregates"] == {
            "by_day": {
                "timeHistogram": {
                    "property": ["sp", "container-x", "timestamp"],
                    "calendarInterval": "1d",
                    "aggregates": {
                        "avg_temp": {"avg": {"property": ["sp", "container-x", "temp"]}},
                        "moving_count": {
                            "movingFunction": {
                                "bucketsPath": "_count",
                                "window": 3,
                                "function": "MovingFunctions.unweightedAvg",
                            }
                        },
                        "raw_total": {"count": {}},
                    },
                }
            },
            "by_region": {
                "uniqueValues": {
                    "property": ["sp", "container-x", "region"],
                    "aggregates": {"max_temp": {"max": {"property": ["sp", "container-x", "temp"]}}},
                    "size": 5,
                }
            },
            "salary_histogram": {
                "numberHistogram": {
                    "property": ["sp", "container-x", "salary"],
                    "interval": 1000,
                    "aggregates": {"sum_salary": {"sum": {"property": ["sp", "container-x", "salary"]}}},
                    "hardBounds": {"min": 0, "max": 10000},
                }
            },
            "by_filters": {
                "filters": {
                    "filters": [
                        {"range": {"property": ["createdTime"], "gte": 1}},
                        {"matchAll": {}},
                    ],
                    "aggregates": {"total": {"count": {}}},
                }
            },
        }

    def test_records_aggregation_dump_round_trip(self) -> None:
        raw = {
            "aggregates": {
                "by_space": {
                    "uniqueValueBuckets": [
                        {"value": "sp", "count": 2, "aggregates": {"max_temp": {"max": 30.0}}},
                    ]
                }
            }
        }
        loaded = RecordsAggregation._load(raw)
        assert loaded.dump() == raw

    def test_records_aggregation_loads_typed_results(self) -> None:
        loaded = RecordsAggregation._load(
            {
                "aggregates": {
                    "avg_temp": {"avg": 22.5},
                    "by_region": {
                        "uniqueValueBuckets": [
                            {
                                "value": "north",
                                "count": 2,
                                "aggregates": {"max_temp": {"max": 30.0}},
                            }
                        ]
                    },
                    "by_number": {"numberHistogramBuckets": [{"intervalStart": 0.0, "count": 1}]},
                    "by_time": {
                        "timeHistogramBuckets": [
                            {
                                "intervalStart": "2024-05-16T00:00:00Z",
                                "count": 3,
                                "aggregates": {"moving": {"fnValue": 7.5}},
                            }
                        ]
                    },
                    "by_filter": {"filterBuckets": [{"count": 4}]},
                    "future": {"futureAggregateResult": 1},
                }
            }
        )

        avg_temp = loaded["avg_temp"]
        assert isinstance(avg_temp, MetricAggregateResult)
        assert avg_temp.aggregate == "avg"
        assert avg_temp.value == 22.5

        by_region = loaded["by_region"]
        assert isinstance(by_region, UniqueValuesAggregateResult)
        assert by_region.buckets[0].value == "north"
        max_temp = by_region.buckets[0].results["max_temp"]
        assert isinstance(max_temp, MetricAggregateResult)
        assert max_temp.value == 30.0

        by_number = loaded["by_number"]
        assert isinstance(by_number, NumberHistogramAggregateResult)
        assert by_number.buckets[0].interval_start == 0.0

        by_time = loaded["by_time"]
        assert isinstance(by_time, TimeHistogramAggregateResult)
        moving = by_time.buckets[0].results["moving"]
        assert isinstance(moving, MovingFunctionAggregateResult)
        assert moving.fn_value == 7.5

        by_filter = loaded["by_filter"]
        assert isinstance(by_filter, FilterAggregateResult)
        assert by_filter.buckets[0].count == 4

        assert loaded["future"].dump() == {"futureAggregateResult": 1}


class TestRecordsAPIFilter:
    def test_list_returns_record_list(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        result = cognite_client.data_modeling.records.filter(stream_id=stream_id)
        assert isinstance(result, RecordList)
        assert len(result) == 1
        assert result[0].external_id == "rec-1"
        assert result[0].properties == {"sp": {"container-x": {"temp": 22.5}}}
        request = httpx_mock.get_requests()[0]
        assert request.url.path.endswith(f"/streams/{stream_id}/records/filter")

    def test_list_default_limit_is_10(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.filter(stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"limit": 10}

    def test_list_sends_last_updated_time_and_limit(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.filter(
            stream_id=stream_id, last_updated_time=TimeRange(gte=1_000_000), limit=50
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["lastUpdatedTime"] == {"gte": 1_000_000}
        assert body["limit"] == 50

    def test_list_sources_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.filter(
            stream_id=stream_id,
            sources=[RecordSourceSelector(RecordContainerId(space="sp", external_id="container-x"), ["*"])],
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "container-x"}, "properties": ["*"]}
        ]

    def test_list_sort_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.filter(
            stream_id=stream_id, sort=InstanceSort(property=["sp", "container-x", "temp"], direction="descending")
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["sort"] == [{"property": ["sp", "container-x", "temp"], "direction": "descending"}]

    def test_list_include_typing(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        filter_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        typing = {"sp": {"container-x": {"temp": {"type": {"type": "float64", "list": False}, "nullable": True}}}}
        httpx_mock.add_response(
            method="POST", url=filter_url_pattern, status_code=200, json={"items": [record_response], "typing": typing}
        )
        result = cognite_client.data_modeling.records.filter(stream_id=stream_id, include_typing=True)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["includeTyping"] is True
        assert isinstance(result.typing, TypeInformation)


class TestRecordsAPISync:
    def test_sync_returns_page_with_cursor(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        items = [{**record_response, "externalId": f"rec-{i}", "status": "created"} for i in range(10)]
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": items, "nextCursor": "abc", "hasNext": False},
        )
        page = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago")
        assert isinstance(page, SyncRecordList)
        assert page.cursor == "abc"
        assert page.has_next is False
        assert page[0].status == "created"
        request = httpx_mock.get_requests()[0]
        assert request.url.path.endswith(f"/streams/{stream_id}/records/sync")
        assert jsgz_load(request.content) == {"initializeCursor": "7d-ago", "limit": 10}

    def test_sync_resume_sends_cursor(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={
                "items": [
                    {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "created"}
                ],
                "nextCursor": "p2",
                "hasNext": True,
            },
        )
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={
                "items": [
                    {"space": "sp", "externalId": "rec-2", "createdTime": 3, "lastUpdatedTime": 4, "status": "updated"}
                ],
                "nextCursor": "p3",
                "hasNext": False,
            },
        )
        first = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="2d-ago", limit=1)
        assert first.has_next is True
        assert first.cursor is not None
        second = cognite_client.data_modeling.records.sync_resume(stream_id=stream_id, cursor=first.cursor, limit=1)
        assert second.cursor == "p3"
        body2 = jsgz_load(httpx_mock.get_requests()[1].content)
        assert body2 == {"cursor": "p2", "limit": 1}

    def test_sync_deleted_tombstone_has_no_properties(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        item = {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "deleted"}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False},
        )
        page = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="c", limit=1)
        assert page[0].status == "deleted"
        assert page[0].properties is None

    def test_sync_include_typing(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        item = {**record_response, "status": "updated"}
        typing = {"sp": {"container-x": {"temp": {"type": {"type": "float64", "list": False}, "nullable": True}}}}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False, "typing": typing},
        )
        page = cognite_client.data_modeling.records.sync(
            stream_id=stream_id, initialize_cursor="c", include_typing=True, limit=1
        )
        assert jsgz_load(httpx_mock.get_requests()[0].content)["includeTyping"] is True
        assert isinstance(page.typing, TypeInformation)

    def test_sync_target_units_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        item = {**record_response, "status": "updated"}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False},
        )
        cognite_client.data_modeling.records.sync(
            stream_id=stream_id,
            initialize_cursor="c",
            target_units=RecordTargetUnits(unit_system_name="Imperial"),
            limit=1,
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["targetUnits"] == {"unitSystemName": "Imperial"}


class TestRecordDTOs:
    def test_record_write_as_id(self, write_item: RecordWrite) -> None:
        rid = write_item.as_id()
        assert isinstance(rid, RecordId)
        assert rid.space == "sp"
        assert rid.external_id == "rec-1"

    def test_record_write_round_trip(self, write_item: RecordWrite) -> None:
        dumped = write_item.dump()
        loaded = RecordWrite._load(dumped)
        assert loaded.space == write_item.space
        assert loaded.external_id == write_item.external_id
        assert len(loaded.sources) == 1
        assert loaded.sources[0].source.space == "sp"
        assert loaded.sources[0].source.external_id == "container-x"
        assert loaded.sources[0].properties == {"temp": 22.5}

    def test_record_source_reference_dump(self) -> None:
        ref = RecordContainerId(space="s", external_id="c")
        d = ref.dump()
        assert d == {"type": "container", "space": "s", "externalId": "c"}

    def test_record_source_dump(self) -> None:
        src = RecordSource(
            source=RecordContainerId(space="s", external_id="c"),
            properties={"x": 1},
        )
        d = src.dump()
        assert d["source"]["type"] == "container"
        assert d["properties"] == {"x": 1}

    def test_record_load_dump_round_trip(self) -> None:
        payload = {
            "space": "sp",
            "externalId": "rec-1",
            "createdTime": 100,
            "lastUpdatedTime": 200,
            "properties": {"sp": {"c": {"temp": 22.5}}},
        }
        record = Record._load(payload)
        assert record.created_time == 100
        assert record.last_updated_time == 200
        assert record.dump() == payload

    def test_record_as_id(self) -> None:
        record = Record(space="sp", external_id="rec-1", created_time=1, last_updated_time=2)
        rid = record.as_id()
        assert isinstance(rid, RecordId)
        assert (rid.space, rid.external_id) == ("sp", "rec-1")

    def test_record_as_write_reconstructs_sources(self) -> None:
        record = Record(
            space="sp",
            external_id="rec-1",
            created_time=1,
            last_updated_time=2,
            properties={"sp": {"c": {"temp": 22.5}}},
        )
        write = record.as_write()
        assert isinstance(write, RecordWrite)
        assert write.dump()["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "c"}, "properties": {"temp": 22.5}}
        ]

    def test_record_list_as_ids_and_as_write(self) -> None:
        records = RecordList(
            [
                Record(space="sp", external_id="rec-1", created_time=1, last_updated_time=2),
                Record(space="sp", external_id="rec-2", created_time=1, last_updated_time=2),
            ]
        )
        assert records.as_ids() == [RecordId("sp", "rec-1"), RecordId("sp", "rec-2")]
        assert [w.external_id for w in records.as_write()] == ["rec-1", "rec-2"]

    def test_time_range_dump_omits_none(self) -> None:
        assert TimeRange(gte=1, lt=5).dump() == {"gte": 1, "lt": 5}
        assert TimeRange().dump() == {}

    def test_record_source_selector_dump(self) -> None:
        selector = RecordSourceSelector(RecordContainerId(space="sp", external_id="c"), ["temp", "pressure"])
        assert selector.dump() == {
            "source": {"type": "container", "space": "sp", "externalId": "c"},
            "properties": ["temp", "pressure"],
        }

    def test_sync_record_as_write_reconstructs_sources(self) -> None:
        record = SyncRecord(
            space="sp",
            external_id="rec-1",
            created_time=1,
            last_updated_time=2,
            status="updated",
            properties={"sp": {"c": {"temp": 22.5}}},
        )
        write = record.as_write()
        assert isinstance(write, RecordWrite)
        assert write.dump()["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "c"}, "properties": {"temp": 22.5}}
        ]

    def test_record_target_units_dump(self) -> None:
        target_units = RecordTargetUnits(
            properties=[RecordTargetUnit(["sp", "c", "pressure"], UnitReference("pressure:pa"))]
        )
        assert target_units.dump() == {
            "properties": [{"property": ["sp", "c", "pressure"], "unit": {"externalId": "pressure:pa"}}]
        }

    def test_record_target_units_rejects_empty_request_mode(
        self, cognite_client: CogniteClient, stream_id: str
    ) -> None:
        expected_err = "Provide exactly one of 'properties' or 'unit_system_name'."
        with pytest.raises(ValueError, match=expected_err):
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="c",
                target_units=RecordTargetUnits(),
                limit=1,
            )

    def test_record_target_units_rejects_multiple_request_modes(
        self, cognite_client: CogniteClient, stream_id: str
    ) -> None:
        expected_err = "Provide exactly one of 'properties' or 'unit_system_name'."
        with pytest.raises(ValueError, match=expected_err):
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="c",
                target_units=RecordTargetUnits(properties=[], unit_system_name="Imperial"),
                limit=1,
            )

    def test_sync_record_load_dump_round_trip(self) -> None:
        payload = {
            "space": "sp",
            "externalId": "rec-1",
            "createdTime": 100,
            "lastUpdatedTime": 200,
            "status": "updated",
            "properties": {"sp": {"c": {"temp": 22.5}}},
        }
        record = SyncRecord._load(payload)
        assert isinstance(record, SyncRecord)
        assert record.status == "updated"
        assert record.dump() == payload

    def test_sync_record_deleted_tombstone(self) -> None:
        record = SyncRecord._load(
            {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "deleted"}
        )
        assert record.status == "deleted"
        assert record.properties is None
        assert "properties" not in record.dump()
