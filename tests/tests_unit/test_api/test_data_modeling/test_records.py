from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling.aggregates import (
    Average,
    Count,
    Filters,
    FiltersResult,
    Max,
    MetricResult,
    Min,
    MovingFunction,
    MovingFunctionResult,
    MovingFunctions,
    NumberHistogram,
    NumberHistogramResult,
    Sum,
    TimeHistogram,
    TimeHistogramResult,
    UniqueValues,
    UniqueValuesResult,
    UnknownResult,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference
from cognite.client.data_classes.data_modeling.ids import ContainerId, PropertyId, PropertyPath, ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, TypeInformation
from cognite.client.data_classes.data_modeling.records import (
    Record,
    RecordContainerId,
    RecordId,
    RecordList,
    RecordsAggregation,
    RecordSource,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordViewId,
    RecordWrite,
    SyncRecord,
    SyncRecordList,
    TimeRange,
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
                source=ContainerId(space="sp", external_id="container-x"),
                properties={"temperature": 22.5, "pressure": 1.013},
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
                            "properties": {"temperature": 22.5, "pressure": 1.013},
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
                            "properties": {"temperature": 22.5, "pressure": 1.013},
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
        assert out.dump()["aggregates"] == {"avg_temp": {"avg": 22.5}}
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
                        "avg_temp": Average(["sp", "container-x", "temp"]),
                        "moving_count": MovingFunction(
                            buckets_path="_count",
                            window=3,
                            function=MovingFunctions.UNWEIGHTED_AVG,
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
        assert isinstance(avg_temp, MetricResult)
        assert avg_temp.aggregate == "avg"
        assert avg_temp.value == 22.5

        by_region = loaded["by_region"]
        assert isinstance(by_region, UniqueValuesResult)
        assert by_region.buckets[0].value == "north"
        max_temp = by_region.buckets[0].aggregates["max_temp"]
        assert isinstance(max_temp, MetricResult)
        assert max_temp.value == 30.0

        by_number = loaded["by_number"]
        assert isinstance(by_number, NumberHistogramResult)
        assert by_number.buckets[0].interval_start == 0.0

        by_time = loaded["by_time"]
        assert isinstance(by_time, TimeHistogramResult)
        moving = by_time.buckets[0].aggregates["moving"]
        assert isinstance(moving, MovingFunctionResult)
        assert moving.fn_value == 7.5

        by_filter = loaded["by_filter"]
        assert isinstance(by_filter, FiltersResult)
        assert by_filter.buckets[0].count == 4

        assert isinstance(loaded["future"], UnknownResult)
        assert loaded["future"].dump() == {"futureAggregateResult": 1}

    def test_aggregate_results_dump_honors_camel_case(self) -> None:
        loaded = RecordsAggregation._load(
            {
                "aggregates": {
                    "avg_temp": {"avg": 22.5},
                    "moving": {"fnValue": 7.5},
                    "by_time": {
                        "timeHistogramBuckets": [
                            {
                                "intervalStart": "2024-05-16T00:00:00Z",
                                "count": 3,
                                "aggregates": {"moving": {"fnValue": 1.5}},
                            }
                        ]
                    },
                    "future": {"futureAggregateResult": 1},
                }
            }
        )

        # camel_case=True round-trips the API payload unchanged.
        assert loaded["avg_temp"].dump() == {"avg": 22.5}
        assert loaded["moving"].dump() == {"fnValue": 7.5}
        assert loaded["by_time"].dump() == {
            "timeHistogramBuckets": [
                {
                    "intervalStart": "2024-05-16T00:00:00Z",
                    "count": 3,
                    "aggregates": {"moving": {"fnValue": 1.5}},
                }
            ]
        }
        assert loaded["future"].dump() == {"futureAggregateResult": 1}

        # camel_case=False snake-cases every API key while leaving client-defined IDs untouched.
        assert loaded["moving"].dump(camel_case=False) == {"fn_value": 7.5}
        assert loaded["by_time"].dump(camel_case=False) == {
            "time_histogram_buckets": [
                {
                    "interval_start": "2024-05-16T00:00:00Z",
                    "count": 3,
                    "aggregates": {"moving": {"fn_value": 1.5}},
                }
            ]
        }
        assert loaded["future"].dump(camel_case=False) == {"future_aggregate_result": 1}

    def test_aggregate_dump_preserves_client_defined_ids(self) -> None:
        # The keys under "aggregates" are chosen by the caller (see the aggregate() examples), so
        # they are user data, not API fields: dump() must echo them back verbatim regardless of
        # camel_case. Only the API-defined field names inside each result get converted.
        loaded = RecordsAggregation._load(
            {
                "aggregates": {
                    "myTopLevelAvg": {"avg": 22.5},
                    "myTimeGroups": {
                        "timeHistogramBuckets": [
                            {
                                "intervalStart": "2024-05-16T00:00:00Z",
                                "count": 3,
                                "aggregates": {"myNestedMoving": {"fnValue": 1.5}},
                            }
                        ]
                    },
                    "myFutureShape": {"futureAggregateResult": 1},
                }
            }
        )

        assert loaded.dump(camel_case=False) == {
            "aggregates": {
                "myTopLevelAvg": {"avg": 22.5},
                "myTimeGroups": {
                    "time_histogram_buckets": [
                        {
                            "interval_start": "2024-05-16T00:00:00Z",
                            "count": 3,
                            "aggregates": {"myNestedMoving": {"fn_value": 1.5}},
                        }
                    ]
                },
                "myFutureShape": {"future_aggregate_result": 1},
            }
        }


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
            sources=[RecordSourceSelector(ContainerId(space="sp", external_id="container-x"), ["*"])],
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
        page = next(cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago"))
        assert isinstance(page, SyncRecordList)
        assert page.cursor == "abc"
        assert page.has_next is False
        assert page[0].status == "created"
        request = httpx_mock.get_requests()[0]
        assert request.url.path.endswith(f"/streams/{stream_id}/records/sync")
        assert jsgz_load(request.content) == {"initializeCursor": "7d-ago", "limit": 1000}

    def test_sync_with_cursor_sends_cursor(
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
        first = next(
            cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="2d-ago", chunk_size=1)
        )
        assert first.has_next is True
        assert first.cursor is not None
        second = next(cognite_client.data_modeling.records.sync(stream_id=stream_id, cursor=first.cursor, chunk_size=1))
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
        page = next(cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="c", chunk_size=1))
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
        page = next(
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id, initialize_cursor="c", include_typing=True, chunk_size=1
            )
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
        next(
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="c",
                target_units=RecordTargetUnits(unit_system_name="Imperial"),
                chunk_size=1,
            )
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["targetUnits"] == {"unitSystemName": "Imperial"}

    @pytest.mark.parametrize("has_next", [False, True])
    def test_sync_partial_chunk_yields_after_one_request(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
        has_next: bool,
    ) -> None:
        # The sync endpoint always returns a 'nextCursor' - that is what makes the feed
        # resumable - so a chunk holding fewer records than 'chunk_size' must not make the
        # SDK keep requesting. Only one response is registered, so any extra request fails.
        items = [{**record_response, "externalId": "rec-1", "status": "created"}]
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": items, "nextCursor": "abc", "hasNext": has_next},
        )
        page = next(
            cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago", chunk_size=10)
        )
        assert len(page) == 1
        assert page.cursor == "abc"
        assert page.has_next is has_next
        assert len(httpx_mock.get_requests()) == 1

    def test_sync_empty_chunk_yields_after_one_request(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        # A drained change feed returns no items, but still a cursor to resume from later.
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [], "nextCursor": "abc", "hasNext": False},
        )
        page = next(
            cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago", chunk_size=10)
        )
        assert len(page) == 0
        assert page.cursor == "abc"
        assert page.has_next is False
        assert len(httpx_mock.get_requests()) == 1

    def test_sync_with_cursor_partial_chunk_yields_after_one_request(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        items = [{**record_response, "externalId": "rec-1", "status": "updated"}]
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": items, "nextCursor": "p2", "hasNext": False},
        )
        page = next(cognite_client.data_modeling.records.sync(stream_id=stream_id, cursor="p1", chunk_size=10))
        assert len(page) == 1
        assert page.cursor == "p2"
        assert len(httpx_mock.get_requests()) == 1

    def test_sync_iterates_all_chunks(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        # Iteration keeps requesting chunks until hasNext is False, rotating the cursor
        # ('initializeCursor' only on the first request) and yielding one list per request.
        pages = [
            {
                "items": [{**record_response, "externalId": "rec-1", "status": "created"}],
                "nextCursor": "c1",
                "hasNext": True,
            },
            {
                "items": [{**record_response, "externalId": "rec-2", "status": "updated"}],
                "nextCursor": "c2",
                "hasNext": True,
            },
            {"items": [], "nextCursor": "c3", "hasNext": False},
        ]
        for page in pages:
            httpx_mock.add_response(method="POST", url=sync_url_pattern, status_code=200, json=page)
        chunks = list(
            cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago", chunk_size=2)
        )
        assert [[record.external_id for record in chunk] for chunk in chunks] == [["rec-1"], ["rec-2"], []]
        assert [chunk.cursor for chunk in chunks] == ["c1", "c2", "c3"]
        assert [chunk.has_next for chunk in chunks] == [True, True, False]
        bodies = [jsgz_load(request.content) for request in httpx_mock.get_requests()]
        assert bodies == [
            {"initializeCursor": "7d-ago", "limit": 2},
            {"cursor": "c1", "limit": 2},
            {"cursor": "c2", "limit": 2},
        ]

    def test_sync_with_cursor_iterates_all_chunks(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        pages = [
            {
                "items": [{**record_response, "externalId": "rec-1", "status": "created"}],
                "nextCursor": "c1",
                "hasNext": True,
            },
            {
                "items": [{**record_response, "externalId": "rec-2", "status": "deleted"}],
                "nextCursor": "c2",
                "hasNext": False,
            },
        ]
        for page in pages:
            httpx_mock.add_response(method="POST", url=sync_url_pattern, status_code=200, json=page)
        chunks = list(cognite_client.data_modeling.records.sync(stream_id=stream_id, cursor="p0"))
        assert [record.external_id for chunk in chunks for record in chunk] == ["rec-1", "rec-2"]
        assert chunks[-1].cursor == "c2"
        assert chunks[-1].has_next is False
        bodies = [jsgz_load(request.content) for request in httpx_mock.get_requests()]
        assert bodies == [{"cursor": "p0", "limit": 1000}, {"cursor": "c1", "limit": 1000}]

    @pytest.mark.parametrize("chunk_size", [0, -2, 1001])
    def test_sync_rejects_out_of_range_chunk_size(
        self,
        cognite_client: CogniteClient,
        stream_id: str,
        chunk_size: int,
    ) -> None:
        # 'chunk_size' is the page size of a single request, which the API caps at 1000.
        # Validation is lazy (the method is a generator), so the raise happens on next().
        with pytest.raises(ValueError, match="between 1 and 1000"):
            next(
                cognite_client.data_modeling.records.sync(
                    stream_id=stream_id, initialize_cursor="c", chunk_size=chunk_size
                )
            )

    def test_sync_requires_exactly_one_cursor(
        self,
        cognite_client: CogniteClient,
        stream_id: str,
    ) -> None:
        # The overloads enforce this statically; the runtime check covers untyped callers.
        with pytest.raises(ValueError, match="exactly one of 'initialize_cursor' or 'cursor'"):
            next(
                cognite_client.data_modeling.records.sync(  # type: ignore[call-overload]
                    stream_id=stream_id, initialize_cursor="7d-ago", cursor="abc"
                )
            )
        with pytest.raises(ValueError, match="exactly one of 'initialize_cursor' or 'cursor'"):
            next(cognite_client.data_modeling.records.sync(stream_id=stream_id))  # type: ignore[call-overload]

    def test_sync_body_shape_with_filter_and_sources(
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
            json={"items": [], "nextCursor": "z", "hasNext": False},
        )
        next(
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="2m-ago",
                filter=filters.Equals(property=["sp", "container-x", "temp"], value=22.5),
                sources=[
                    RecordSourceSelector(source=ContainerId(space="sp", external_id="container-x"), properties=["*"])
                ],
                chunk_size=5,
            )
        )
        assert jsgz_load(httpx_mock.get_requests()[0].content) == {
            "initializeCursor": "2m-ago",
            "limit": 5,
            "filter": {"equals": {"property": ["sp", "container-x", "temp"], "value": 22.5}},
            "sources": [
                {"source": {"space": "sp", "externalId": "container-x", "type": "container"}, "properties": ["*"]}
            ],
        }


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
        assert loaded.sources[0].properties == {"temperature": 22.5, "pressure": 1.013}

    def test_record_source_reference_dump(self) -> None:
        ref = ContainerId(space="s", external_id="c")
        d = ref.dump()
        assert d == {"type": "container", "space": "s", "externalId": "c"}

    def test_record_source_dump(self) -> None:
        src = RecordSource(
            source=ContainerId(space="s", external_id="c"),
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
        selector = RecordSourceSelector(ContainerId(space="sp", external_id="c"), ["temp", "pressure"])
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
            next(
                cognite_client.data_modeling.records.sync(
                    stream_id=stream_id,
                    initialize_cursor="c",
                    target_units=RecordTargetUnits(),
                )
            )

    def test_record_target_units_rejects_multiple_request_modes(
        self, cognite_client: CogniteClient, stream_id: str
    ) -> None:
        expected_err = "Provide exactly one of 'properties' or 'unit_system_name'."
        with pytest.raises(ValueError, match=expected_err):
            next(
                cognite_client.data_modeling.records.sync(
                    stream_id=stream_id,
                    initialize_cursor="c",
                    target_units=RecordTargetUnits(properties=[], unit_system_name="Imperial"),
                )
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

    def test_sync_record_list_public_load(self) -> None:
        items = [
            {"space": "sp", "externalId": f"rec-{i}", "createdTime": 1, "lastUpdatedTime": 2, "status": "created"}
            for i in range(2)
        ]
        page = SyncRecordList.load(items)
        assert isinstance(page, SyncRecordList)
        assert [record.external_id for record in page] == ["rec-0", "rec-1"]
        assert page.cursor is None
        assert page.has_next is False
        assert page.typing is None


class TestRecordsAPIFilterLimit:
    @pytest.mark.parametrize("limit", [1001, 5000])
    def test_filter_rejects_limit_above_max(self, cognite_client: CogniteClient, stream_id: str, limit: int) -> None:
        with pytest.raises(ValueError, match="'limit' must be between 1 and 1000"):
            cognite_client.data_modeling.records.filter(stream_id=stream_id, limit=limit)

    @pytest.mark.parametrize("limit", [None, -1])
    def test_filter_rejects_unlimited(self, cognite_client: CogniteClient, stream_id: str, limit: object) -> None:
        with pytest.raises((TypeError, ValueError), match="'limit'"):
            cognite_client.data_modeling.records.filter(stream_id=stream_id, limit=limit)  # type: ignore[arg-type]


class TestRecordPropertyPathValidation:
    """The records DTOs take property paths too, with the same bare-string hazard."""

    def test_record_target_unit_rejects_bare_string_property(self) -> None:
        with pytest.raises(TypeError, match="'property' must be a sequence of strings"):
            RecordTargetUnit("sp.c.temp", UnitReference("temperature:deg_c"))  # type: ignore[arg-type]

    def test_record_source_selector_rejects_bare_string_properties(self) -> None:
        with pytest.raises(TypeError, match="'properties' must be a sequence of strings"):
            RecordSourceSelector(ContainerId(space="sp", external_id="c"), "temp")  # type: ignore[arg-type]

    def test_record_source_selector_rejects_no_properties(self) -> None:
        # The API requires minItems: 1 for properties.
        with pytest.raises(ValueError, match="'properties' must not be empty"):
            RecordSourceSelector(ContainerId(space="sp", external_id="c"), [])

    def test_record_target_unit_accepts_view_property_reference(self) -> None:
        target_unit = RecordTargetUnit(
            (ViewId("sp", "my_view", "v1"), "pressure"),
            UnitReference("pressure:pa"),
        )
        assert target_unit.dump() == {
            "property": ["sp", "my_view/v1", "pressure"],
            "unit": {"externalId": "pressure:pa"},
        }

    def test_source_property_tuple_rejects_non_string_property(self) -> None:
        with pytest.raises(TypeError, match="must have a string property"):
            RecordTargetUnit((ViewId("sp", "my_view", "v1"), 42), UnitReference("pressure:pa"))  # type: ignore[arg-type]


class TestRecordViewId:
    def test_init_and_attributes(self) -> None:
        view_id = RecordViewId(space="my_space", external_id="my_view", version="v1")
        assert view_id.space == "my_space"
        assert view_id.external_id == "my_view"
        assert view_id.version == "v1"
        assert view_id.as_tuple() == ("my_space", "my_view", "v1")
        assert view_id.as_source_identifier() == "my_view/v1"
        assert view_id.as_property_ref("temp") == ("my_space", "my_view/v1", "temp")

    def test_version_is_required(self) -> None:
        # Unlike a general ViewId, record view sources must be fully versioned.
        with pytest.raises(TypeError):
            RecordViewId(space="my_space", external_id="my_view")  # type: ignore[call-arg]

    def test_dump(self) -> None:
        view_id = RecordViewId(space="my_space", external_id="my_view", version="v1")
        assert view_id.dump() == {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "type": "view",
        }

    def test_load(self) -> None:
        raw = {"space": "my_space", "externalId": "my_view", "version": "v1", "type": "view"}
        loaded = RecordViewId.load(raw)
        assert isinstance(loaded, RecordViewId)
        assert loaded.space == "my_space"
        assert loaded.external_id == "my_view"
        assert loaded.version == "v1"

    def test_load_from_tuple(self) -> None:
        loaded = RecordViewId.load(("my_space", "my_view", "v1"))
        assert isinstance(loaded, RecordViewId)
        assert loaded.as_tuple() == ("my_space", "my_view", "v1")


class TestRecordSourceViews:
    def test_source_with_record_view_id(self) -> None:
        src = RecordSource(
            source=RecordViewId("sp", "my_view", "v1"),
            properties={"temp": 25.0},
        )
        assert isinstance(src.source, RecordViewId)
        assert src.dump() == {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": {"temp": 25.0},
        }

    def test_source_with_view_id(self) -> None:
        src = RecordSource(
            source=ViewId("sp", "my_view", "v1"),
            properties={"temp": 25.0},
        )
        assert isinstance(src.source, ViewId)
        assert src.dump() == {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": {"temp": 25.0},
        }

    def test_source_with_tuple(self) -> None:
        src = RecordSource(
            source=("sp", "my_view", "v1"),
            properties={"temp": 25.0},
        )
        assert isinstance(src.source, ViewId)
        assert src.dump()["source"] == {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"}

    def test_source_load_view(self) -> None:
        raw = {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": {"temp": 25.0},
        }
        loaded = RecordSource._load(raw)
        assert isinstance(loaded.source, ViewId)
        assert loaded.source.space == "sp"
        assert loaded.source.external_id == "my_view"
        assert loaded.source.version == "v1"
        assert loaded.properties == {"temp": 25.0}

    def test_source_accepts_snake_case_dict(self) -> None:
        view_src = RecordSource(
            source={"space": "sp", "external_id": "my_view", "version": "v1"},  # type: ignore[arg-type]
            properties={"temp": 25.0},
        )
        assert view_src.source == RecordViewId("sp", "my_view", "v1")

        container_src = RecordSource(
            source={"space": "sp", "external_id": "my_container"},  # type: ignore[arg-type]
            properties={"pressure": 1.0},
        )
        assert container_src.source == RecordContainerId("sp", "my_container")

    def test_source_load_rejects_unknown_type(self) -> None:
        raw = {"source": {"space": "sp", "externalId": "x", "type": "node"}, "properties": {}}
        with pytest.raises(ValueError, match="must be 'container' or 'view', but was 'node'"):
            RecordSource._load(raw)

    def test_source_rejects_wrong_tuple_length(self) -> None:
        with pytest.raises(ValueError, match="Invalid tuple length"):
            RecordSource(source=("sp",), properties={})  # type: ignore[arg-type]

    def test_source_rejects_view_without_version(self) -> None:
        with pytest.raises(ValueError, match="requires an explicit version"):
            RecordSource(source=ViewId("sp", "my_view"), properties={})


class TestRecordSourceSelectorViews:
    def test_selector_with_view_id(self) -> None:
        sel = RecordSourceSelector(
            source=ViewId("sp", "my_view", "v1"),
            properties=["temp", "humidity"],
        )
        assert isinstance(sel.source, ViewId)
        assert sel.dump() == {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": ["temp", "humidity"],
        }

    def test_selector_with_tuple(self) -> None:
        sel = RecordSourceSelector(
            source=("sp", "my_view", "v1"),
            properties=["*"],
        )
        assert isinstance(sel.source, ViewId)
        assert sel.dump() == {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": ["*"],
        }

    def test_selector_load_view(self) -> None:
        raw = {
            "source": {"space": "sp", "externalId": "my_view", "version": "v1", "type": "view"},
            "properties": ["temp"],
        }
        loaded = RecordSourceSelector._load(raw)
        assert isinstance(loaded.source, ViewId)
        assert loaded.properties == ["temp"]


class TestRecordsAPIViewsOperations:
    def test_ingest_with_view_sources(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_ingest: None,
        stream_id: str,
    ) -> None:
        write_record = RecordWrite(
            space="sp",
            external_id="rec-1",
            sources=[
                RecordSource(
                    source=ViewId(space="sp", external_id="my_view", version="v1"),
                    properties={"temp": 22.5},
                )
            ],
        )
        cognite_client.data_modeling.records.ingest(write_record, stream_id=stream_id)
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
                            "source": {"type": "view", "space": "sp", "externalId": "my_view", "version": "v1"},
                            "properties": {"temp": 22.5},
                        }
                    ],
                }
            ]
        }

    def test_upsert_with_view_sources(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_upsert: None,
        stream_id: str,
    ) -> None:
        write_record = RecordWrite(
            space="sp",
            external_id="rec-1",
            sources=[
                RecordSource(
                    source=("sp", "my_view", "v1"),
                    properties={"temp": 23.0},
                )
            ],
        )
        cognite_client.data_modeling.records.upsert(write_record, stream_id=stream_id)
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
                            "source": {"type": "view", "space": "sp", "externalId": "my_view", "version": "v1"},
                            "properties": {"temp": 23.0},
                        }
                    ],
                }
            ]
        }

    def test_filter_with_view_sources(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.filter(
            stream_id=stream_id,
            sources=[RecordSourceSelector(source=ViewId("sp", "my_view", "v1"), properties=["temp"])],
        )
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body["sources"] == [
            {
                "source": {"type": "view", "space": "sp", "externalId": "my_view", "version": "v1"},
                "properties": ["temp"],
            }
        ]

    def test_sync_with_view_sources(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            json={
                "items": [],
                "nextCursor": "cur-1",
                "hasNext": False,
            },
        )
        feed = cognite_client.data_modeling.records.sync(
            stream_id=stream_id,
            initialize_cursor="1d-ago",
            sources=[RecordSourceSelector(source=("sp", "my_view", "v1"), properties=["*"])],
        )
        list(feed)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body["sources"] == [
            {
                "source": {"type": "view", "space": "sp", "externalId": "my_view", "version": "v1"},
                "properties": ["*"],
            }
        ]

    def test_record_as_write_reconstructs_view_sources(self) -> None:
        raw_record = {
            "space": "sp",
            "externalId": "rec-1",
            "createdTime": 100,
            "lastUpdatedTime": 200,
            "properties": {
                "sp": {
                    "my_view/v1": {"temp": 22.5},
                    "my_container": {"pressure": 1.0},
                }
            },
        }
        record = Record._load(raw_record)
        write_rec = record.as_write()
        assert isinstance(write_rec, RecordWrite)
        assert len(write_rec.sources) == 2

        view_src = next(s for s in write_rec.sources if isinstance(s.source, RecordViewId))
        assert isinstance(view_src.source, RecordViewId)
        assert view_src.source.space == "sp"
        assert view_src.source.external_id == "my_view"
        assert view_src.source.version == "v1"
        assert view_src.properties == {"temp": 22.5}

        cnt_src = next(s for s in write_rec.sources if isinstance(s.source, RecordContainerId))
        assert isinstance(cnt_src.source, RecordContainerId)
        assert cnt_src.source.space == "sp"
        assert cnt_src.source.external_id == "my_container"
        assert cnt_src.properties == {"pressure": 1.0}


class TestRecordsAggregateWithViews:
    def test_aggregates_with_view_property_references(self) -> None:
        view = ViewId("my_space", "my_view", "v1")
        rec_view = RecordViewId("my_space", "my_view", "v1")

        # Every accepted way of referencing a view property should dump identically:
        properties: list[PropertyPath] = [
            (view, "temperature"),
            (rec_view, "temperature"),
            view.as_property_ref("temperature"),
            PropertyId(view, "temperature"),
        ]
        for property_ in properties:
            avg = Average(property=property_)
            assert avg.dump() == {"avg": {"property": ["my_space", "my_view/v1", "temperature"]}}

        assert Sum((view, "score")).dump() == {"sum": {"property": ["my_space", "my_view/v1", "score"]}}
        assert Min((view, "score")).dump() == {"min": {"property": ["my_space", "my_view/v1", "score"]}}
        assert Max((view, "score")).dump() == {"max": {"property": ["my_space", "my_view/v1", "score"]}}
        assert Count((view, "score")).dump() == {"count": {"property": ["my_space", "my_view/v1", "score"]}}
        assert UniqueValues((view, "player")).dump() == {
            "uniqueValues": {"property": ["my_space", "my_view/v1", "player"]}
        }
        assert NumberHistogram((view, "score"), interval=10.0).dump() == {
            "numberHistogram": {"property": ["my_space", "my_view/v1", "score"], "interval": 10.0}
        }
        assert TimeHistogram((view, "ts"), calendar_interval="1d").dump() == {
            "timeHistogram": {"property": ["my_space", "my_view/v1", "ts"], "calendarInterval": "1d"}
        }

    def test_aggregate_api_call_with_view_aggregates_and_filters(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        stream_id: str,
        records_base_url: str,
    ) -> None:
        view = ViewId("my_space", "my_view", "v1")
        httpx_mock.add_response(
            method="POST",
            url=records_base_url + "/aggregate",
            json={"aggregates": {"avg_temp": {"avg": 22.5}}},
        )
        res = cognite_client.data_modeling.records.aggregate(
            stream_id=stream_id,
            aggregates={"avg_temp": Average((view, "temperature"))},
            filter=filters.Equals((view, "status"), "active"),
        )
        avg_res = res["avg_temp"]
        assert isinstance(avg_res, MetricResult)
        assert avg_res.value == 22.5
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {
            "aggregates": {
                "avg_temp": {
                    "avg": {
                        "property": ["my_space", "my_view/v1", "temperature"],
                    }
                }
            },
            "filter": {
                "equals": {
                    "property": ["my_space", "my_view/v1", "status"],
                    "value": "active",
                }
            },
        }
