from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import filters
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
    RecordContainerId,
    RecordId,
    RecordsAggregation,
    RecordSource,
    RecordWrite,
    Sum,
    TimeHistogram,
    TimeHistogramAggregateResult,
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
            last_updated_time={"gte": 1_000_000},
            filter=filters.Equals(["space"], "sp"),
            target_units={"unitSystemName": "SI"},
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
