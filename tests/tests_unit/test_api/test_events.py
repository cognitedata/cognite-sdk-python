import re
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    AggregateResult,
    EndTimeFilter,
    TimestampRange,
)
from cognite.client.data_classes.events import Event, EventFilter, EventList, EventUpdate, EventWrite
from tests.tests_unit.conftest import DefaultResourceGenerator
from tests.utils import get_url, jsgz_load


@pytest.fixture
def example_event() -> dict[str, Any]:
    return {
        "externalId": "string",
        "startTime": 0,
        "endTime": 0,
        "description": "string",
        "metadata": {"metadata-key": "metadata-value"},
        "assetIds": [1],
        "source": "string",
        "id": 1,
        "lastUpdatedTime": 0,
        "createdTime": 0,
    }


@pytest.fixture
def mock_events_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, example_event: dict[str, Any]
) -> Iterator[HTTPXMock]:
    response_body = {"items": [example_event]}
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/.+")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield httpx_mock


@pytest.fixture
def mock_count_aggregate_response(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> Iterator[HTTPXMock]:
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/events/aggregate")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": [{"count": 10}]})
    yield httpx_mock


@pytest.fixture
def mock_aggregate_unique_values_response(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> Iterator[HTTPXMock]:
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/events/aggregate")
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json={"items": [{"count": 5, "value": "WORKORDER"}]}
    )
    yield httpx_mock


class TestEvents:
    def test_retrieve_single(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.retrieve(id=1)
        assert isinstance(res, Event)
        assert example_event == res.dump(camel_case=True)

    def test_retrieve_multiple(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.retrieve_multiple(ids=[1])
        assert isinstance(res, EventList)
        assert [example_event] == res.dump(camel_case=True)

    def test_list(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.list(source="bla")
        assert "bla" == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["source"]
        assert [example_event] == res.dump(camel_case=True)

    def test_list_partitions(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        for _ in range(10):
            httpx_mock.add_response(
                method="POST", url=get_url(cognite_client.events) + "/events/list", status_code=200, json={"items": []}
            )
        cognite_client.events.list(partitions=10, limit=float("inf"))  # type: ignore[arg-type]
        assert 10 == len(httpx_mock.get_requests())

    def test_list_with_dataset_ids(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        cognite_client.events.list(source="bla", data_set_ids=[1], data_set_external_ids=["x"])
        assert [{"id": 1}, {"externalId": "x"}] == jsgz_load(mock_events_response.get_requests()[0].content)["filter"][
            "dataSetIds"
        ]

    def test_list_sorting(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.list(sort=["startTime:desc"])
        modern_sort_expr = [
            {
                "property": ["startTime"],
                "order": "desc",
                "nulls": "auto",
            },
        ]
        assert modern_sort_expr == jsgz_load(mock_events_response.get_requests()[0].content)["sort"]
        assert [example_event] == res.dump(camel_case=True)

    def test_list_sorting_combined_with_partitions(
        self, cognite_client: CogniteClient, mock_events_response: Any
    ) -> None:
        with pytest.raises(ValueError):
            cognite_client.events.list(sort=["startTime:desc"], partitions=10)

    def test_list_with_time_dict(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        cognite_client.events.list(start_time={"min": 20})
        assert 20 == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]

    def test_list_with_timestamp_range(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        cognite_client.events.list(start_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]

    def test_count_aggregate(self, cognite_client: CogniteClient, mock_count_aggregate_response: Any) -> None:
        res = cognite_client.events.aggregate(filter={"type": "WORKORDER"})
        assert isinstance(res[0], AggregateResult)
        assert res[0].count == 10

    def test_call_root(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        list(cognite_client.events(asset_subtree_external_ids=["a"], limit=10))
        calls = mock_events_response.get_requests()
        assert 1 == len(calls)
        expected = {"limit": 10, "filter": {"assetSubtreeIds": [{"externalId": "a"}]}}
        assert expected == jsgz_load(calls[0].content)

    def test_list_subtree(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        cognite_client.events.list(limit=10, asset_subtree_external_ids=["a"], asset_subtree_ids=[1, 2])
        calls = mock_events_response.get_requests()
        assert 1 == len(calls)
        expected = {"limit": 10, "filter": {"assetSubtreeIds": [{"id": 1}, {"id": 2}, {"externalId": "a"}]}}
        assert expected == jsgz_load(calls[0].content)

    def test_list_ongoing_wrong_signature(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError):
            EndTimeFilter(is_null=True, max=100)

    def test_create_single(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.create(EventWrite(external_id="1"))
        assert isinstance(res, Event)
        assert example_event == res.dump(camel_case=True)

    def test_create_multiple(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.create([EventWrite(external_id="1")])
        assert isinstance(res, EventList)
        assert [example_event] == res.dump(camel_case=True)

    def test_iter_single(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        for event in cognite_client.events:
            assert example_event == event.dump(camel_case=True)

    def test_iter_chunk(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        for events in cognite_client.events(chunk_size=1):
            assert [example_event] == events.dump(camel_case=True)

    def test_delete_single(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        res = cognite_client.events.delete(id=1)
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )
        assert res is None

    def test_delete_multiple(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        res = cognite_client.events.delete(id=[1], ignore_unknown_ids=True)
        assert {"ignoreUnknownIds": True, "items": [{"id": 1}]} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )
        assert res is None

    def test_update_with_resource_class(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.update(DefaultResourceGenerator.event(id=1))
        assert isinstance(res, Event)
        assert example_event == res.dump(camel_case=True)

    def test_update_with_update_class(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.update(EventUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Event)
        assert example_event == res.dump(camel_case=True)

    def test_update_multiple(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.update([EventUpdate(id=1).description.set("blabla")])
        assert isinstance(res, EventList)
        assert [example_event] == res.dump(camel_case=True)

    def test_search(
        self, cognite_client: CogniteClient, mock_events_response: HTTPXMock, example_event: dict[str, Any]
    ) -> None:
        res = cognite_client.events.search(filter=EventFilter(external_id_prefix="abc"))
        assert [example_event] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(
        self,
        cognite_client: CogniteClient,
        mock_events_response: HTTPXMock,
        filter_field: str,
        example_event: dict[str, Any],
    ) -> None:
        res = cognite_client.events.search(filter={filter_field: "bla"})
        assert [example_event] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "bla"}, "limit": 25} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )

    def test_event_update_object(self) -> None:
        update = (
            EventUpdate(1)
            .asset_ids.add([])
            .asset_ids.remove([])
            .description.set("")
            .description.set(None)
            .end_time.set(1)
            .end_time.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .source.set(1)
            .source.set(None)
            .start_time.set(1)
            .start_time.set(None)
        )
        assert isinstance(update, EventUpdate)


@pytest.fixture
def mock_events_empty(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> Iterator[HTTPXMock]:
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    yield httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_event_list_to_pandas(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        import pandas as pd

        df = cognite_client.events.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df.at[0, "metadata"]

    def test_event_list_to_pandas_empty(self, cognite_client: CogniteClient, mock_events_empty: Any) -> None:
        import pandas as pd

        events = cognite_client.events.list()
        df = events.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_event_to_pandas(self, cognite_client: CogniteClient, mock_events_response: Any) -> None:
        import pandas as pd

        event = cognite_client.events.retrieve(id=1)
        assert event
        df = event.to_pandas(expand_metadata=True, metadata_prefix="", camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [1] == df.at["assetIds", "value"]
        assert "metadata-value" == df.at["metadata-key", "value"]
