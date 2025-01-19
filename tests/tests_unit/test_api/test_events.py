import re

import pytest

from cognite.client._api.events import Event, EventList, EventUpdate
from cognite.client.data_classes import (
    AggregateResult,
    EndTimeFilter,
    EventFilter,
    TimestampRange,
)
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_events_response(httpx_mock, cognite_client):
    response_body = {
        "items": [
            {
                "externalId": "string",
                "startTime": 0,
                "endTime": 0,
                "description": "string",
                "metadata": {"metadata-key": "metadata-value"},
                "assetIds": [1],
                "source": "string",
                "id": 1,
                "lastUpdatedTime": 0,
            }
        ]
    }

    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/.+")
    # ....assert_all_requests_are_fired = False  # TODO

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


@pytest.fixture
def mock_count_aggregate_response(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/events/aggregate")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": [{"count": 10}]})
    yield httpx_mock


@pytest.fixture
def mock_aggregate_unique_values_response(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/events/aggregate")
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json={"items": [{"count": 5, "value": "WORKORDER"}]}
    )
    yield httpx_mock


class TestEvents:
    def test_retrieve_single(self, cognite_client, mock_events_response):
        res = cognite_client.events.retrieve(id=1)
        assert isinstance(res, Event)
        assert mock_events_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_events_response):
        res = cognite_client.events.retrieve_multiple(ids=[1])
        assert isinstance(res, EventList)
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_events_response):
        res = cognite_client.events.list(source="bla")
        assert "bla" == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["source"]
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_partitions(self, cognite_client, mock_events_response):
        cognite_client.events.list(partitions=10, limit=float("inf"))
        assert 10 == len(mock_events_response.get_requests())

    def test_list_with_dataset_ids(self, cognite_client, mock_events_response):
        cognite_client.events.list(source="bla", data_set_ids=[1], data_set_external_ids=["x"])
        assert [{"id": 1}, {"externalId": "x"}] == jsgz_load(mock_events_response.get_requests()[0].content)["filter"][
            "dataSetIds"
        ]

    def test_list_sorting(self, cognite_client, mock_events_response):
        res = cognite_client.events.list(sort=["startTime:desc"])
        modern_sort_expr = [
            {
                "property": ["startTime"],
                "order": "desc",
                "nulls": "auto",
            },
        ]
        assert modern_sort_expr == jsgz_load(mock_events_response.get_requests()[0].content)["sort"]
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_sorting_combined_with_partitions(self, cognite_client, mock_events_response):
        with pytest.raises(ValueError):
            cognite_client.events.list(sort=["startTime:desc"], partitions=10)

    def test_list_with_time_dict(self, cognite_client, mock_events_response):
        cognite_client.events.list(start_time={"min": 20})
        assert 20 == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_events_response):
        cognite_client.events.list(start_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.get_requests()[0].content)["filter"]["startTime"]

    def test_count_aggregate(self, cognite_client, mock_count_aggregate_response):
        res = cognite_client.events.aggregate(filter={"type": "WORKORDER"})
        assert isinstance(res[0], AggregateResult)
        assert res[0].count == 10

    def test_call_root(self, cognite_client, mock_events_response):
        list(cognite_client.events(asset_subtree_external_ids=["a"], limit=10))
        calls = mock_events_response.get_requests()
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"externalId": "a"}]},
        } == jsgz_load(calls[0].content)

    def test_list_subtree(self, cognite_client, mock_events_response):
        cognite_client.events.list(limit=10, asset_subtree_external_ids=["a"], asset_subtree_ids=[1, 2])
        calls = mock_events_response.get_requests()
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"id": 2}, {"externalId": "a"}]},
        } == jsgz_load(calls[0].content)

    def test_list_ongoing_wrong_signature(self, cognite_client):
        with pytest.raises(ValueError):
            EndTimeFilter(is_null=True, max=100)

    def test_create_single(self, cognite_client, mock_events_response):
        res = cognite_client.events.create(Event(external_id="1"))
        assert isinstance(res, Event)
        assert mock_events_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_events_response):
        res = cognite_client.events.create([Event(external_id="1")])
        assert isinstance(res, EventList)
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_events_response):
        for event in cognite_client.events:
            assert mock_events_response.get_requests()[0].response.json()["items"][0] == event.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_events_response):
        for events in cognite_client.events(chunk_size=1):
            assert mock_events_response.get_requests()[0].response.json()["items"] == events.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_events_response):
        res = cognite_client.events.delete(id=1)
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_events_response):
        res = cognite_client.events.delete(id=[1], ignore_unknown_ids=True)
        assert {"ignoreUnknownIds": True, "items": [{"id": 1}]} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_events_response):
        res = cognite_client.events.update(Event(id=1))
        assert isinstance(res, Event)
        assert mock_events_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_events_response):
        res = cognite_client.events.update(EventUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Event)
        assert mock_events_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_events_response):
        res = cognite_client.events.update([EventUpdate(id=1).description.set("blabla")])
        assert isinstance(res, EventList)
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_search(self, cognite_client, mock_events_response):
        res = cognite_client.events.search(filter=EventFilter(external_id_prefix="abc"))
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(self, cognite_client, mock_events_response, filter_field):
        res = cognite_client.events.search(filter={filter_field: "bla"})
        assert mock_events_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "bla"}, "limit": 25} == jsgz_load(
            mock_events_response.get_requests()[0].content
        )

    def test_event_update_object(self):
        assert isinstance(
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
            .start_time.set(None),
            EventUpdate,
        )


@pytest.fixture
def mock_events_empty(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.events)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    yield httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_event_list_to_pandas(self, cognite_client, mock_events_response):
        import pandas as pd

        df = cognite_client.events.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_event_list_to_pandas_empty(self, cognite_client, mock_events_empty):
        import pandas as pd

        df = cognite_client.events.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_event_to_pandas(self, cognite_client, mock_events_response):
        import pandas as pd

        df = cognite_client.events.retrieve(id=1).to_pandas(expand_metadata=True, metadata_prefix="", camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [1] == df.loc["assetIds"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]
