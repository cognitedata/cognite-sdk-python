import re

import pytest

from cognite.client import CogniteClient
from cognite.client._api.events import Event, EventList, EventUpdate
from cognite.client.data_classes import EventFilter, TimestampRange
from tests.utils import jsgz_load

EVENTS_API = CogniteClient().events


@pytest.fixture
def mock_events_response(rsps):
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

    url_pattern = re.compile(re.escape(EVENTS_API._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestEvents:
    def test_retrieve_single(self, mock_events_response):
        res = EVENTS_API.retrieve(id=1)
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, mock_events_response):
        res = EVENTS_API.retrieve_multiple(ids=[1])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, mock_events_response):
        res = EVENTS_API.list(source="bla")
        assert "bla" == jsgz_load(mock_events_response.calls[0].request.body)["filter"]["source"]
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_partitions(self, mock_events_response):
        EVENTS_API.list(partitions=13, limit=float("inf"))
        assert 13 == len(mock_events_response.calls)

    def test_list_sorting(self, mock_events_response):
        res = EVENTS_API.list(sort=["startTime:desc"])
        assert ["startTime:desc"] == jsgz_load(mock_events_response.calls[0].request.body)["sort"]
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_sorting_combined_with_partitions(self, mock_events_response):
        with pytest.raises(ValueError):
            EVENTS_API.list(sort=["startTime:desc"], partitions=10)

    def test_list_with_time_dict(self, mock_events_response):
        EVENTS_API.list(start_time={"min": 20})
        assert 20 == jsgz_load(mock_events_response.calls[0].request.body)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.calls[0].request.body)["filter"]["startTime"]

    def test_list_with_timestamp_range(self, mock_events_response):
        EVENTS_API.list(start_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_events_response.calls[0].request.body)["filter"]["startTime"]["min"]
        assert "max" not in jsgz_load(mock_events_response.calls[0].request.body)["filter"]["startTime"]

    def test_call_root(self, mock_events_response):
        list(
            EVENTS_API.__call__(
                root_asset_ids=[23], root_asset_external_ids=["a", "b"], asset_subtree_external_ids=["a"], limit=10
            )
        )
        calls = mock_events_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {
                "rootAssetIds": [{"id": 23}, {"externalId": "a"}, {"externalId": "b"}],
                "assetSubtreeIds": [{"externalId": "a"}],
            },
        } == jsgz_load(calls[0].request.body)

    def test_list_root_ids_list(self, mock_events_response):
        EVENTS_API.list(root_asset_ids=[1, 2], limit=10)
        calls = mock_events_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {"rootAssetIds": [{"id": 1}, {"id": 2}]}} == jsgz_load(
            calls[0].request.body
        )

    def test_list_root_extids_list(self, mock_events_response):
        EVENTS_API.list(root_asset_external_ids=["1", "2"], limit=10)
        calls = mock_events_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"rootAssetIds": [{"externalId": "1"}, {"externalId": "2"}]},
        } == jsgz_load(calls[0].request.body)

    def test_list_subtree(self, mock_events_response):
        res = EVENTS_API.list(limit=10, asset_subtree_external_ids=["a"], asset_subtree_ids=[1, 2])
        calls = mock_events_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"id": 2}, {"externalId": "a"}]},
        } == jsgz_load(calls[0].request.body)

    def test_create_single(self, mock_events_response):
        res = EVENTS_API.create(Event(external_id="1"))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_events_response):
        res = EVENTS_API.create([Event(external_id="1")])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_events_response):
        for event in EVENTS_API:
            assert mock_events_response.calls[0].response.json()["items"][0] == event.dump(camel_case=True)

    def test_iter_chunk(self, mock_events_response):
        for events in EVENTS_API(chunk_size=1):
            assert mock_events_response.calls[0].response.json()["items"] == events.dump(camel_case=True)

    def test_delete_single(self, mock_events_response):
        res = EVENTS_API.delete(id=1)
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} == jsgz_load(
            mock_events_response.calls[0].request.body
        )
        assert res is None

    def test_delete_multiple(self, mock_events_response):
        res = EVENTS_API.delete(id=[1], ignore_unknown_ids=True)
        assert {"ignoreUnknownIds": True, "items": [{"id": 1}]} == jsgz_load(mock_events_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, mock_events_response):
        res = EVENTS_API.update(Event(id=1))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, mock_events_response):
        res = EVENTS_API.update(EventUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_events_response):
        res = EVENTS_API.update([EventUpdate(id=1).description.set("blabla")])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_search(self, mock_events_response):
        res = EVENTS_API.search(filter=EventFilter(external_id_prefix="abc"))
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 100} == jsgz_load(
            mock_events_response.calls[0].request.body
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(self, mock_events_response, filter_field):
        res = EVENTS_API.search(filter={filter_field: "bla"})
        assert mock_events_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"description": None}, "filter": {"externalIdPrefix": "bla"}, "limit": 100} == jsgz_load(
            mock_events_response.calls[0].request.body
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
def mock_events_empty(rsps):
    url_pattern = re.compile(re.escape(EVENTS_API._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json={"items": []})
    yield rsps


@pytest.mark.dsl
class TestPandasIntegration:
    def test_event_list_to_pandas(self, mock_events_response):
        import pandas as pd

        df = EVENTS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_event_list_to_pandas_empty(self, mock_events_empty):
        import pandas as pd

        df = EVENTS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_event_to_pandas(self, mock_events_response):
        import pandas as pd

        df = EVENTS_API.retrieve(id=1).to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [1] == df.loc["assetIds"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]
