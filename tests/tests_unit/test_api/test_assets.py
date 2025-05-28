import re

import pytest
from httpx import Request as HttpxRequest # Added for respx side_effect type hinting
from httpx import Response as HttpxResponse # Added for respx side_effect type hinting


from cognite.client._api.assets import Asset, AssetList, AssetUpdate
from cognite.client.data_classes import AggregateResultItem, AssetFilter, Label, LabelFilter, TimestampRange
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import convert_all_keys_to_snake_case
from tests.utils import jsgz_load

EXAMPLE_ASSET = {
    "externalId": "string",
    "name": "string",
    "parentId": 1,
    "description": "string",
    "metadata": {"metadata-key": "metadata-value"},
    "labels": [{"externalId": "PUMP"}],
    "source": "string",
    "id": 1,
    "lastUpdatedTime": 0,
    "rootId": 1,
}


@pytest.fixture
def mock_assets_response(respx_mock, cognite_client): # Changed rsps to respx_mock
    response_body = {"items": [EXAMPLE_ASSET]}
    url_pattern = re.compile(re.escape(cognite_client.assets._get_base_url_with_base_path()) + "/.+")
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json=response_body)
    yield respx_mock


@pytest.fixture
def mock_get_subtree_base(respx_mock, cognite_client): # Changed rsps to respx_mock
    base_assets_api_path = cognite_client.assets._get_base_url_with_base_path() + "/assets"
    
    respx_mock.post(f"{base_assets_api_path}/byids").respond(status_code=200, json={"items": [{"id": 1}]})
    
    # Mock multiple calls to /assets/list. Assuming the SDK makes these calls sequentially
    # and respx will match them in the order they are defined if the URL is identical.
    # If request bodies are different (e.g. due to cursors), respx can distinguish.
    # For this fixture, we define them sequentially.
    respx_mock.post(f"{base_assets_api_path}/list").respond(
        status_code=200, json={"items": [{"id": 2, "parentId": 1}, {"id": 3, "parentId": 1}, {"id": 4, "parentId": 1}]}
    )
    respx_mock.post(f"{base_assets_api_path}/list").respond(
        status_code=200, json={"items": [{"id": 5, "parentId": 2}, {"id": 6, "parentId": 2}]}
    )
    respx_mock.post(f"{base_assets_api_path}/list").respond(
        status_code=200, json={"items": [{"id": 7, "parentId": 3}, {"id": 8, "parentId": 3}]}
    )
    respx_mock.post(f"{base_assets_api_path}/list").respond(
        status_code=200, json={"items": [{"id": 9, "parentId": 4}, {"id": 10, "parentId": 4}]}
    )
    # Ensure a final empty list to stop pagination if the test relies on it
    respx_mock.post(f"{base_assets_api_path}/list").respond(status_code=200, json={"items": []})
    yield respx_mock


@pytest.fixture
def mock_get_subtree(mock_get_subtree_base): # mock_get_subtree_base now uses respx_mock
    # The base fixture should already cover the necessary calls.
    # If an additional empty response is needed specifically for this, ensure it's the last one.
    yield mock_get_subtree_base


@pytest.fixture
def mock_get_subtree_w_request_failure(mock_get_subtree_base, cognite_client): # mock_get_subtree_base uses respx_mock
    # This will add another route. If the URL is the same as others in mock_get_subtree_base,
    # it needs to be specific enough not to interfere, or be called in a specific order.
    # Let's assume this is for a specific call that fails.
    url_to_fail = cognite_client.assets._get_base_url_with_base_path() + "/assets/list"
    # Add a new route that fails, or modify an existing one if respx allows dynamic changes
    # For simplicity, adding a new route that will be matched if previous ones for the same URL aren't.
    # This might require careful test structure to ensure this failing route is hit.
    # A more robust approach is to make this route very specific if the request body differs,
    # or use a side_effect in mock_get_subtree_base to control failures.
    mock_get_subtree_base.post(url_to_fail).respond(
        status_code=500, json={"error": {"message": "Service Unavailable"}}
    )
    yield mock_get_subtree_base


class TestAssets:
    def test_retrieve_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.retrieve(id=1)
        assert isinstance(res, Asset)
        assert mock_assets_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.retrieve_multiple(ids=[1])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.list(name="bla")
        assert "bla" == jsgz_load(mock_assets_response.calls.last.request.content)["filter"]["name"]
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_aggregated_properties_param(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", aggregated_properties=["childCount"])
        assert ["childCount"] == jsgz_load(mock_assets_response.calls.last.request.content)["aggregatedProperties"]

    def test_list_with_aggregated_properties_param_when_snake_cased(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", aggregated_properties=["child_count"])
        assert ["childCount"] == jsgz_load(mock_assets_response.calls.last.request.content)["aggregatedProperties"]

    def test_list_with_dataset_ids(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", data_set_ids=[1], data_set_external_ids=["x"])
        assert [{"id": 1}, {"externalId": "x"}] == jsgz_load(mock_assets_response.calls.last.request.content)["filter"][
            "dataSetIds"
        ]

    def test_list_parent(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(parent_ids=[1, 2], parent_external_ids=["abc"], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"parentIds": [1, 2], "parentExternalIds": ["abc"]},
        } == jsgz_load(calls.last.request.content)

    def test_list_subtree(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(asset_subtree_ids=1, asset_subtree_external_ids=["a"], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"externalId": "a"}]},
        } == jsgz_load(calls.last.request.content)

    def test_list_with_time_dict(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(created_time={"min": 20})
        assert 20 == jsgz_load(mock_assets_response.calls.last.request.content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_assets_response.calls.last.request.content)["filter"]["createdTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_assets_response.calls.last.request.content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_assets_response.calls.last.request.content)["filter"]["createdTime"]

    def test_create_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.create(Asset(external_id="1", name="blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.create([Asset(external_id="1", name="blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_assets_response):
        for asset in cognite_client.assets:
            assert mock_assets_response.calls.last.response.json()["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_assets_response):
        for assets in cognite_client.assets(chunk_size=1):
            assert mock_assets_response.calls.last.response.json()["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=1)
        assert {"items": [{"id": 1}], "recursive": False, "ignoreUnknownIds": False} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )
        assert res is None

    def test_delete_single_recursive(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=1, recursive=True)
        assert {"items": [{"id": 1}], "recursive": True, "ignoreUnknownIds": False} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=[1], ignore_unknown_ids=True)
        assert {"items": [{"id": 1}], "recursive": False, "ignoreUnknownIds": True} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update(Asset(id=1))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update(AssetUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update([AssetUpdate(id=1).description.set("blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_update_labels_single(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.add("PUMP").labels.remove("VALVE")])
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "VALVE"}]}}
        assert expected == jsgz_load(mock_assets_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_multiple(self, cognite_client, mock_assets_response):
        cognite_client.assets.update(
            [AssetUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["VALVE", "VERIFIED"])]
        )
        expected = {
            "labels": {
                "add": [{"externalId": "PUMP"}, {"externalId": "ROTATING_EQUIPMENT"}],
                "remove": [{"externalId": "VALVE"}, {"externalId": "VERIFIED"}],
            }
        }
        assert expected == jsgz_load(mock_assets_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_set_single(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.set("PUMP")])
        expected = {"labels": {"set": [{"externalId": "PUMP"}]}}
        assert expected == jsgz_load(mock_assets_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_set_multiple(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.set(["PUMP", "VALVE"])])
        expected = {"labels": {"set": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]}}
        assert expected == jsgz_load(mock_assets_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_resource_class(self, cognite_client, mock_assets_response):
        cognite_client.assets.update(Asset(id=1, labels=[Label(external_id="Pump")], name="Abc"))
        expected = {"name": {"set": "Abc"}, "labels": {"set": [{"externalId": "Pump"}]}}
        assert expected == jsgz_load(mock_assets_response.calls.last.request.content)["items"][0]["update"]

    def test_labels_filter_contains_all(self, cognite_client, mock_assets_response):
        my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
        cognite_client.assets.list(labels=my_label_filter)
        assert {"containsAll": [{"externalId": "PUMP"}, {"externalId": "VERIFIED"}]} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )["filter"]["labels"]

    def test_labels_filter_contains_any(self, cognite_client, mock_assets_response):
        my_label_filter = LabelFilter(contains_any=["PUMP", "VALVE"])
        cognite_client.assets.list(labels=my_label_filter)
        assert {"containsAny": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )["filter"]["labels"]

    def test_create_asset_with_label(self, cognite_client, mock_assets_response):
        cognite_client.assets.create(
            Asset(name="test", labels=[Label(external_id="PUMP"), Label(external_id="VERIFIED")])
        )
        assert {"name": "test", "labels": [{"externalId": "PUMP"}, {"externalId": "VERIFIED"}]} == jsgz_load(
            mock_assets_response.calls.last.request.content
        )["items"][0]

    def test_search(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.search(filter=AssetFilter(name="1"))
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"name": "1"},
            "limit": 25,
        } == jsgz_load(mock_assets_response.calls.last.request.content)

    @pytest.mark.parametrize("filter_field", ["parent_ids", "parentIds"])
    def test_search_dict_filter(self, cognite_client, mock_assets_response, filter_field):
        res = cognite_client.assets.search(filter={filter_field: "bla"})
        assert mock_assets_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"parentIds": "bla"},
            "limit": 25,
        } == jsgz_load(mock_assets_response.calls.last.request.content)

    def test_get_subtree(self, cognite_client, mock_get_subtree_base): # Changed mock_get_subtree to mock_get_subtree_base for clarity
        # Ensure the routes are set up for multiple calls if retrieve_subtree makes them
        # This test might need more specific route definitions if the list calls have different bodies/params
        assets = cognite_client.assets.retrieve_subtree(id=1)
        assert len(assets) == 10
        for i, asset in enumerate(assets):
            assert asset.id == i + 1
        # Add assertions on mock_get_subtree_base.calls if specific call order/content is important

    def test_get_subtree_w_depth(self, cognite_client, mock_get_subtree_base): # Changed mock_get_subtree to mock_get_subtree_base
        # mock_get_subtree_base.assert_all_requests_are_fired = False # Not directly applicable to respx
        assets = cognite_client.assets.retrieve_subtree(id=1, depth=1)
        assert len(assets) == 4
        for i, asset in enumerate(assets):
            assert asset.id == i + 1
        # Add assertions on mock_get_subtree_base.calls

    def test_get_subtree_w_error(self, cognite_client, mock_get_subtree_w_request_failure):
        with pytest.raises(CogniteAPIError):
            cognite_client.assets.retrieve_subtree(id=1)
        # Add assertions on mock_get_subtree_w_request_failure.calls

    def test_assets_update_object(self):
        assert isinstance(
            AssetUpdate(1)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .metadata.set(None)
            .labels.add(["PUMP"])
            .labels.remove(["VALVE"])
            .name.set("")
            .name.set(None)
            .source.set(1)
            .source.set(None)
            .data_set_id.set(123),
            AssetUpdate,
        )


@pytest.fixture
def mock_assets_empty(respx_mock, cognite_client): # Changed rsps to respx_mock
    url_pattern = re.compile(re.escape(cognite_client.assets._get_base_url_with_base_path()) + "/.+")
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json={"items": []})
    yield respx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_asset_list_to_pandas(self, cognite_client, mock_assets_response):
        import pandas as pd

        df = cognite_client.assets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_asset_list_to_pandas_empty(self, cognite_client, mock_assets_empty):
        import pandas as pd

        df = cognite_client.assets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_asset_to_pandas(self, cognite_client, mock_assets_response):
        import pandas as pd

        asset = cognite_client.assets.retrieve(id=1)
        df = asset.to_pandas(expand_metadata=True, metadata_prefix="")
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert 1 == df.loc["id"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]

    def test_expand_aggregates(self):
        agg_props = {"childCount": 0, "depth": 4, "path": [{"id": 35927223}, {"id": 20283836}, {"id": 296}]}
        asset = Asset(name="foo", aggregates=AggregateResultItem._load(agg_props))
        expanded = asset.to_pandas(expand_aggregates=True)
        not_expanded = asset.to_pandas(expand_aggregates=False)

        assert expanded.columns == ["value"]  # This was wrongly col=0 prior to 7.8.1
        assert not_expanded.columns == ["value"]
        assert convert_all_keys_to_snake_case(agg_props) == not_expanded.loc["aggregates"].item()
        assert agg_props["childCount"] == expanded.loc["aggregates.child_count"].item()
        assert agg_props["depth"] == expanded.loc["aggregates.depth"].item()
        assert agg_props["path"] == expanded.loc["aggregates.path"].item()

    # need subtree here to get list, since to_pandas on a single Asset gives int for id, but on AssetList it gives int64
    def test_asset_id_from_to_pandas(self, cognite_client, mock_get_subtree):
        df = cognite_client.assets.retrieve_subtree(id=1).to_pandas()
        cognite_client.assets.retrieve(id=df.iloc[0]["id"])
