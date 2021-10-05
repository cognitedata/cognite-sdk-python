import json
import queue
import re
import time
from collections import OrderedDict

import pytest

from cognite.client._api.assets import Asset, AssetList, AssetUpdate, _AssetPoster, _AssetPosterWorker
from cognite.client.data_classes import AssetFilter, Label, LabelFilter, TimestampRange
from cognite.client.exceptions import CogniteAPIError
from tests.utils import jsgz_load, set_request_limit

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
def mock_assets_response(rsps, cognite_client):
    response_body = {"items": [EXAMPLE_ASSET]}
    url_pattern = re.compile(re.escape(cognite_client.assets._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_get_subtree_base(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/byids",
        status=200,
        json={"items": [{"id": 1}]},
    )
    rsps.add(
        rsps.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=200,
        json={"items": [{"id": 2, "parentId": 1}, {"id": 3, "parentId": 1}, {"id": 4, "parentId": 1}]},
    )
    rsps.add(
        rsps.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=200,
        json={"items": [{"id": 5, "parentId": 2}, {"id": 6, "parentId": 2}]},
    )
    rsps.add(
        rsps.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=200,
        json={"items": [{"id": 7, "parentId": 3}, {"id": 8, "parentId": 3}]},
    )
    rsps.add(
        rsps.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=200,
        json={"items": [{"id": 9, "parentId": 4}, {"id": 10, "parentId": 4}]},
    )
    yield rsps


@pytest.fixture
def mock_get_subtree(mock_get_subtree_base, cognite_client):
    mock_get_subtree_base.add(
        mock_get_subtree_base.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=200,
        json={"items": []},
    )
    yield mock_get_subtree_base


@pytest.fixture
def mock_get_subtree_w_request_failure(mock_get_subtree_base, cognite_client):
    mock_get_subtree_base.add(
        mock_get_subtree_base.POST,
        cognite_client.assets._get_base_url_with_base_path() + "/assets/list",
        status=503,
        json={"error": {"message": "Service Unavailable"}},
    )
    yield mock_get_subtree_base


class TestAssets:
    def test_retrieve_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.retrieve(id=1)
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.retrieve_multiple(ids=[1])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.list(name="bla")
        assert "bla" == jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["name"]
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_aggregated_properties_param(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", aggregated_properties=["childCount"])
        assert ["childCount"] == jsgz_load(mock_assets_response.calls[0].request.body)["aggregatedProperties"]

    def test_list_with_aggregated_properties_param_when_snake_cased(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", aggregated_properties=["child_count"])
        assert ["childCount"] == jsgz_load(mock_assets_response.calls[0].request.body)["aggregatedProperties"]

    def test_list_with_dataset_ids(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(name="bla", data_set_ids=[1], data_set_external_ids=["x"])
        assert [{"id": 1}, {"externalId": "x"}] == jsgz_load(mock_assets_response.calls[0].request.body)["filter"][
            "dataSetIds"
        ]

    def test_list_root(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(root_ids=[{"id": 1}, {"externalId": "abc"}], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {"rootIds": [{"id": 1}, {"externalId": "abc"}]}} == jsgz_load(
            calls[0].request.body
        )

    def test_list_parent(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(parent_ids=[1, 2], parent_external_ids=["abc"], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"parentIds": [1, 2], "parentExternalIds": ["abc"]},
        } == jsgz_load(calls[0].request.body)

    def test_list_subtree(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(asset_subtree_ids=1, asset_subtree_external_ids=["a"], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"externalId": "a"}]},
        } == jsgz_load(calls[0].request.body)

    def test_list_with_time_dict(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(created_time={"min": 20})
        assert 20 == jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["createdTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["createdTime"]

    def test_call_root(self, cognite_client, mock_assets_response):
        list(cognite_client.assets.__call__(root_ids=[{"id": 1}, {"externalId": "abc"}], limit=10))
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {"rootIds": [{"id": 1}, {"externalId": "abc"}]}} == jsgz_load(
            calls[0].request.body
        )

    def test_list_root_ids_list(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(root_ids=[1, 2], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {"rootIds": [{"id": 1}, {"id": 2}]}} == jsgz_load(
            calls[0].request.body
        )

    def test_list_root_extids_list(self, cognite_client, mock_assets_response):
        cognite_client.assets.list(root_external_ids=["1", "2"], limit=10)
        calls = mock_assets_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"rootIds": [{"externalId": "1"}, {"externalId": "2"}]},
        } == jsgz_load(calls[0].request.body)

    def test_create_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.create(Asset(external_id="1", name="blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.create([Asset(external_id="1", name="blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_assets_response):
        for asset in cognite_client.assets:
            assert mock_assets_response.calls[0].response.json()["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_assets_response):
        for assets in cognite_client.assets(chunk_size=1):
            assert mock_assets_response.calls[0].response.json()["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=1)
        assert {"items": [{"id": 1}], "recursive": False, "ignoreUnknownIds": False} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )
        assert res is None

    def test_delete_single_recursive(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=1, recursive=True)
        assert {"items": [{"id": 1}], "recursive": True, "ignoreUnknownIds": False} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.delete(id=[1], ignore_unknown_ids=True)
        assert {"items": [{"id": 1}], "recursive": False, "ignoreUnknownIds": True} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update(Asset(id=1))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update(AssetUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.update([AssetUpdate(id=1).description.set("blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_update_labels_single(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.add("PUMP").labels.remove("VALVE")])
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "VALVE"}]}}
        assert expected == jsgz_load(mock_assets_response.calls[0].request.body)["items"][0]["update"]

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
        assert expected == jsgz_load(mock_assets_response.calls[0].request.body)["items"][0]["update"]

    def test_update_labels_set_single(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.set("PUMP")])
        expected = {"labels": {"set": [{"externalId": "PUMP"}]}}
        assert expected == jsgz_load(mock_assets_response.calls[0].request.body)["items"][0]["update"]

    def test_update_labels_set_multiple(self, cognite_client, mock_assets_response):
        cognite_client.assets.update([AssetUpdate(id=1).labels.set(["PUMP", "VALVE"])])
        expected = {"labels": {"set": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]}}
        assert expected == jsgz_load(mock_assets_response.calls[0].request.body)["items"][0]["update"]

    # resource.update doesn't support full replacement of labels (set operation)
    def test_ignore_labels_resource_class(self, cognite_client, mock_assets_response):
        cognite_client.assets.update(Asset(id=1, labels=[Label(external_id="Pump")], name="Abc"))
        assert {"name": {"set": "Abc"}} == jsgz_load(mock_assets_response.calls[0].request.body)["items"][0]["update"]

    def test_labels_filter_contains_all(self, cognite_client, mock_assets_response):
        my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
        cognite_client.assets.list(labels=my_label_filter)
        assert {"containsAll": [{"externalId": "PUMP"}, {"externalId": "VERIFIED"}]} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )["filter"]["labels"]

    def test_labels_filter_contains_any(self, cognite_client, mock_assets_response):
        my_label_filter = LabelFilter(contains_any=["PUMP", "VALVE"])
        cognite_client.assets.list(labels=my_label_filter)
        assert {"containsAny": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )["filter"]["labels"]

    def test_create_asset_with_label(self, cognite_client, mock_assets_response):
        cognite_client.assets.create(
            Asset(name="test", labels=[Label(external_id="PUMP"), Label(external_id="VERIFIED")])
        )
        assert {"name": "test", "labels": [{"externalId": "PUMP"}, {"externalId": "VERIFIED"}]} == jsgz_load(
            mock_assets_response.calls[0].request.body
        )["items"][0]

    def test_search(self, cognite_client, mock_assets_response):
        res = cognite_client.assets.search(filter=AssetFilter(name="1"))
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"name": "1"},
            "limit": 100,
        } == jsgz_load(mock_assets_response.calls[0].request.body)

    @pytest.mark.parametrize("filter_field", ["parent_ids", "parentIds"])
    def test_search_dict_filter(self, cognite_client, mock_assets_response, filter_field):
        res = cognite_client.assets.search(filter={filter_field: "bla"})
        assert mock_assets_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"parentIds": "bla"},
            "limit": 100,
        } == jsgz_load(mock_assets_response.calls[0].request.body)

    def test_get_subtree(self, cognite_client, mock_get_subtree):
        assets = cognite_client.assets.retrieve_subtree(id=1)
        assert len(assets) == 10
        for i, asset in enumerate(assets):
            assert asset.id == i + 1

    def test_get_subtree_w_depth(self, cognite_client, mock_get_subtree):
        mock_get_subtree.assert_all_requests_are_fired = False
        assets = cognite_client.assets.retrieve_subtree(id=1, depth=1)
        assert len(assets) == 4
        for i, asset in enumerate(assets):
            assert asset.id == i + 1

    def test_get_subtree_w_error(self, cognite_client, mock_get_subtree_w_request_failure):
        with pytest.raises(CogniteAPIError):
            cognite_client.assets.retrieve_subtree(id=1)

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


class TestAssetPosterWorker:
    def test_run(self, cognite_client, mock_assets_response):
        q_req = queue.Queue()
        q_res = queue.Queue()

        w = _AssetPosterWorker(request_queue=q_req, response_queue=q_res, client=cognite_client.assets)
        w.start()
        q_req.put([Asset()])
        time.sleep(0.1)
        w.stop = True
        assert [Asset._load(mock_assets_response.calls[0].response.json()["items"][0])] == q_res.get()
        assert 1 == len(mock_assets_response.calls)


def generate_asset_tree(root_external_id: str, depth: int, children_per_node: int, current_depth=1):
    assert 1 <= children_per_node <= 10, "children_per_node must be between 1 and 10"
    assets = []
    if current_depth == 1:
        assets = [Asset(external_id=root_external_id)]
    if depth > current_depth:
        for i in range(children_per_node):
            asset = Asset(parent_external_id=root_external_id, external_id="{}{}".format(root_external_id, i))
            assets.append(asset)
            if depth > current_depth + 1:
                assets.extend(
                    generate_asset_tree(root_external_id + str(i), depth, children_per_node, current_depth + 1)
                )
    return assets


class TestAssetPoster:
    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self, cognite_client):
        assets = [Asset(external_id="1"), Asset(parent_external_id="1", parent_id=1, external_id="2")]
        with pytest.raises(AssertionError, match="has both"):
            _AssetPoster(assets, cognite_client.assets)

    def test_validate_asset_hierarchy_duplicate_ref_ids(self, cognite_client):
        assets = [Asset(external_id="1"), Asset(parent_external_id="1", external_id="1")]
        with pytest.raises(AssertionError, match="Duplicate"):
            _AssetPoster(assets, cognite_client.assets)

    def test_validate_asset_hierarchy__more_than_limit_only_resolved_assets(self, cognite_client):
        with set_request_limit(cognite_client.assets, 1):
            _AssetPoster(
                [Asset(external_id="a1", parent_id=1), Asset(external_id="a2", parent_id=2)], cognite_client.assets
            )

    def test_validate_asset_hierarchy_circular_dependencies(self, cognite_client):
        assets = [
            Asset(external_id="1", parent_external_id="3"),
            Asset(external_id="2", parent_external_id="1"),
            Asset(external_id="3", parent_external_id="2"),
        ]
        with set_request_limit(cognite_client.assets, 1):
            with pytest.raises(AssertionError, match="circular dependencies"):
                _AssetPoster(assets, cognite_client.assets)

    def test_validate_asset_hierarchy_self_dependency(self, cognite_client):
        assets = [Asset(external_id="1"), Asset(external_id="2", parent_external_id="2")]
        with set_request_limit(cognite_client.assets, 1):
            with pytest.raises(AssertionError, match="circular dependencies"):
                _AssetPoster(assets, cognite_client.assets)

    def test_initialize(self, cognite_client):
        assets = [
            Asset(external_id="1"),
            Asset(external_id="3", parent_external_id="1"),
            Asset(external_id="2", parent_external_id="1"),
            Asset(external_id="4", parent_external_id="2"),
        ]

        ap = _AssetPoster(assets, cognite_client.assets)
        assert OrderedDict({str(i): None for i in range(1, 5)}) == ap.remaining_external_ids
        assert {
            "1": {Asset(external_id="2", parent_external_id="1"), Asset(external_id="3", parent_external_id="1")},
            "2": {Asset(external_id="4", parent_external_id="2")},
            "3": set(),
            "4": set(),
        } == ap.external_id_to_children
        assert {"1": 3, "2": 1, "3": 0, "4": 0} == ap.external_id_to_descendent_count
        assert ap.assets_remaining() is True
        assert 0 == len(ap.posted_assets)
        assert ap.request_queue.empty()
        assert ap.response_queue.empty()
        assert {"1", "2", "3", "4"} == ap.remaining_external_ids_set

    def test_get_unblocked_assets__assets_unblocked_by_default_less_than_limit(self, cognite_client):
        assets = generate_asset_tree(root_external_id="0", depth=4, children_per_node=10)
        ap = _AssetPoster(assets=assets, client=cognite_client.assets)
        unblocked_assets_lists = ap._get_unblocked_assets()
        assert 1 == len(unblocked_assets_lists)
        assert 1000 == len(unblocked_assets_lists[0])

    def test_get_unblocked_assets__assets_unblocked_by_default_more_than_limit(self, cognite_client):
        assets = []
        for i in range(4):
            assets.extend(generate_asset_tree(root_external_id=str(i), depth=2, children_per_node=2))
        with set_request_limit(cognite_client.assets, 3):
            ap = _AssetPoster(assets=assets, client=cognite_client.assets)
            unblocked_assets_lists = ap._get_unblocked_assets()
        assert 4 == len(unblocked_assets_lists)
        for li in unblocked_assets_lists:
            assert 3 == len(li)

    def test_get_unblocked_assets_parent_ref_null_pointer(self, cognite_client):
        assets = [Asset(parent_external_id="1", external_id="2")]
        ap = _AssetPoster(assets, cognite_client.assets)
        asset_list = ap._get_unblocked_assets()
        assert len(asset_list) == 1

    @pytest.fixture
    def mock_post_asset_hierarchy(self, cognite_client, rsps):
        cognite_client.assets._config.max_workers = 1

        def request_callback(request):
            items = jsgz_load(request.body)["items"]
            response_assets = []
            for item in items:
                parent_id = None
                if "parentId" in item:
                    parent_id = item["parentId"]
                if "parentExternalId" in item:
                    parent_id = item["parentExternalId"] + "id"
                id = item.get("externalId", "root_") + "id"
                response_assets.append(
                    {
                        "id": id,
                        "parentId": parent_id,
                        "externalId": item["externalId"],
                        "parentExternalId": item.get("parentExternalId"),
                    }
                )
            return 200, {}, json.dumps({"items": response_assets})

        rsps.add_callback(
            rsps.POST,
            cognite_client.assets._get_base_url_with_base_path() + "/assets",
            callback=request_callback,
            content_type="application/json",
        )
        yield rsps
        cognite_client.assets._config.max_workers = 10

    @pytest.mark.parametrize(
        "limit, depth, children_per_node, expected_num_calls",
        [(100, 4, 10, 13), (9, 3, 9, 11), (100, 101, 1, 2), (1, 10, 1, 10)],
    )
    def test_post_hierarchy(
        self, cognite_client, limit, depth, children_per_node, expected_num_calls, mock_post_asset_hierarchy
    ):
        assets = generate_asset_tree(root_external_id="0", depth=depth, children_per_node=children_per_node)
        with set_request_limit(cognite_client.assets, limit):
            created_assets = cognite_client.assets.create_hierarchy(assets)

        assert len(assets) == len(created_assets)
        assert expected_num_calls - 1 <= len(mock_post_asset_hierarchy.calls) <= expected_num_calls + 1
        for asset in created_assets:
            if asset.id == "0id":
                assert asset.parent_id is None
            else:
                assert asset.id[:-3] == asset.parent_id[:-2]

    def test_post_assets_over_limit_only_resolved(self, cognite_client, mock_post_asset_hierarchy):
        with set_request_limit(cognite_client.assets, 1):
            _AssetPoster(
                [Asset(external_id="a1", parent_id=1), Asset(external_id="a2", parent_id=2)], cognite_client.assets
            ).post()
        assert 2 == len(mock_post_asset_hierarchy.calls)

    @pytest.fixture
    def mock_post_assets_failures(self, cognite_client, rsps):
        def request_callback(request):
            items = jsgz_load(request.body)["items"]
            response_assets = []
            item = items[0]
            parent_id = None
            if "parentId" in item:
                parent_id = item["parentId"]
            if "parentExternalId" in item:
                parent_id = item["parentExternalId"] + "id"
            id = item.get("refId", "root_") + "id"
            response_assets.append(
                {
                    "id": id,
                    "parentId": parent_id,
                    "externalId": item["externalId"],
                    "parentExternalId": item.get("parentExternalId"),
                }
            )

            if item["name"] == "400":
                return 400, {}, json.dumps({"error": {"message": "user error", "code": 400}})

            if item["name"] == "500":
                return 500, {}, json.dumps({"error": {"message": "internal server error", "code": 500}})

            return 200, {}, json.dumps({"items": response_assets})

        rsps.add_callback(
            rsps.POST,
            cognite_client.assets._get_base_url_with_base_path() + "/assets",
            callback=request_callback,
            content_type="application/json",
        )
        with set_request_limit(cognite_client.assets, 1):
            yield rsps

    def test_post_with_failures(self, cognite_client, mock_post_assets_failures):
        assets = [
            Asset(name="200", external_id="0"),
            Asset(name="200", parent_external_id="0", external_id="01"),
            Asset(name="400", parent_external_id="0", external_id="02"),
            Asset(name="200", parent_external_id="02", external_id="021"),
            Asset(name="200", parent_external_id="021", external_id="0211"),
            Asset(name="500", parent_external_id="0", external_id="03"),
            Asset(name="200", parent_external_id="03", external_id="031"),
        ]
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.assets.create_hierarchy(assets)

        assert {a.external_id for a in e.value.unknown} == {"03"}
        assert {a.external_id for a in e.value.failed} == {"02", "021", "0211", "031"}
        assert {a.external_id for a in e.value.successful} == {"0", "01"}


@pytest.fixture
def mock_assets_empty(rsps, cognite_client):
    url_pattern = re.compile(re.escape(cognite_client.assets._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json={"items": []})
    yield rsps


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
        df = asset.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert 1 == df.loc["id"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]

    # need subtree here to get list, since to_pandas on a single Asset gives int for id, but on AssetList it gives int64
    def test_asset_id_from_to_pandas(self, cognite_client, mock_get_subtree):
        df = cognite_client.assets.retrieve_subtree(id=1).to_pandas()
        cognite_client.assets.retrieve(id=df.iloc[0]["id"])
