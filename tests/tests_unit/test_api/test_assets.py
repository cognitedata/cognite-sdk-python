import json
import queue
import re
import time
from collections import OrderedDict

import pytest

from cognite.client import CogniteClient
from cognite.client._api.assets import Asset, AssetList, AssetUpdate, _AssetPoster, _AssetPosterWorker
from cognite.client.exceptions import CogniteAssetPostingError
from tests.utils import jsgz_load

ASSETS_API = CogniteClient().assets


@pytest.fixture
def mock_assets_response(rsps):
    response_body = {
        "data": {
            "items": [
                {
                    "path": [0],
                    "externalId": "string",
                    "name": "string",
                    "parentId": 1,
                    "description": "string",
                    "metadata": {"metadata-key": "metadata-value"},
                    "source": "string",
                    "id": 1,
                    "lastUpdatedTime": 0,
                    "depth": 0,
                }
            ]
        }
    }

    url_pattern = re.compile(re.escape(ASSETS_API._base_url) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def set_limit():
    def set_limit(limit):
        ASSETS_API._LIMIT = limit

    limit_tmp = ASSETS_API._LIMIT
    yield set_limit
    ASSETS_API._LIMIT = limit_tmp


class TestAssets:
    def test_get_single(self, mock_assets_response):
        res = ASSETS_API.get(id=1)
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_get_multiple(self, mock_assets_response):
        res = ASSETS_API.get(id=[1])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_list(self, mock_assets_response):
        res = ASSETS_API.list(name="bla")
        assert "bla" == jsgz_load(mock_assets_response.calls[0].request.body)["filter"]["name"]
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_create_single(self, mock_assets_response):
        res = ASSETS_API.create(Asset(external_id="1", name="blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_assets_response):
        res = ASSETS_API.create([Asset(external_id="1", name="blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_assets_response):
        for asset in ASSETS_API:
            assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, mock_assets_response):
        for assets in ASSETS_API(chunk_size=1):
            assert mock_assets_response.calls[0].response.json()["data"]["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, mock_assets_response):
        res = ASSETS_API.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_assets_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_assets_response):
        res = ASSETS_API.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_assets_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, mock_assets_response):
        res = ASSETS_API.update(Asset(id=1))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, mock_assets_response):
        res = ASSETS_API.update(AssetUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_assets_response):
        res = ASSETS_API.update([AssetUpdate(id=1).description.set("blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_search(self, mock_assets_response):
        res = ASSETS_API.search()
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_assets_update_object(self):
        assert isinstance(
            AssetUpdate(1)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.add({})
            .metadata.remove([])
            .metadata.set({})
            .metadata.set(None)
            .name.set("")
            .name.set(None)
            .source.set(1)
            .source.set(None),
            AssetUpdate,
        )


class TestAssetPosterWorker:
    def test_run(self, mock_assets_response):
        q_req = queue.Queue()
        q_res = queue.Queue()

        w = _AssetPosterWorker(request_queue=q_req, response_queue=q_res, client=ASSETS_API)
        w.start()
        q_req.put([Asset()])
        time.sleep(0.1)
        w.stop = True
        assert [Asset._load(mock_assets_response.calls[0].response.json()["data"]["items"][0])] == q_res.get()
        assert 1 == len(mock_assets_response.calls)


def generate_asset_tree(root_ref_id: str, depth: int, children_per_node: int, current_depth=1):
    assert 1 <= children_per_node <= 10, "children_per_node must be between 1 and 10"
    assets = []
    if current_depth == 1:
        assets = [Asset(ref_id=root_ref_id)]
    if depth > current_depth:
        for i in range(children_per_node):
            asset = Asset(parent_ref_id=root_ref_id, ref_id="{}{}".format(root_ref_id, i))
            assets.append(asset)
            if depth > current_depth + 1:
                assets.extend(generate_asset_tree(root_ref_id + str(i), depth, children_per_node, current_depth + 1))
    return assets


class TestAssetPoster:
    def test_validate_asset_hierarchy_parent_ref_null_pointer(self):
        assets = [Asset(parent_ref_id="1", ref_id="2")]
        with pytest.raises(AssertionError, match="does not point"):
            _AssetPoster(assets, ASSETS_API)

    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1", parent_id=1, ref_id="2")]
        with pytest.raises(AssertionError, match="has both"):
            _AssetPoster(assets, ASSETS_API)

    def test_validate_asset_hierarchy_duplicate_ref_ids(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1", ref_id="1")]
        with pytest.raises(AssertionError, match="Duplicate"):
            _AssetPoster(assets, ASSETS_API)

    def test_validate_asset_hierarchy__more_than_limit_only_resolved_assets(self, set_limit):
        set_limit(1)
        _AssetPoster([Asset(parent_id=1), Asset(parent_id=2)], ASSETS_API)

    def test_validate_asset_hierarchy_circular_dependencies(self, set_limit):
        set_limit(1)
        assets = [
            Asset(ref_id="1", parent_ref_id="3"),
            Asset(ref_id="2", parent_ref_id="1"),
            Asset(ref_id="3", parent_ref_id="2"),
        ]
        with pytest.raises(AssertionError, match="circular dependencies"):
            _AssetPoster(assets, ASSETS_API)

    def test_validate_asset_hierarchy_self_dependency(self, set_limit):
        set_limit(1)
        assets = [Asset(ref_id="1"), Asset(ref_id="2", parent_ref_id="2")]
        with pytest.raises(AssertionError, match="circular dependencies"):
            _AssetPoster(assets, ASSETS_API)

    def test_initialize(self):
        assets = [
            Asset(ref_id="1"),
            Asset(ref_id="3", parent_ref_id="1"),
            Asset(ref_id="2", parent_ref_id="1"),
            Asset(ref_id="4", parent_ref_id="2"),
        ]

        ap = _AssetPoster(assets, ASSETS_API)
        assert OrderedDict({str(i): None for i in range(1, 5)}) == ap.remaining_ref_ids
        assert {} == ap.ref_id_to_id
        assert {
            "1": {Asset(ref_id="2", parent_ref_id="1"), Asset(ref_id="3", parent_ref_id="1")},
            "2": {Asset(ref_id="4", parent_ref_id="2")},
            "3": set(),
            "4": set(),
        } == ap.ref_id_to_children
        assert {"1": 3, "2": 1, "3": 0, "4": 0} == ap.ref_id_to_descendent_count
        assert ap.assets_remaining() is True
        assert 0 == len(ap.posted_assets)
        assert ap.request_queue.empty()
        assert ap.response_queue.empty()
        assert {"1", "2", "3", "4"} == ap.remaining_ref_ids_set

    def test_get_unblocked_assets__assets_unblocked_by_default_less_than_limit(self):
        assets = generate_asset_tree(root_ref_id="0", depth=4, children_per_node=10)
        ap = _AssetPoster(assets=assets, client=ASSETS_API)
        unblocked_assets_lists = ap._get_unblocked_assets()
        assert 1 == len(unblocked_assets_lists)
        assert 1000 == len(unblocked_assets_lists[0])

    def test_get_unblocked_assets__assets_unblocked_by_default_more_than_limit(self, set_limit):
        set_limit(3)
        assets = []
        for i in range(4):
            assets.extend(generate_asset_tree(root_ref_id=str(i), depth=2, children_per_node=2))
        ap = _AssetPoster(assets=assets, client=ASSETS_API)
        unblocked_assets_lists = ap._get_unblocked_assets()
        assert 4 == len(unblocked_assets_lists)
        for li in unblocked_assets_lists:
            assert 3 == len(li)

    @pytest.fixture
    def mock_post_asset_hierarchy(self, rsps):
        ASSETS_API._max_workers = 1

        def request_callback(request):
            items = jsgz_load(request.body)["items"]
            response_assets = []
            for item in items:
                parent_id = None
                if "parentId" in item:
                    parent_id = item["parentId"]
                if "parentRefId" in item:
                    parent_id = item["parentRefId"] + "id"
                id = item.get("refId", "root_") + "id"
                response_assets.append({"id": id, "parentId": parent_id, "path": [parent_id or "", id]})
            return 200, {}, json.dumps({"data": {"items": response_assets}})

        rsps.add_callback(
            rsps.POST, ASSETS_API._base_url + "/assets", callback=request_callback, content_type="application/json"
        )
        yield rsps
        ASSETS_API._max_workers = 10

    @pytest.mark.parametrize(
        "limit, depth, children_per_node, expected_num_calls",
        [(100, 4, 10, 13), (9, 3, 9, 11), (100, 101, 1, 2), (1, 10, 1, 10)],
    )
    def test_post_hierarchy(
        self, limit, depth, children_per_node, expected_num_calls, mock_post_asset_hierarchy, set_limit
    ):
        set_limit(limit)
        assets = generate_asset_tree(root_ref_id="0", depth=depth, children_per_node=children_per_node)

        created_assets = ASSETS_API.create(assets)

        assert len(assets) == len(created_assets)
        assert expected_num_calls - 1 <= len(mock_post_asset_hierarchy.calls) <= expected_num_calls + 1
        for asset in created_assets:
            if asset.id == "0id":
                assert asset.parent_id is None
            else:
                assert asset.id[:-3] == asset.parent_id[:-2]

    def test_post_assets_over_limit_only_resolved(self, set_limit, mock_post_asset_hierarchy):
        set_limit(1)
        _AssetPoster([Asset(parent_id=1), Asset(parent_id=2)], ASSETS_API).post()
        assert 2 == len(mock_post_asset_hierarchy.calls)

    @pytest.fixture
    def mock_post_asset_hierarchy_with_failures(self, rsps, set_limit):
        set_limit(1)

        def request_callback(request):
            items = jsgz_load(request.body)["items"]
            response_assets = []
            item = items[0]
            parent_id = None
            if "parentId" in item:
                parent_id = item["parentId"]
            if "parentRefId" in item:
                parent_id = item["parentRefId"] + "id"
            id = item.get("refId", "root_") + "id"
            response_assets.append({"id": id, "parentId": parent_id, "path": [parent_id or "", id]})

            if item["name"] == "400":
                return 400, {}, json.dumps({"error": {"message": "user error", "code": 400}})

            if item["name"] == "500":
                return 500, {}, json.dumps({"error": {"message": "internal server error", "code": 500}})

            return 200, {}, json.dumps({"data": {"items": response_assets}})

        rsps.add_callback(
            rsps.POST, ASSETS_API._base_url + "/assets", callback=request_callback, content_type="application/json"
        )
        yield rsps

    def test_post_with_failures(self, mock_post_asset_hierarchy_with_failures):
        assets = [
            Asset(name="200", ref_id="0"),
            Asset(name="200", ref_id="01", parent_ref_id="0"),
            Asset(name="400", ref_id="02", parent_ref_id="0"),
            Asset(name="200", ref_id="021", parent_ref_id="02"),
            Asset(name="200", ref_id="0211", parent_ref_id="021"),
            Asset(name="500", ref_id="03", parent_ref_id="0"),
            Asset(name="200", ref_id="031", parent_ref_id="03"),
        ]
        with pytest.raises(CogniteAssetPostingError, match="Some assets failed to post") as e:
            ASSETS_API.create(assets)

        assert {a.ref_id for a in e.value.may_have_been_posted} == {"03"}
        assert {a.ref_id for a in e.value.not_posted} == {"02", "021", "0211", "031"}
        assert {a.ref_id for a in e.value.posted} == {"0", "01"}


@pytest.fixture
def mock_assets_empty(rsps):
    url_pattern = re.compile(re.escape(ASSETS_API._base_url) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json={"data": {"items": []}})
    yield rsps


@pytest.mark.dsl
class TestPandasIntegration:
    def test_asset_list_to_pandas(self, mock_assets_response):
        import pandas as pd

        df = ASSETS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_asset_list_to_pandas_empty(self, mock_assets_empty):
        import pandas as pd

        df = ASSETS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_asset_to_pandas(self, mock_assets_response):
        import pandas as pd

        df = ASSETS_API.get(id=1).to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [0] == df.loc["path"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]


class TestAssetHierarchyVisualization:
    def test_normal_tree(self):
        assets = AssetList(
            [Asset(id=1, path=[1]), Asset(id=2, path=[1, 2]), Asset(id=3, path=[1, 3]), Asset(id=4, path=[1, 3, 4])]
        )
        assert """
1
path: [1]
|______ 2
        path: [1, 2]
|______ 3
        path: [1, 3]
        |______ 4
                path: [1, 3, 4]
""" == str(
            assets
        )

    def test_multiple_root_nodes(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=2, path=[2]),
                Asset(id=3, path=[1, 3]),
                Asset(id=4, path=[2, 4]),
                Asset(id=5, path=[2, 4, 5]),
            ]
        )
        assert """
1
path: [1]
|______ 3
        path: [1, 3]

********************************************************************************

2
path: [2]
|______ 4
        path: [2, 4]
        |______ 5
                path: [2, 4, 5]
""" == str(
            assets
        )

    def test_parent_nodes_missing(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=2, path=[1, 2]),
                Asset(id=4, path=[1, 2, 3, 4]),
                Asset(id=6, path=[1, 5, 6]),
            ]
        )
        assert """
1
path: [1]
|______ 2
        path: [1, 2]

--------------------------------------------------------------------------------

                |______ 4
                        path: [1, 2, 3, 4]

--------------------------------------------------------------------------------

        |______ 6
                path: [1, 5, 6]
""" == str(
            assets
        )

    def test_expand_dicts(self):
        assets = AssetList([Asset(id=1, path=[1], metadata={"a": "b", "c": "d"})])
        assert """
1
metadata:
 - a: b
 - c: d
path: [1]
""" == str(
            assets
        )

    def test_all_cases_combined(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=3, path=[2, 3], metadata={"k1": "v1", "k2": "v2"}),
                Asset(id=2, path=[2]),
                Asset(id=4, path=[10, 4]),
                Asset(id=99, path=[20, 99]),
                Asset(id=5, path=[20, 10, 5]),
            ]
        )
        assert """
1
path: [1]

********************************************************************************

2
path: [2]
|______ 3
        metadata:
         - k1: v1
         - k2: v2
        path: [2, 3]

********************************************************************************

|______ 4
        path: [10, 4]

********************************************************************************

        |______ 5
                path: [20, 10, 5]

--------------------------------------------------------------------------------

|______ 99
        path: [20, 99]
""" == str(
            assets
        )
