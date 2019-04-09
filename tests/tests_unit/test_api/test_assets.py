import json
import queue
import re
import time

import pytest

from cognite.client import CogniteClient
from cognite.client.api.assets import Asset, AssetList, AssetPoster, AssetPosterWorker, AssetUpdate
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
    rsps.assert_all_requests_are_fired = False

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

        AssetPosterWorker(
            request_queue=q_req, response_queue=q_res, client=ASSETS_API, assets_remaining=lambda: True
        ).start()
        q_req.put([Asset()])
        time.sleep(0.1)
        assert [Asset._load(mock_assets_response.calls[0].response.json()["data"]["items"][0])] == q_res.get()
        assert 1 == len(mock_assets_response.calls)


class TestAssetPoster:
    def test_validate_asset_hierarchy_parent_ref_null_pointer(self):
        assets = [Asset(parent_ref_id="1")]
        with pytest.raises(AssertionError, match="does not point"):
            AssetPoster.validate_asset_hierarchy(assets)

    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1", parent_id=1)]
        with pytest.raises(AssertionError, match="has both"):
            AssetPoster.validate_asset_hierarchy(assets)

    def test_initialize_ref_id_to_remaining_children_map(self):
        assets = [
            Asset(ref_id="0"),
            Asset(parent_ref_id="0", ref_id="1"),
            Asset(parent_ref_id="1"),
            Asset(parent_ref_id="1"),
        ]

        assert {
            "0": [Asset(parent_ref_id="0", ref_id="1")],
            "1": [Asset(parent_ref_id="1"), Asset(parent_ref_id="1")],
        } == AssetPoster.initialize_ref_id_to_remaining_children_map(assets)

    def test_(self):
        pass

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
                response_assets.append({"id": item["refId"] + "id", "parentId": parent_id})
            return 200, {}, json.dumps({"data": {"items": response_assets}})

        rsps.add_callback(
            rsps.POST, ASSETS_API._base_url + "/assets", callback=request_callback, content_type="application/json"
        )
        yield rsps
        ASSETS_API._max_workers = 10

    def test_post_hierarchy__depth_3__children_per_node_10(self, mock_post_asset_hierarchy, set_limit):
        set_limit(100)
        assets = [Asset(ref_id="0")]
        for i in range(10):
            assets.append(Asset(parent_ref_id="0", ref_id="0{}".format(i)))
            for j in range(10):
                assets.append(Asset(parent_ref_id="0{}".format(i), ref_id="0{}{}".format(i, j)))
                for k in range(10):
                    assets.append(Asset(parent_ref_id="0{}{}".format(i, j), ref_id="0{}{}{}".format(i, j, k)))

        created_assets = ASSETS_API.create(assets)
        assert len(assets) == len(created_assets)
        assert 12 == len(mock_post_asset_hierarchy.calls)
        for asset in created_assets:
            if asset.id == "0id":
                assert asset.parent_id is None
            else:
                assert asset.id[:-3] == asset.parent_id[:-2]

    def test_post_hierarchy__depth_101__children_per_node_1(self, mock_post_asset_hierarchy, set_limit):
        set_limit(100)
        assets = [Asset(ref_id="0")]
        for i in range(1, 101):
            assets.append(Asset(ref_id=str(i), parent_ref_id=str(i - 1)))

        created_assets = ASSETS_API.create(assets)
        assert len(assets) == len(created_assets)
        assert 2 == len(mock_post_asset_hierarchy.calls)
        for asset in created_assets:
            if asset.id == "0id":
                assert asset.parent_id is None
            else:
                assert int(asset.id[:-2]) - 1 == int(asset.parent_id[:-2])


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
