import json
import re
import threading

import pytest

from cognite.client import CogniteClient
from cognite.client.api.assets import Asset, AssetList, AssetPoster, AssetPosterWorker, AssetQueue, AssetUpdate
from tests.utils import jsgz_load

ASSETS_API = CogniteClient().assets


@pytest.fixture
def mock_assets_response(rsps):
    response_body = {
        "data": {
            "items": [
                {
                    "externalId": "string",
                    "name": "string",
                    "parentId": 1,
                    "description": "string",
                    "metadata": {},
                    "source": "string",
                    "id": 1,
                    "lastUpdatedTime": 0,
                    "path": [0],
                    "depth": 0,
                }
            ]
        }
    }

    url_pattern = re.compile(re.escape(ASSETS_API._base_url) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


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


class TestAssetQueue:
    def test_get_asset(self):
        q = AssetQueue(assets=[Asset()])
        assert Asset() == q.get_asset()
        assert q.empty()


@pytest.mark.usefixtures("mock_assets_response")
class TestAssetPosterWorker:
    @pytest.fixture
    def asset_worker_and_queue(self):
        q = AssetQueue([Asset(ref_id="1")])
        remaining_assets = [Asset()]
        ASSETS_API._LIMIT = 1
        worker = AssetPosterWorker(ASSETS_API, q, remaining_assets, AssetList([]), threading.Lock())
        yield worker, q
        ASSETS_API._LIMIT = 1000

    def test_worker_get_assets_from_queue(self, asset_worker_and_queue):
        worker, q = asset_worker_and_queue
        worker.start()
        q.put(Asset())
        worker.remaining_assets = []
        worker.join()
        assert q.empty()
        assert 2 == len(worker.created_assets)

    def test_fill_empty_buffer(self, asset_worker_and_queue):
        worker, q = asset_worker_and_queue

        worker.fill_buffer_with_assets_from_queue()
        assert [Asset(ref_id="1")] == worker.asset_buffer
        assert worker.buffer_is_full()

        worker.empty_buffer()
        assert [] == worker.asset_buffer
        assert worker.buffer_is_empty()

    def test_all_assets_posted(self, asset_worker_and_queue):
        worker, q = asset_worker_and_queue
        assert not worker.all_assets_posted()
        worker.start()
        worker.remaining_assets = []
        worker.join()
        assert worker.all_assets_posted()

    def test_update_ref_id_map(self, asset_worker_and_queue):
        worker, q = asset_worker_and_queue
        worker.asset_buffer = (Asset(ref_id="1"), Asset(ref_id="2"))
        worker.update_ref_id_map([1, 2])
        assert {"1": 1, "2": 2} == worker.ref_id_map

    def test_fill_queue_with_unblocked_assets(self, asset_worker_and_queue):
        worker, q = asset_worker_and_queue
        worker.remaining_assets = [Asset(parent_ref_id="1"), Asset(parent_ref_id="1")]
        worker.fill_buffer_with_assets_from_queue()
        worker.post_assets_in_buffer()
        worker.fill_queue_with_unblocked_assets()
        assert [Asset(parent_id=1), Asset(parent_id=1)] == [q.get_asset(), q.get_asset()]


class TestAssetPoster:
    def test_get_unblocked_assets(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1"), Asset(parent_id=1), Asset()]
        ap = AssetPoster(assets, ASSETS_API)
        assert [Asset(ref_id="1"), Asset(parent_id=1), Asset()] == ap.get_unblocked_assets()

    def test_initialize_queue(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1"), Asset(parent_id=1), Asset()]
        ap = AssetPoster(assets, ASSETS_API)
        ap.initialize_queue()
        assert [Asset(ref_id="1"), Asset(parent_id=1), Asset()] == [ap.queue.get_asset() for i in range(3)]

    def test_validate_asset_hierarchy_parent_ref_null_pointer(self):
        assets = [Asset(parent_ref_id="1")]
        ap = AssetPoster(assets, ASSETS_API)
        with pytest.raises(AssertionError, match="does not point"):
            ap.validate_asset_hierarchy()

    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self):
        assets = [Asset(ref_id="1"), Asset(parent_ref_id="1", parent_id=1)]
        ap = AssetPoster(assets, ASSETS_API)
        with pytest.raises(AssertionError, match="has both"):
            ap.validate_asset_hierarchy()

    @pytest.fixture
    def mock_post_asset_hierarchy(self, rsps):
        def request_callback(request):
            items = jsgz_load(request.body)["items"]
            response_assets = []
            for item in items:
                response_assets.append({"id": item["refId"] + "id", "parentId": item.get("parentId")})
            return 200, {}, json.dumps({"data": {"items": response_assets}})

        rsps.add_callback(
            rsps.POST, ASSETS_API._base_url + "/assets", callback=request_callback, content_type="application/json"
        )

    def test_post_hierarchy(self, mock_post_asset_hierarchy):
        assets = [Asset(ref_id="0")]
        for i in range(10):
            assets.append(Asset(parent_ref_id="0", ref_id=f"0{i}"))
            for j in range(10):
                assets.append(Asset(parent_ref_id=f"0{i}", ref_id=f"0{i}{j}"))
                for k in range(10):
                    assets.append(Asset(parent_ref_id=f"0{i}{j}", ref_id=f"0{i}{j}{k}"))

        created_assets = ASSETS_API.create(assets)
        assert 1111 == len(created_assets)
        for asset in created_assets:
            if asset.id == "0id":
                assert asset.parent_id is None
            else:
                assert asset.id[:-3] == asset.parent_id[:-2]
