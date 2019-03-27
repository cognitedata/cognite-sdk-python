import re

import pytest

from cognite.client import CogniteClient
from cognite.client.api.assets import Asset, AssetList, AssetUpdate
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
        res = ASSETS_API.list()
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
        for asset in ASSETS_API.iter():
            assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, mock_assets_response):
        for assets in ASSETS_API.iter(chunk_size=1):
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
        res = ASSETS_API.update(AssetUpdate(id=1).description_set("blabla"))
        assert isinstance(res, Asset)
        assert mock_assets_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_assets_response):
        res = ASSETS_API.update([AssetUpdate(id=1).description_set("blabla")])
        assert isinstance(res, AssetList)
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_search(self, mock_assets_response):
        res = ASSETS_API.search()
        assert mock_assets_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)
