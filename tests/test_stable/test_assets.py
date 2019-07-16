import time

import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.stable.assets import Asset, AssetListResponse, AssetResponse
from tests.conftest import generate_random_string

assets = CogniteClient().assets


@pytest.fixture(scope="module")
def get_asset_subtree_response():
    return assets.get_asset_subtree(asset_id=6354653755843357, limit=2)


@pytest.fixture(scope="module")
def get_assets_response():
    return assets.get_assets(limit=1)


def test_get_assets_response_object(get_assets_response):
    assert isinstance(get_assets_response, AssetListResponse)
    assert get_assets_response.next_cursor() is not None
    assert get_assets_response.previous_cursor() is None
    assert len(get_assets_response)
    assert isinstance(get_assets_response[0], AssetResponse)
    assert isinstance(get_assets_response[:1], AssetListResponse)
    assert len(get_assets_response[:1]) == 1


def test_get_assets_with_metadata_args():
    res = assets.get_assets(limit=1, metadata={"something": "something"})
    assert not res.to_json()


def test_get_asset():
    res = assets.get_asset(6354653755843357)
    assert isinstance(res, AssetResponse)
    assert isinstance(res.to_json(), dict)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert res.to_pandas().shape[1] == 1


def test_attributes_not_none():
    asset = assets.get_asset(6354653755843357)
    for key, val in asset.__dict__.items():
        if key is "metadata" or (key is "parent_id" and asset.depth == 0):
            assert val is None
        else:
            assert val is not None, "{} is None".format(key)


def test_asset_subtree_object(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response, AssetListResponse)
    assert get_asset_subtree_response.next_cursor() is not None
    assert get_asset_subtree_response.previous_cursor() is None


def test_json(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_json(), list)


def test_pandas(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_pandas(), pd.DataFrame)


@pytest.fixture(scope="module")
def created_asset():
    random_asset_name = "test_asset" + generate_random_string(10)
    a1 = Asset(name=random_asset_name)
    res = assets.post_assets([a1])
    assert isinstance(res, AssetListResponse)
    assert res.to_json()[0]["name"] == random_asset_name
    assert res.to_json()[0].get("id") != None

    assets_response = assets.get_assets(random_asset_name, depth=0)
    while len(assets_response) == 0:
        assets_response = assets.get_assets(random_asset_name, depth=0)
        time.sleep(0.5)
    return assets_response[0]


def test_update_asset(created_asset):
    new_name = generate_random_string(10)
    res = assets.update_asset(created_asset.id, name=new_name)
    assert new_name == res.name


def test_update_multiple_assets(created_asset):
    new_name = generate_random_string(10)
    res = assets.update_assets([Asset(id=created_asset.id, name=new_name)])
    assert new_name == res[0].name


def test_delete_assets(created_asset):
    res = assets.delete_assets([created_asset.id])
    assert res is None


def test_search_for_assets():
    res = assets.search_for_assets()
    assert len(res.to_json()) > 0


def test_search_for_asset_in_subtree(rsps):
    rsps.add(
        rsps.GET, assets._base_url + "/assets/search", status=200, json={"data": {"items": [{"id": 1, "name": "bla"}]}}
    )
    assets.search_for_assets(asset_subtrees=[1, 2, 3])
    assets.search_for_assets()
    assert "assetSubtrees=%5B1%2C+2%2C+3%5D" in rsps.calls[0].request.url
    assert "assetSubtrees" not in rsps.calls[1].request.url
