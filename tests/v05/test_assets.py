import numpy as np
import pandas as pd
import pytest

from cognite.v05 import assets
from cognite.v05.dto import Asset, AssetResponse, AssetListResponse

ASSET_NAME = 'test_asset'


@pytest.fixture(scope='module')
def get_asset_subtree_response():
    return assets.get_asset_subtree(asset_id=6354653755843357, limit=1)


@pytest.fixture(scope='module')
def get_assets_response():
    return assets.get_assets(limit=1)


def test_get_assets_response_object(get_assets_response):
    assert isinstance(get_assets_response, AssetListResponse)
    assert get_assets_response.next_cursor() is not None
    assert get_assets_response.previous_cursor() is None


def test_get_asset():
    res = assets.get_asset(6354653755843357)
    assert isinstance(res, AssetResponse)
    assert isinstance(res.to_json(), dict)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert isinstance(res.to_ndarray(), np.ndarray)


def test_asset_subtree_object(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response, AssetListResponse)
    assert get_asset_subtree_response.next_cursor() is not None
    assert get_asset_subtree_response.previous_cursor() is None


def test_json(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_json(), list)


def test_pandas(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_pandas(), pd.DataFrame)


def test_ndarray(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_ndarray(), np.ndarray)


def test_post_assets():
    a1 = Asset(name=ASSET_NAME)
    res = assets.post_assets([a1])
    assert isinstance(res, AssetListResponse)
    assert res.to_json()[0]['name'] == ASSET_NAME
    assert res.to_json()[0].get('id') != None


def test_delete_assets():
    asset = assets.get_assets(ASSET_NAME, depth=0)
    id = asset.to_json()[0]['id']
    res = assets.delete_assets([id])
    assert res == {}
    assert len(assets.get_assets(ASSET_NAME, depth=0).to_json()) == 0
