import pytest

from cognite.v04 import assets
from cognite.v04.dto import Asset, AssetResponse

ASSET_NAME = 'test_asset'


@pytest.fixture(scope='module')
def get_asset_subtree_response():
    return assets.get_asset_subtree(limit=1)


@pytest.fixture(scope='module')
def get_assets_response():
    return assets.get_assets(limit=1)


def test_get_assets_response_object(get_assets_response):
    from cognite.v04.dto import AssetResponse
    assert isinstance(get_assets_response, AssetResponse)
    assert get_assets_response.next_cursor() is not None
    assert get_assets_response.previous_cursor() is None


def test_asset_subtree_object(get_asset_subtree_response):
    from cognite.v04.dto import AssetResponse
    assert isinstance(get_asset_subtree_response, AssetResponse)
    assert get_asset_subtree_response.next_cursor() is not None
    assert get_asset_subtree_response.previous_cursor() is None


def test_json(get_asset_subtree_response):
    assert isinstance(get_asset_subtree_response.to_json(), list)


def test_pandas(get_asset_subtree_response):
    import pandas as pd
    assert isinstance(get_asset_subtree_response.to_pandas(), pd.DataFrame)


def test_ndarray(get_asset_subtree_response):
    import numpy as np
    assert isinstance(get_asset_subtree_response.to_ndarray(), np.ndarray)


def test_post_assets():
    a1 = Asset(name=ASSET_NAME)
    res = assets.post_assets([a1])
    assert isinstance(res, AssetResponse)
    assert res.to_json()[0]['name'] == ASSET_NAME
    assert res.to_json()[0].get('id') != None


def test_delete_assets():
    asset = assets.get_assets(ASSET_NAME, depth=0)
    id = asset.to_json()[0]['id']
    res = assets.delete_assets([id])
    assert res == {}
    assert len(assets.get_assets(ASSET_NAME, depth=0).to_json()) == 0
