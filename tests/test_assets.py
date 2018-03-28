# TODO: Will run these integration tests when we have a proper setup for doing them.
# import pytest
#
#
# @pytest.fixture(scope='module')
# def asset_subtree():
#     from cognite.assets import get_asset_subtree
#     return get_asset_subtree(limit=1)
#
#
# @pytest.fixture(scope='module')
# def get_assets_response():
#     from cognite.assets import get_assets
#     return get_assets(limit=1)
#
#
# def test_get_assets_response_object(get_assets_response):
#     from cognite.data_objects import AssetSearchObject
#     assert isinstance(get_assets_response, AssetSearchObject)
#     assert get_assets_response.next_cursor() is not None
#     assert get_assets_response.previous_cursor() is None
#
#
# def test_asset_subtree_object(asset_subtree):
#     from cognite.data_objects import AssetSearchObject
#     assert isinstance(asset_subtree, AssetSearchObject)
#     assert asset_subtree.next_cursor() is not None
#     assert asset_subtree.previous_cursor() is None
#
#
# def test_json(asset_subtree):
#     assert isinstance(asset_subtree.to_json(), list)
#
#
# def test_pandas(asset_subtree):
#     import pandas as pd
#     assert isinstance(asset_subtree.to_pandas(), pd.DataFrame)
#
#
# def test_ndarray(asset_subtree):
#     import numpy as np
#     assert isinstance(asset_subtree.to_ndarray(), np.ndarray)
