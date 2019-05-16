import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetFilter, AssetUpdate
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import _utils
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient(debug=True)


@pytest.fixture
def new_asset():
    ts = COGNITE_CLIENT.assets.create(Asset(name="any"))
    yield ts
    COGNITE_CLIENT.assets.delete(id=ts.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.assets.retrieve(ts.id)
    assert 400 == e.value.code


def generate_asset_tree(root_external_id: str, depth: int, children_per_node: int, current_depth=1):
    assert 1 <= children_per_node <= 10, "children_per_node must be between 1 and 10"
    assets = []
    if current_depth == 1:
        assets = [Asset(external_id=root_external_id, name=root_external_id)]
    if depth > current_depth:
        for i in range(children_per_node):
            external_id = "{}{}".format(root_external_id, i)
            asset = Asset(parent_external_id=root_external_id, external_id=external_id, name=external_id)
            assets.append(asset)
            if depth > current_depth + 1:
                assets.extend(
                    generate_asset_tree(root_external_id + str(i), depth, children_per_node, current_depth + 1)
                )
    return assets


@pytest.fixture
def new_asset_hierarchy(mocker):
    random_prefix = "test_{}_".format(_utils.random_string(10))
    assets = generate_asset_tree(random_prefix + "0", depth=5, children_per_node=5)
    mocker.spy(COGNITE_CLIENT.assets, "_post")

    with set_request_limit(COGNITE_CLIENT.assets, 50):
        COGNITE_CLIENT.assets.create(assets)

    assert 26 == COGNITE_CLIENT.assets._post.call_count

    ext_ids = [a.external_id for a in assets]
    yield random_prefix, ext_ids

    COGNITE_CLIENT.assets.delete(external_id=random_prefix + "0")


class TestAssetsAPI:
    def test_get(self):
        res = COGNITE_CLIENT.assets.list(limit=1)
        assert res[0] == COGNITE_CLIENT.assets.retrieve(res[0].id)

    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.assets, "_post")

        with set_request_limit(COGNITE_CLIENT.assets, 10):
            res = COGNITE_CLIENT.assets.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.assets._post.call_count

    def test_search(self):
        res = COGNITE_CLIENT.assets.search(name="test__asset_0", filter=AssetFilter(name="test__asset_0"))
        assert len(res) > 0

    def test_update(self, new_asset):
        update_asset = AssetUpdate(new_asset.id).name.set("newname")
        res = COGNITE_CLIENT.assets.update(update_asset)
        assert "newname" == res.name

    def test_post_asset_hierarchy(self, new_asset_hierarchy):
        prefix, ext_ids = new_asset_hierarchy
        posted_assets = COGNITE_CLIENT.assets.retrieve(external_id=ext_ids)
        external_id_to_id = {a.external_id: a.id for a in posted_assets}

        for asset in posted_assets:
            if asset.external_id == prefix + "0":
                assert asset.parent_id is None
            else:
                assert asset.parent_id == external_id_to_id[asset.external_id[:-1]]
