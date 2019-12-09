import time
from unittest import mock

import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Asset, AssetFilter, AssetUpdate
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_asset():
    ts = COGNITE_CLIENT.assets.create(Asset(name="any"))
    yield ts
    COGNITE_CLIENT.assets.delete(id=ts.id)
    assert COGNITE_CLIENT.assets.retrieve(ts.id) is None


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
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.assets, "_post", wraps=COGNITE_CLIENT.assets._post) as _:
        yield


@pytest.fixture
def new_asset_hierarchy(post_spy):
    random_prefix = "test_{}_".format(utils._auxiliary.random_string(10))
    assets = generate_asset_tree(random_prefix + "0", depth=5, children_per_node=5)

    with set_request_limit(COGNITE_CLIENT.assets, 50):
        COGNITE_CLIENT.assets.create_hierarchy(assets)

    assert 20 < COGNITE_CLIENT.assets._post.call_count < 30

    ext_ids = [a.external_id for a in assets]
    yield random_prefix, ext_ids

    COGNITE_CLIENT.assets.delete(external_id=random_prefix + "0", recursive=True)


@pytest.fixture
def root_test_asset():
    for asset in COGNITE_CLIENT.assets(root=True):
        if asset.name.startswith("test__"):
            return asset


@pytest.fixture
def new_root_asset():
    external_id = "my_root_{}".format(utils._auxiliary.random_string(10))
    root = Asset(external_id=external_id, name="my_root")
    root = COGNITE_CLIENT.assets.create(root)
    yield root
    COGNITE_CLIENT.assets.delete(external_id=external_id, recursive=True)
    assert COGNITE_CLIENT.assets.retrieve(external_id=external_id) is None


class TestAssetsAPI:
    def test_get(self):
        res = COGNITE_CLIENT.assets.list(limit=1)
        assert res[0] == COGNITE_CLIENT.assets.retrieve(res[0].id)

    def test_retrieve_unknown(self):
        res = COGNITE_CLIENT.assets.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            COGNITE_CLIENT.assets.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = COGNITE_CLIENT.assets.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.assets, 10):
            res = COGNITE_CLIENT.assets.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.assets._post.call_count

    def test_partitioned_list(self, post_spy):
        # stop race conditions by cutting off max created time
        maxtime = int(time.time() - 3600) * 1000
        res_flat = COGNITE_CLIENT.assets.list(limit=None, created_time={"max": maxtime})
        res_part = COGNITE_CLIENT.assets.list(partitions=8, limit=None, created_time={"max": maxtime})
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_list_with_aggregated_properties_param(self, post_spy):
        res = COGNITE_CLIENT.assets.list(limit=10, aggregated_properties=["child_count"])
        for asset in res:
            assert {"childCount"} == asset.aggregates.keys()
            assert isinstance(asset.aggregates["childCount"], int)

    def test_search(self):
        res = COGNITE_CLIENT.assets.search(name="test__asset_0", filter=AssetFilter(name="test__asset_0"))
        assert len(res) > 0

    def test_search_query(self):
        res = COGNITE_CLIENT.assets.search(query="test asset 0", limit=5)
        assert len(res) > 0

    def test_update(self, new_asset):
        update_asset = AssetUpdate(new_asset.id).name.set("newname")
        res = COGNITE_CLIENT.assets.update(update_asset)
        assert "newname" == res.name

    def test_delete_with_nonexisting(self):
        a = COGNITE_CLIENT.assets.create(Asset(name="any"))
        COGNITE_CLIENT.assets.delete(id=a.id, external_id="this asset does not exist", ignore_unknown_ids=True)
        assert COGNITE_CLIENT.assets.retrieve(id=a.id) is None

    def test_post_asset_hierarchy(self, new_asset_hierarchy):
        prefix, ext_ids = new_asset_hierarchy
        posted_assets = COGNITE_CLIENT.assets.retrieve_multiple(external_ids=ext_ids)
        external_id_to_id = {a.external_id: a.id for a in posted_assets}

        for asset in posted_assets:
            if asset.external_id == prefix + "0":
                assert asset.parent_id is None
            else:
                assert asset.parent_id == external_id_to_id[asset.external_id[:-1]]

    def test_get_subtree(self, root_test_asset):
        assert 781 == len(COGNITE_CLIENT.assets.retrieve_subtree(root_test_asset.id))
        assert 6 == len(COGNITE_CLIENT.assets.retrieve_subtree(root_test_asset.id, depth=1))

    def test_create_asset_hierarchy_parent_external_id_not_in_request(self, new_root_asset):
        root = new_root_asset
        children = generate_asset_tree(
            root_external_id=root.external_id, depth=5, children_per_node=10, current_depth=2
        )

        COGNITE_CLIENT.assets.create_hierarchy(children)

        external_ids = [asset.external_id for asset in children] + [root.external_id]
        posted_assets = COGNITE_CLIENT.assets.retrieve_multiple(external_ids=external_ids)
        external_id_to_id = {a.external_id: a.id for a in posted_assets}
        for asset in posted_assets:
            if asset.external_id == root.external_id:
                assert asset.parent_id is None
            else:
                assert asset.parent_id == external_id_to_id[asset.external_id[:-1]]
