import random
import time
from unittest import mock

import pytest

from cognite.client import utils
from cognite.client.data_classes import Asset, AssetFilter, AssetList, AssetUpdate
from cognite.client.exceptions import CogniteNotFoundError
from tests.utils import set_request_limit


@pytest.fixture
def new_asset(cognite_client):
    ts = cognite_client.assets.create(Asset(name="any", description="haha", metadata={"a": "b"}))
    yield ts
    cognite_client.assets.delete(id=ts.id)
    assert cognite_client.assets.retrieve(ts.id) is None


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
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.assets, "_post", wraps=cognite_client.assets._post) as _:
        yield


@pytest.fixture
def new_asset_hierarchy(cognite_client, post_spy):
    random_prefix = "test_{}_".format(utils._auxiliary.random_string(10))
    assets = generate_asset_tree(random_prefix + "0", depth=5, children_per_node=5)

    with set_request_limit(cognite_client.assets, 50):
        cognite_client.assets.create_hierarchy(assets)

    assert 20 < cognite_client.assets._post.call_count < 30

    ext_ids = [a.external_id for a in assets]
    yield random_prefix, ext_ids

    cognite_client.assets.delete(external_id=random_prefix + "0", recursive=True)


@pytest.fixture
def root_test_asset(cognite_client):
    for _ in range(5):  # remove after race condition CDF-10303 is fixed
        for asset in cognite_client.assets(root=True):
            if asset.name.startswith("test__"):
                return asset
        time.sleep(1)
    assert False  # should never be reached


@pytest.fixture
def new_root_asset(cognite_client):
    external_id = "my_root_{}".format(utils._auxiliary.random_string(10))
    root = Asset(external_id=external_id, name="my_root")
    root = cognite_client.assets.create(root)
    yield root
    cognite_client.assets.delete(external_id=external_id, recursive=True)
    assert cognite_client.assets.retrieve(external_id=external_id) is None


class TestAssetsAPI:
    def test_get(self, cognite_client):
        res = cognite_client.assets.list(limit=1)
        assert res[0] == cognite_client.assets.retrieve(res[0].id)

    def test_retrieve_unknown(self, cognite_client):
        res = cognite_client.assets.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            cognite_client.assets.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = cognite_client.assets.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.assets, 10):
            res = cognite_client.assets.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.assets._post.call_count

    def test_partitioned_list(self, cognite_client, post_spy):
        # stop race conditions by cutting off max created time
        maxtime = int(time.time() - 3600) * 1000
        res_flat = cognite_client.assets.list(limit=None, created_time={"max": maxtime})
        res_part = cognite_client.assets.list(partitions=8, limit=None, created_time={"max": maxtime})
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_compare_partitioned_gen_and_list(self, cognite_client, post_spy):
        # stop race conditions by cutting off max created time
        maxtime = int(time.time() - 3600) * 1000
        res_generator = cognite_client.assets(partitions=8, limit=None, created_time={"max": maxtime})
        res_list = cognite_client.assets.list(partitions=8, limit=None, created_time={"max": maxtime})
        assert {a.id for a in res_generator} == {a.id for a in res_list}

    def test_list_with_aggregated_properties_param(self, cognite_client, post_spy):
        res = cognite_client.assets.list(limit=10, aggregated_properties=["child_count"])
        for asset in res:
            assert {"childCount"} == asset.aggregates.keys()
            assert isinstance(asset.aggregates["childCount"], int)

    def test_aggregate(self, cognite_client, new_asset):
        res = cognite_client.assets.aggregate(filter=AssetFilter(name="test__asset_0"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.assets.search(name="test__asset_0", filter=AssetFilter(name="test__asset_0"))
        assert len(res) > 0

    def test_search_query(self, cognite_client):
        res = cognite_client.assets.search(query="test asset 0", limit=5)
        assert len(res) > 0

    def test_update(self, cognite_client, new_asset):
        assert new_asset.metadata == {"a": "b"}
        assert new_asset.description == "haha"
        update_asset = AssetUpdate(new_asset.id).name.set("newname").metadata.set(None).description.set(None)
        res = cognite_client.assets.update(update_asset)
        assert "newname" == res.name
        assert res.metadata == {}
        assert res.description is None

    def test_delete_with_nonexisting(self, cognite_client):
        a = cognite_client.assets.create(Asset(name="any"))
        cognite_client.assets.delete(id=a.id, external_id="this asset does not exist", ignore_unknown_ids=True)
        assert cognite_client.assets.retrieve(id=a.id) is None

    def test_post_asset_hierarchy(self, cognite_client, new_asset_hierarchy):
        prefix, ext_ids = new_asset_hierarchy
        posted_assets = cognite_client.assets.retrieve_multiple(external_ids=ext_ids)
        external_id_to_id = {a.external_id: a.id for a in posted_assets}

        for asset in posted_assets:
            if asset.external_id == prefix + "0":
                assert asset.parent_id is None
            else:
                assert asset.parent_id == external_id_to_id[asset.external_id[:-1]]

    def test_get_subtree(self, cognite_client, root_test_asset):
        assert isinstance(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)), AssetList)
        assert 0 == len(cognite_client.assets.retrieve_subtree(external_id="non_existing_asset"))
        assert 0 == len(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)))
        assert 781 == len(cognite_client.assets.retrieve_subtree(root_test_asset.id))
        subtree = cognite_client.assets.retrieve_subtree(root_test_asset.id, depth=1)
        assert 6 == len(subtree)
        assert all(subtree.get(id=a.id) is not None for a in subtree)  # test if .get works on result

    def test_create_asset_hierarchy_parent_external_id_not_in_request(self, cognite_client, new_root_asset):
        root = new_root_asset
        children = generate_asset_tree(
            root_external_id=root.external_id, depth=5, children_per_node=10, current_depth=2
        )

        cognite_client.assets.create_hierarchy(children)

        external_ids = [asset.external_id for asset in children] + [root.external_id]
        posted_assets = cognite_client.assets.retrieve_multiple(external_ids=external_ids)
        external_id_to_id = {a.external_id: a.id for a in posted_assets}
        for asset in posted_assets:
            if asset.external_id == root.external_id:
                assert asset.parent_id is None
            else:
                assert asset.parent_id == external_id_to_id[asset.external_id[:-1]]
