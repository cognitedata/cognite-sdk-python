import random
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetHierarchy,
    AssetList,
    AssetUpdate,
    GeoLocation,
    GeoLocationFilter,
    Geometry,
    GeometryFilter,
)
from cognite.client.exceptions import CogniteAssetHierarchyError, CogniteNotFoundError
from cognite.client.utils._auxiliary import random_string
from tests.utils import set_request_limit


@pytest.fixture
def new_asset(cognite_client):
    ts = cognite_client.assets.create(Asset(name="any", description="haha", metadata={"a": "b"}))
    yield ts
    cognite_client.assets.delete(id=ts.id)
    assert cognite_client.assets.retrieve(ts.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with patch.object(cognite_client.assets, "_post", wraps=cognite_client.assets._post) as _:
        yield


@pytest.fixture(scope="module")
def root_test_asset(cognite_client):
    return cognite_client.assets.retrieve(external_id="test__asset_0")


@pytest.fixture(scope="module")
def root_test_asset_subtree(root_test_asset):
    return root_test_asset.subtree(depth=3)  # Don't need all for testing, just some children


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

    def test_aggregate_metadata_keys(self, cognite_client, new_asset):
        res = cognite_client.assets.aggregate_metadata_keys()
        assert len(res) > 1
        assert set(res[0]) == {"count", "value", "values"}
        assert isinstance(res[0].value, str)
        assert res[0].count > 1

    def test_aggregate_metadata_values(self, cognite_client, new_asset):
        res = cognite_client.assets.aggregate_metadata_values(keys=["a"])
        assert len(res) > 0
        assert set(res[0]) == {"count", "value", "values"}
        assert isinstance(res[0].value, str)
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

    def test_get_subtree(self, cognite_client, root_test_asset):
        assert isinstance(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)), AssetList)
        assert 0 == len(cognite_client.assets.retrieve_subtree(external_id="non_existing_asset"))
        assert 0 == len(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)))
        assert 790 == len(cognite_client.assets.retrieve_subtree(root_test_asset.id))
        subtree = cognite_client.assets.retrieve_subtree(root_test_asset.id, depth=1)
        assert 6 == len(subtree)
        assert all(subtree.get(id=a.id) is not None for a in subtree)

    def test_create_with_geo_location(self, cognite_client):
        geo_location = GeoLocation(
            type="Feature",
            geometry=Geometry(type="LineString", coordinates=[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]]),
            properties={},
        )
        try:
            a = cognite_client.assets.create(Asset(name="any", geo_location=geo_location))

            result_asset = cognite_client.assets.retrieve(id=a.id)
            assert result_asset is not None
            assert result_asset.geo_location == geo_location

        finally:
            cognite_client.assets.delete(id=a.id)

    def test_filter_by_geo_location(self, cognite_client):
        geo_location = GeoLocation(
            type="Feature",
            geometry=Geometry(type="LineString", coordinates=[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]]),
            properties={},
        )

        try:
            a = cognite_client.assets.create(Asset(name="any", geo_location=geo_location))
            result_assets = cognite_client.assets.list(
                geo_location=GeoLocationFilter(
                    relation="WITHIN",
                    shape=GeometryFilter(
                        type="Polygon", coordinates=[[[32.0, 83.0], [-86.0, -23.0], [119.0, -25.0], [32.0, 83.0]]]
                    ),
                ),
                limit=1,
            )
            assert len(result_assets) > 0

        finally:
            cognite_client.assets.delete(id=a.id)


def generate_orphan_assets(n_id, n_xid, sample_from):
    # Orphans only: We link all assets to an existing asset (some by ID, others by XID):
    s = random_string(20)
    id_assets = [
        Asset(name="a", external_id=f"child-by-id-{i}-{s}", parent_id=parent.id)
        for i, parent in enumerate(random.sample(sample_from, k=n_id))
    ]
    xid_assets = [
        Asset(name="a", external_id=f"child-by-xid-{i}-{s}", parent_external_id=parent.external_id)
        for i, parent in enumerate(random.sample(sample_from, k=n_xid))
    ]
    # Shuffle for good measure ;)
    random.shuffle(assets := id_assets + xid_assets)
    return assets


def create_asset_tower(n):
    xid = f"test-tower-{{}}-{random_string(15)}"
    props = dict(
        name=random_string(10),
        description=random_string(10),
        metadata={random_string(5): random_string(5)},
        source=random_string(10),
    )
    return [
        Asset(external_id=xid.format(0), **props),
        *(Asset(external_id=xid.format(i), parent_external_id=xid.format(i - 1), **props) for i in range(1, n)),
    ]


@contextmanager
def create_hierarchy_with_cleanup(client, assets, upsert=False, upsert_mode=""):
    # Do not call this auto-cleanup method for asset hierarchies expected to fail, it will
    # just cause you more misery since delete also will fail in 'finally' block :)
    assert len(assets) <= 1000, "cleanup might fail if we can't fit all in 1 request"
    try:
        yield client.assets.create_hierarchy(assets, upsert=upsert, upsert_mode=upsert_mode)
    finally:
        if isinstance(assets, AssetHierarchy):
            assets = assets._assets
        client.assets.delete(external_id=[a.external_id for a in assets])


@pytest.fixture(scope="class")
def set_create_lim(cognite_client):
    with pytest.MonkeyPatch.context() as mp:
        # We set a low limit to hopefully detect bugs in how resources are split (+threading)
        # without unnecessarily overloading the API with many thousand assets/request:
        mp.setattr(cognite_client.assets, "_CREATE_LIMIT", 3)
        mp.setattr(cognite_client.assets._config, "max_workers", 2)
        yield


class TestAssetsAPICreateHierarchy:
    @pytest.mark.parametrize("n_roots", (0, 1, 4))
    def test_variable_number_of_root_assets(self, cognite_client, n_roots, root_test_asset, set_create_lim):
        s = random_string(10)
        assets = []
        for i in range(n_roots):
            assets.append(Asset(name="a", external_id=f"root-{i}-{s}"))
            assets.append(Asset(name="a", external_id=f"child-{i}-{s}", parent_external_id=f"root-{i}-{s}"))
        if not assets:
            assets.append(Asset(name="a", external_id=f"child-1-{s}", parent_external_id=root_test_asset.external_id))

        with create_hierarchy_with_cleanup(cognite_client, assets) as created:
            assert len(assets) == len(created)
            # Make sure `.get` has the exact same mapping keys:
            assert set(AssetList(assets)._external_id_to_item) == set(created._external_id_to_item)

    @pytest.mark.parametrize(
        "n_id, n_xid, pass_hierarchy",
        (
            # we dont bother testing "only xids" as this is done extensively in other tests
            (1, 0, False),
            (1, 0, True),
            (1, 1, False),
            (1, 1, True),
            (4, 2, False),
            (4, 2, True),
        ),
    )
    def test_orphans__parent_linked_using_mixed_ids_xids(
        self, n_id, n_xid, pass_hierarchy, cognite_client, root_test_asset_subtree, set_create_lim
    ):
        assets = generate_orphan_assets(n_id, n_xid, sample_from=root_test_asset_subtree)
        expected = set(AssetList(assets)._external_id_to_item)
        if pass_hierarchy:
            assets = AssetHierarchy(assets, ignore_orphans=True)

        with create_hierarchy_with_cleanup(cognite_client, assets) as created:
            assert len(assets) == len(created)
            # Make sure `.get` has the exact same mapping keys:
            assert expected == set(created._external_id_to_item)

    def test_orphans__blocked_if_passed_as_asset_hierarchy_instance(self, cognite_client, root_test_asset_subtree):
        assets = generate_orphan_assets(2, 2, sample_from=root_test_asset_subtree)
        hierarchy_fails = AssetHierarchy(assets, ignore_orphans=False)
        hierarchy_succeeds = AssetHierarchy(assets, ignore_orphans=True)

        with pytest.raises(CogniteAssetHierarchyError, match=r"^Asset hierarchy is not valid. Issue\(s\): 4 orphans$"):
            cognite_client.assets.create_hierarchy(hierarchy_fails, upsert=False)

        with create_hierarchy_with_cleanup(cognite_client, hierarchy_succeeds) as created:
            assert len(assets) == len(created)

    def test_upsert_mode_with_patch(self, cognite_client):
        assets = create_asset_tower(5)
        created = cognite_client.assets.create_hierarchy(assets, upsert=False)
        assert len(assets) == len(created)

        # We set only a subset of fields to ensure existing fields are left untouched.
        # Given metadata should extend existing. #TODO: Do the same for labels.
        patch_assets = [
            Asset(name="a", description="b", metadata={"meta": "data"}, external_id=a.external_id) for a in assets
        ]
        # Advanced update: Move one asset to new parent:
        moved = patch_assets[-2]
        moved_old_parent = assets[-2].parent_external_id  # Note: not part of patch_assets[-2]
        moved.parent_external_id = assets[0].external_id

        with create_hierarchy_with_cleanup(
            cognite_client, patch_assets, upsert=True, upsert_mode="patch"
        ) as patch_created:
            assert len(patch_assets) == len(patch_created)
            # Was 'moved' moved?!
            patch_moved = patch_created.get(external_id=moved.external_id)
            assert moved_old_parent != moved.parent_external_id
            assert moved.parent_external_id == patch_moved.parent_external_id

            for a1, a2, a3 in zip(
                sorted(assets, key=hash),
                sorted(patch_assets, key=hash),
                sorted(patch_created, key=hash),
            ):
                # Was patched:
                assert a1.name != a2.name == a3.name
                assert a1.description != a2.description == a3.description
                # Should have been left untouched:
                assert a2.source is None and a1.source == a3.source
                # Should have been added:
                assert a3.metadata == {**a2.metadata, **a1.metadata}

    def test_upsert_mode_with_replace(self, cognite_client):
        assets = create_asset_tower(5)
        created = cognite_client.assets.create_hierarchy(assets, upsert=False)
        assert len(assets) == len(created)

        # We set only a subset of fields to ensure existing fields are removed/nulled.
        # Given metadata should replace existing. #TODO: Do the same for labels.
        patch_assets = [
            Asset(name="a", description="b", metadata={"meta": "data"}, external_id=a.external_id) for a in assets
        ]
        # Advanced update: Move one asset to new parent:
        moved = patch_assets[-2]
        moved_old_parent = assets[-2].parent_external_id  # Note: not part of patch_assets[-2]
        moved.parent_external_id = assets[0].external_id

        with create_hierarchy_with_cleanup(
            cognite_client, patch_assets, upsert=True, upsert_mode="replace"
        ) as patch_created:
            assert len(patch_assets) == len(patch_created)
            # Was 'moved' moved?!
            patch_moved = patch_created.get(external_id=moved.external_id)
            assert moved_old_parent != moved.parent_external_id
            assert moved.parent_external_id == patch_moved.parent_external_id

            for a1, a2, a3 in zip(
                sorted(assets, key=hash),
                sorted(patch_assets, key=hash),
                sorted(patch_created, key=hash),
            ):
                # Was patched:
                assert a1.name != a2.name == a3.name
                assert a1.description != a2.description == a3.description
                # Should have been nulled:
                assert a1.source != a3.source is a2.source is None
                # Should have been replaced:
                assert a3.metadata == a2.metadata != a1.metadata
