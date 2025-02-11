import random
import time
from contextlib import contextmanager
from unittest.mock import patch

import numpy as np
import pytest

from cognite.client import CogniteClient
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
from cognite.client.data_classes import filters as flt
from cognite.client.data_classes.assets import AssetProperty, AssetWrite, AssetWriteList
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError, CogniteAssetHierarchyError, CogniteNotFoundError
from cognite.client.utils._text import random_string
from cognite.client.utils._time import timestamp_to_ms
from tests.utils import rng_context, set_max_workers, set_request_limit

TEST_LABEL = "integration test label, dont delete"


@pytest.fixture(scope="module")
def test_label(cognite_client):
    # Labels does not support retrieve:
    label = cognite_client.labels.list(external_id_prefix=TEST_LABEL, limit=None).get(external_id=TEST_LABEL)
    if label is not None:
        return label
    # Recreate if someone has deleted it
    from cognite.client.data_classes import LabelDefinition

    return cognite_client.labels.create(LabelDefinition(external_id=TEST_LABEL, name="integration test label"))


@pytest.fixture
def new_asset(cognite_client, test_label):
    ts = cognite_client.assets.create(Asset(name="any", description="haha", metadata={"a": "b"}, labels=[test_label]))
    yield ts
    cognite_client.assets.delete(id=ts.id)
    assert cognite_client.assets.retrieve(ts.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with patch.object(cognite_client.assets, "_post", wraps=cognite_client.assets._post) as _:
        yield


@pytest.fixture(scope="session")
def twenty_assets(cognite_client: CogniteClient) -> AssetList:
    prefix = "twenty_assets:"
    root = AssetWrite(external_id=f"{prefix}root", name="root")
    assets = AssetWriteList(
        [root]
        + [
            AssetWrite(
                external_id=f"{prefix}asset{i}",
                name=f"asset{i}",
                parent_external_id=root.external_id,
            )
            for i in range(19)
        ]
    )
    return cognite_client.assets.upsert(assets, mode="replace")


@pytest.fixture(scope="module")
def root_test_asset(cognite_client) -> Asset:
    root = AssetWrite(external_id="test__asset_root", name="test__asset_root")
    retrieved = cognite_client.assets.retrieve(external_id=root.external_id)
    if retrieved is not None:
        return retrieved
    hierarchy = generate_asset_tree(root, first_level_size=5, size=1000, depth=8)
    cognite_client.assets.create_hierarchy(hierarchy, upsert=True, upsert_mode="replace")
    return cognite_client.assets.retrieve(external_id=root.external_id)


@rng_context(seed=0)
def generate_asset_tree(root: AssetWrite, first_level_size: int, size: int, depth: int) -> list[AssetWrite]:
    # A power law distribution describes the real shape of an asset hierarchy, i.e., few roots, many leaves.
    count_per_level = np.random.power(0.2, depth)
    count_per_level.sort()
    total = count_per_level.sum()
    count_per_level = (count_per_level / total) * size
    count_per_level = np.ceil(count_per_level).astype(np.int64)
    count_per_level[0] = first_level_size
    count_per_level[-1] = size - count_per_level[:-1].sum() - 1
    last_level = [root]
    hierarchy = [root]
    for level, count in enumerate(count_per_level, 1):
        this_level = []
        for asset_no in range(count):
            parent = random.choice(last_level)
            identifier = f"test__asset_depth_{level}_asset_{asset_no}"
            asset = AssetWrite(
                name=f"Asset {asset_no} depth@{level}", external_id=identifier, parent_external_id=parent.external_id
            )
            this_level.append(asset)
        last_level = this_level
        hierarchy.extend(this_level)
    return hierarchy


@pytest.fixture(scope="module")
def root_test_asset_subtree(cognite_client, root_test_asset):
    yield (tree := root_test_asset.subtree(depth=3))  # Don't need all for testing, just some children
    try:
        # Add a little cleanup to make sure we don't grow this tree over time (as stuff fails)
        to_delete = [
            asset.id
            for asset in tree
            if not asset.name.startswith("test__asset_") and asset.created_time < timestamp_to_ms("6h-ago")
        ]
        if to_delete:
            cognite_client.assets.delete(id=to_delete)
    except Exception:
        pass


@pytest.fixture(scope="session")
def asset_list(cognite_client: CogniteClient) -> AssetList:
    prefix = "integration_test:"
    assets = AssetList(
        [
            Asset(
                external_id=f"{prefix}asset1",
                name="asset1",
            ),
            Asset(
                external_id=f"{prefix}asset2",
                parent_external_id=f"{prefix}asset1",
                name="asset2",
                metadata={
                    "timezone": "Europe/Oslo",
                },
            ),
            Asset(
                external_id=f"{prefix}asset3",
                parent_external_id=f"{prefix}asset1",
                name="asset3",
                metadata={"timezone": "America/New_York"},
            ),
        ]
    )
    return cognite_client.assets.upsert(assets, mode="replace")


@pytest.fixture
def is_integration_test() -> Filter:
    return flt.Prefix("externalId", "integration_test:")


class TestAssetsAPI:
    def test_get(self, cognite_client):
        res = cognite_client.assets.list(limit=1)
        assert res[0].id == cognite_client.assets.retrieve(res[0].id).id

    def test_retrieve_unknown(self, cognite_client):
        res = cognite_client.assets.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            cognite_client.assets.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = cognite_client.assets.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    @pytest.mark.usefixtures("twenty_assets")
    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.assets, 10):
            res = cognite_client.assets.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.assets._post.call_count

    @pytest.mark.usefixtures("twenty_assets")
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
            assert {"childCount"} == asset.aggregates.dump(camel_case=True).keys()
            assert isinstance(asset.aggregates.child_count, int)

    def test_aggregate(self, cognite_client, twenty_assets: AssetList):
        res = cognite_client.assets.aggregate(filter=AssetFilter(name=twenty_assets[0].name))
        assert res[0].count > 0

    def test_search(self, cognite_client, twenty_assets: AssetList):
        name = twenty_assets[0].name
        res = cognite_client.assets.search(name, filter=AssetFilter(name=name))
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

    def test_update_without_assetupdate(self, cognite_client, new_asset, test_label):
        assert new_asset.metadata == {"a": "b"}
        # Labels are subclasses of dict, so we can't compare so easily:
        assert len(new_asset.labels) == 1
        assert new_asset.labels[0].external_id == test_label.external_id

        new_asset.metadata = {}
        new_asset.labels = []
        updated_asset = cognite_client.assets.update(new_asset)
        # Both should be cleared:
        assert updated_asset.metadata == {}
        assert updated_asset.labels is None  # Api does not return empty list when empty :shrug:

    def test_update_without_assetupdate_none_doesnt_replace(self, cognite_client, new_asset, test_label):
        assert new_asset.metadata == {"a": "b"}
        # Labels are subclasses of dict, so we can't compare so easily:
        assert len(new_asset.labels) == 1
        assert new_asset.labels[0].external_id == test_label.external_id

        new_asset.metadata = None
        new_asset.labels = None
        updated_asset = cognite_client.assets.update(new_asset)
        # Both should be left unchanged:
        assert updated_asset.metadata == {"a": "b"}
        assert len(updated_asset.labels) == 1
        assert updated_asset.labels[0].external_id == test_label.external_id

    def test_delete_with_nonexisting(self, cognite_client):
        a = cognite_client.assets.create(Asset(name="any"))
        cognite_client.assets.delete(id=a.id, external_id="this asset does not exist", ignore_unknown_ids=True)
        assert cognite_client.assets.retrieve(id=a.id) is None

    def test_get_subtree(self, cognite_client, root_test_asset: Asset) -> None:
        assert isinstance(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)), AssetList)
        assert 0 == len(cognite_client.assets.retrieve_subtree(external_id="non_existing_asset"))
        assert 0 == len(cognite_client.assets.retrieve_subtree(id=random.randint(1, 10)))
        # 'root_test_asset' (+children) is used in other tests as parents, so we just check '<=':
        assert 781 <= len(cognite_client.assets.retrieve_subtree(root_test_asset.id))
        subtree = cognite_client.assets.retrieve_subtree(root_test_asset.id, depth=1)
        assert 6 == len(subtree)
        assert all(subtree.get(id=a.id) is not None for a in subtree)

    def test_create_with_geo_location(self, cognite_client):
        geo_location = GeoLocation(
            type="Feature",
            geometry=Geometry(type="LineString", coordinates=[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]]),
            properties={},
        )
        a = None
        try:
            a = cognite_client.assets.create(Asset(name="any", geo_location=geo_location))

            result_asset = cognite_client.assets.retrieve(id=a.id)
            assert result_asset is not None
            assert result_asset.geo_location == geo_location
        finally:
            if a is not None:
                cognite_client.assets.delete(id=a.id)

    @pytest.mark.skip(
        "This seems to not be enabled for new CDF projects. Similarly asset_ids "
        "is not valid part of a GeoSpatial feature."
    )
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

    def test_upsert_2_asset_one_preexisting(self, cognite_client: CogniteClient) -> None:
        new_asset = Asset(external_id="test_upsert_2_asset_one_preexisting:new" + random_string(5), name="my new asset")
        preexisting = Asset(
            external_id="test_upsert_2_asset_one_preexisting:preexisting" + random_string(5),
            name="my preexisting asset",
        )
        preexisting_update = Asset.load(preexisting.dump(camel_case=True))
        preexisting_update.name = "my preexisting asset updated"
        try:
            created_existing = cognite_client.assets.create(preexisting)
            assert created_existing.id is not None

            res = cognite_client.assets.upsert([new_asset, preexisting_update], mode="replace")

            assert len(res) == 2
            assert new_asset.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_asset.name == res[0].name
            assert preexisting_update.name == res[1].name
        finally:
            cognite_client.assets.delete(
                external_id=[new_asset.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_filter_on_metadata_key(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        in_europe = flt.Prefix(AssetProperty.metadata_key("timezone"), "Europe")

        result = cognite_client.assets.filter(
            filter=flt.And(is_integration_test, in_europe),
            sort=("external_id", "asc"),
            aggregated_properties=["child_count"],
        )
        assert len(result) == 1, "Expected only one asset to match the filter"
        assert result[0].external_id == "integration_test:asset2"

    def test_filter_without_sort(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        in_europe = flt.Prefix(AssetProperty.metadata_key("timezone"), "Europe")

        result = cognite_client.assets.filter(
            filter=flt.And(is_integration_test, in_europe), aggregated_properties=["child_count"], sort=None
        )
        assert len(result) == 1, "Expected only one asset to match the filter"
        assert result[0].external_id == "integration_test:asset2"

    def test_list_with_advanced_filter(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ):
        adv_filter = flt.And(is_integration_test, flt.Prefix(AssetProperty.metadata_key("timezone"), "Europe"))
        result = cognite_client.assets.list(
            external_id_prefix="integration_test:",
            advanced_filter=adv_filter,
            sort=("external_id", "asc"),
        )
        assert len(result) == 1, "Expected only one asset to match the filter"
        assert result[0].external_id == "integration_test:asset2"

    def test_aggregate_count(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        count = cognite_client.assets.aggregate_count(advanced_filter=is_integration_test)
        assert count >= len(asset_list), "Expected at least the created asset list to exist"

    def test_aggregate_has_parent_id_count(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        count = cognite_client.assets.aggregate_count(AssetProperty.parent_id, advanced_filter=is_integration_test)
        assert count >= sum(1 for a in asset_list if a.parent_id)

    def test_aggregate_timezone_count(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        count = cognite_client.assets.aggregate_cardinality_values(
            AssetProperty.metadata_key("timezone"), advanced_filter=is_integration_test
        )
        assert count >= len({a.metadata["timezone"] for a in asset_list if "timezone" in a.metadata})

    def test_aggregate_metadata_keys_count(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        count = cognite_client.assets.aggregate_cardinality_properties(
            AssetProperty.metadata, advanced_filter=is_integration_test
        )
        assert count >= len({key for a in asset_list for key in a.metadata or []})

    def test_aggregate_unique_timezone(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        result = cognite_client.assets.aggregate_unique_values(
            AssetProperty.metadata_key("timezone"), advanced_filter=is_integration_test
        )
        assert result
        # Casefold is needed because the aggregation is case insensitive
        assert set(result.unique) >= {a.metadata["timezone"].casefold() for a in asset_list if "timezone" in a.metadata}

    def test_aggregate_unique_metadata_keys(
        self, cognite_client: CogniteClient, asset_list: AssetList, is_integration_test: Filter
    ) -> None:
        result = cognite_client.assets.aggregate_unique_properties(
            AssetProperty.metadata, advanced_filter=is_integration_test
        )
        assert result
        assert {tuple(item.value["property"]) for item in result} >= {
            ("metadata", key.casefold()) for a in asset_list for key in a.metadata or []
        }

    def test_create_strips_server_side_set_fields(self, cognite_client: CogniteClient, new_asset: Asset) -> None:
        my_new_asset = new_asset.external_id = "my_new_asset"
        try:
            # New asset is obtained from .create, so it should have server-side set fields which need to be stripped.
            created = cognite_client.assets.create(new_asset)
            assert created.external_id == my_new_asset
            assert created.name == "any"
            assert created.description == "haha"
            assert created.metadata == {"a": "b"}
        finally:
            cognite_client.assets.delete(external_id="my_new_asset", ignore_unknown_ids=True)


def generate_orphan_assets(n_id, n_xid, sample_from):
    # Orphans only: We link all assets to an existing asset (some by ID, others by XID):
    # Note however that orphans linking with parent ID are ignored!
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
    with set_max_workers(cognite_client, 2), pytest.MonkeyPatch.context() as mp:
        # We set a low limit to hopefully detect bugs in how resources are split (+threading)
        # without unnecessarily overloading the API with many thousand assets/request:
        mp.setattr(cognite_client.assets, "_CREATE_LIMIT", 3)
        yield


@pytest.mark.usefixtures("set_create_lim")
class TestAssetsAPICreateHierarchy:
    @pytest.mark.parametrize("n_roots", (0, 1, 4))
    def test_variable_number_of_root_assets(self, cognite_client, n_roots, root_test_asset):
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
        self, n_id, n_xid, pass_hierarchy, cognite_client, root_test_asset_subtree
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

        with pytest.raises(CogniteAssetHierarchyError, match=r"^Asset hierarchy is not valid. Issue\(s\): 2 orphans$"):
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

    def test_upsert_mode_false_doesnt_patch(self, cognite_client):
        # SDK 5.10.1 and earlier versions had a bug that could run upsert even when False.
        assets = create_asset_tower(5)
        created = cognite_client.assets.create_hierarchy(assets, upsert=False)
        assert len(assets) == len(created)

        for a in assets:
            a.name = "a"
            a.metadata = {"meta": "data"}

        with pytest.raises(CogniteAPIError, match="Latest error: Asset id duplicated") as err:
            with create_hierarchy_with_cleanup(cognite_client, assets, upsert=False, upsert_mode="patch"):
                pytest.fail("Expected 409 API error: 'Asset id duplicated'")
        assert err.value.code == 409

    def test_upsert_mode__only_first_batch_is_updated(self, cognite_client):
        # SDK 6.24.0 and earlier versions had a bug when using upsert that could lead to only the first
        # _CREATE_LIMIT number of assets (updated) being returned.
        assets = AssetList(create_asset_tower(10))
        expected_xids = set(assets.as_external_ids())
        created = cognite_client.assets.create_hierarchy(assets, upsert=False)
        assert set(created.as_external_ids()) == expected_xids

        for a in assets:
            a.description = "updated <3"

        with create_hierarchy_with_cleanup(cognite_client, assets, upsert=True, upsert_mode="patch") as updated:
            assert set(updated.as_external_ids()) == expected_xids
            assert all(upd.description == "updated <3" for upd in updated)

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

    @pytest.mark.parametrize("upsert_mode", ("patch", "replace"))
    def test_upsert_and_insert_in_same_request(self, cognite_client, upsert_mode, monkeypatch, post_spy):
        assets = create_asset_tower(4)
        # Create only first 2:
        created = cognite_client.assets.create_hierarchy(assets[:2], upsert=False)
        assert len(created) == 2
        assert 1 == cognite_client.assets._post.call_count
        # We now send a request with both assets that needs to be created and updated:
        monkeypatch.setattr(cognite_client.assets, "_CREATE_LIMIT", 50)  # Monkeypatch inside a monkeypatch, nice
        with create_hierarchy_with_cleanup(
            cognite_client, assets, upsert=True, upsert_mode=upsert_mode
        ) as patch_created:
            assert len(patch_created) == 4
            assert set(AssetList(assets)._external_id_to_item) == set(patch_created._external_id_to_item)
            # 1+3 because 3 additional calls were made:
            # 1) Try create all (fail), 2) create non-duplicated (success), 3) update duplicated (success)
            assert 1 + 3 == cognite_client.assets._post.call_count
            resource_paths = [call[0][0] for call in cognite_client.assets._post.call_args_list]
            assert resource_paths == ["/assets", "/assets", "/assets", "/assets/update"]
