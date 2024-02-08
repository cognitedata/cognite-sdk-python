import io
import itertools
import random
import string
import textwrap
import time
from contextlib import redirect_stdout
from pathlib import Path
from unittest import TestCase, mock
from unittest.mock import call

import pytest

from cognite.client.data_classes import (
    Asset,
    AssetHierarchy,
    AssetList,
    Event,
    EventList,
    FileMetadata,
    FileMetadataList,
)
from cognite.client.exceptions import CogniteAssetHierarchyError
from tests.utils import rng_context


class TestAsset:
    def test_get_events(self, cognite_client):
        cognite_client.events.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=cognite_client)
        a.events()
        assert cognite_client.events.list.call_args == call(asset_ids=[1])
        assert cognite_client.events.list.call_count == 1

    def test_get_time_series(self, cognite_client):
        cognite_client.time_series.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=cognite_client)
        a.time_series()
        assert cognite_client.time_series.list.call_args == call(asset_ids=[1])
        assert cognite_client.time_series.list.call_count == 1

    def test_get_sequences(self, cognite_client):
        cognite_client.sequences.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=cognite_client)
        a.sequences()
        assert cognite_client.sequences.list.call_args == call(asset_ids=[1])
        assert cognite_client.sequences.list.call_count == 1

    def test_get_files(self, cognite_client):
        cognite_client.files.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=cognite_client)
        a.files()
        assert cognite_client.files.list.call_args == call(asset_ids=[1])
        assert cognite_client.files.list.call_count == 1

    def test_get_parent(self, cognite_client):
        cognite_client.assets.retrieve = mock.MagicMock()
        a1 = Asset(parent_id=1, cognite_client=cognite_client)
        a1.parent()
        assert cognite_client.assets.retrieve.call_args == call(id=1)
        assert cognite_client.assets.retrieve.call_count == 1

    def test_get_children(self, cognite_client):
        cognite_client.assets.list = mock.MagicMock()
        a1 = Asset(id=1, cognite_client=cognite_client)
        a1.children()
        assert cognite_client.assets.list.call_args == call(parent_ids=[1], limit=None)
        assert cognite_client.assets.list.call_count == 1

    def test_get_subtree(self, cognite_client):
        cognite_client.assets.retrieve_subtree = mock.MagicMock()
        a1 = Asset(id=1, cognite_client=cognite_client)
        a1.subtree(depth=1)
        assert cognite_client.assets.retrieve_subtree.call_args == call(id=1, depth=1)
        assert cognite_client.assets.retrieve_subtree.call_count == 1


class TestAssetList:
    def test_get_events(self, cognite_client):
        cognite_client.events.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.events()
        assert cognite_client.events.list.call_args == call(asset_ids=[1], limit=None)
        assert cognite_client.events.list.call_count == 1

    def test_get_time_series(self, cognite_client):
        cognite_client.time_series.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.time_series()
        assert cognite_client.time_series.list.call_args == call(asset_ids=[1], limit=None)
        assert cognite_client.time_series.list.call_count == 1

    def test_get_sequences(self, cognite_client):
        cognite_client.sequences.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.sequences()
        assert cognite_client.sequences.list.call_args == call(asset_ids=[1], limit=None)
        assert cognite_client.sequences.list.call_count == 1

    def test_get_files(self, cognite_client):
        cognite_client.files.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.files()
        assert cognite_client.files.list.call_args == call(asset_ids=[1], limit=None)
        assert cognite_client.files.list.call_count == 1

    @pytest.mark.parametrize(
        "resource_class, resource_list_class, method",
        [(FileMetadata, FileMetadataList, "files"), (Event, EventList, "events")],
    )
    def test_get_related_resources_should_not_return_duplicates(
        self, resource_class, resource_list_class, method, cognite_client
    ):
        r1 = resource_class(id=1)
        r2 = resource_class(id=2)
        r3 = resource_class(id=3)
        resources_a1 = resource_list_class([r1])
        resources_a2 = resource_list_class([r2, r3])
        resources_a3 = resource_list_class([r2, r3])

        mock_method = mock.Mock()
        setattr(cognite_client, method, mock_method)
        mock_method.list.side_effect = [resources_a1, resources_a2, resources_a3]
        mock_method._config = mock.Mock(max_workers=3)

        assets = AssetList([Asset(id=1), Asset(id=2), Asset(id=3)], cognite_client=cognite_client)
        assets._actual_method = assets._retrieve_related_resources

        def override_chunk_size(*a, **kw):
            kw["chunk_size"] = 1
            return assets._actual_method(*a, **kw)

        assets._retrieve_related_resources = override_chunk_size

        resources = getattr(assets, method)()
        expected = [r1, r2, r3]
        TestCase().assertCountEqual(expected, resources)  # Asserts equal, but ignores ordering

    @pytest.mark.dsl
    def test_to_pandas_nullable_int(self, cognite_client):
        import pandas as pd

        for camel_case in [False, True]:
            assert (
                pd.Int64Dtype()
                == AssetList([Asset(parent_id=123), Asset(parent_id=None)]).to_pandas(camel_case=camel_case).dtypes[0]
            )


def basic_issue_assets():
    return [
        Asset(name="a1", external_id="i am groot", parent_external_id=None),
        # Duplicated XIDs:
        Asset(name="a2", external_id="a", parent_external_id="i am groot"),
        Asset(name="a3", external_id="a", parent_external_id="i am groot"),
        # Duplicated AND orphan:
        Asset(name="a4", external_id="a", parent_external_id="i am orphan"),
        # Orphan:
        Asset(name="a5", external_id="b", parent_external_id="i am orphan"),
        # Invalid (missing XIDs):
        Asset(name="a6", external_id=None, parent_external_id="i am groot"),
        # Doubly defined parent asset:
        Asset(name="a7", external_id="c", parent_external_id="i am groot", parent_id=42),
    ]


def basic_issue_output():
    return textwrap.dedent(
        """\
        Invalid assets (no name, external ID, or with ID set):
        ────────────────────┬────────────────────┬────────────────────┬────────────────────
            External ID     │     Parent ID      │ Parent external ID │        Name
        ────────────────────┼────────────────────┼────────────────────┼────────────────────
        None                │None                │i am groot          │a6

        Assets with parent link given by both ID and external ID:
        ────────────────────┬────────────────────┬────────────────────┬────────────────────
            External ID     │     Parent ID      │ Parent external ID │        Name
        ────────────────────┼────────────────────┼────────────────────┼────────────────────
        c                   │42                  │i am groot          │a7

        Orphan assets (parent ext. ID not part of given assets)
        ────────────────────┬────────────────────┬────────────────────┬────────────────────
            External ID     │ Parent external ID │        Name        │    Description
        ────────────────────┼────────────────────┼────────────────────┼────────────────────
        a                   │i am orphan         │a4                  │None
        b                   │i am orphan         │a5                  │None

        Assets with duplicated external ID:
        ────────────────────┬────────────────────┬────────────────────┬────────────────────
            External ID     │ Parent external ID │        Name        │    Description
        ────────────────────┼────────────────────┼────────────────────┼────────────────────
        a                   │i am groot          │a2                  │None
        a                   │i am groot          │a3                  │None
        a                   │i am orphan         │a4                  │None

        Unable to check for cyclical references before above issues are fixed
        """
    )


def cycles_issue_assets():
    mostly_cycles = [  # Add two normal assets
        Asset(name="no-cycle", external_id="", parent_external_id=None),
        Asset(name="no-cycle-too", external_id="not-empty", parent_external_id=""),
    ]
    # Make cycles of various lengths (4, 9, 16, 25, 36, 49, 64):
    for n, s in enumerate("abcdefg", 2):
        n **= 2
        mostly_cycles.extend(
            Asset(name="R2D2", external_id=f"{s}{i}", parent_external_id=f"{s}{j}")
            for i, j in itertools.zip_longest(range(n), range(1, n), fillvalue=0)
        )
        # Add a child asset to a "cycle" asset (i.e. not directly part of the loop):
        mostly_cycles.append(Asset(name="D2R2", external_id=f"{s}-infinity", parent_external_id=f"{s}{1}"))
    with rng_context(round(time.time(), -3)):  # make parallel test-runners agree on 'random'
        random.shuffle(mostly_cycles)
    return mostly_cycles


def cycles_issue_output():
    return textwrap.dedent(
        """\
        Asset hierarchy had cyclical references:
        - 7 cycles
        - 203 assets part of a cycle
        - 7 non-cycle assets connected to a cycle asset
        ───────────────────────────────────────────────────────────────────────────────────
        Cycle 1/7:
        """
    )


class TestAssetHierarchy:
    @pytest.mark.parametrize(
        "asset",
        (
            # Invalid name:
            Asset(name="", external_id="foo"),
            Asset(name=None, external_id="foo"),
            # Invalid external_id (empty str allowed):
            Asset(name="a", external_id=None),
            # Id given:
            Asset(name="a", external_id="", id=123),
        ),
    )
    def test_validate_asset_hierarchy___invalid_assets(self, asset):
        hierarchy = AssetHierarchy([asset]).validate(on_error="ignore")
        assert len(hierarchy.invalid) == 1
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 1 invalid$"):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__orphans_given_ignore_false(self):
        assets = [
            Asset(name="a", parent_external_id="1", external_id="2"),
            Asset(name="a", parent_external_id="2", external_id="3"),
        ]
        hierarchy = AssetHierarchy(assets, ignore_orphans=False).validate(on_error="ignore")
        assert len(hierarchy.orphans) == 1
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 1 orphans$"):
            hierarchy.is_valid(on_error="raise")

    @pytest.mark.parametrize("n", [1, 2, 3, 10])
    def test_validate_asset_hierarchy__orphans_given_ignore_false__all_parent_external_id(self, n):
        assets = [Asset(name=c, external_id=c, parent_external_id="foo") for c in string.ascii_letters[:n]]
        hierarchy = AssetHierarchy(assets, ignore_orphans=False).validate(on_error="ignore")
        assert len(assets) == len(hierarchy.orphans) == n
        with pytest.raises(CogniteAssetHierarchyError, match=rf"Issue\(s\): {n} orphans$"):
            hierarchy.is_valid(on_error="raise")

    @pytest.mark.parametrize("n", [0, 1, 2, 3, 10])
    def test_validate_asset_hierarchy__orphans_given_ignore_false__all_parent_id(self, n):
        assets = [Asset(name=c, external_id=c, parent_id=ord(c)) for c in string.ascii_letters[:n]]
        hierarchy = AssetHierarchy(assets, ignore_orphans=False).validate(on_error="ignore")
        # Parent ID links are never considered orphans (offline validation impossible as ID can't be set):
        assert len(hierarchy.orphans) == 0
        hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__orphans_given_ignore_true(self):
        assets = [
            Asset(name="a", parent_external_id="1", external_id="2"),
            Asset(name="a", parent_external_id="2", external_id="3"),
        ]
        hierarchy = AssetHierarchy(assets, ignore_orphans=True).validate(on_error="ignore")
        assert len(hierarchy.orphans) == 1  # note: still marked as orphans, but no issues are raised:
        assert hierarchy.is_valid(on_error="raise") is True

    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self):
        assets = [
            Asset(name="a", external_id="1"),
            Asset(name="a", parent_external_id="1", parent_id=1, external_id="2"),
        ]
        hierarchy = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(hierarchy.unsure_parents) == 1
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 1 unsure_parents$"):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy_duplicate_ref_ids(self):
        assets = [Asset(name="a", external_id="1"), Asset(name="a", parent_external_id="1", external_id="1")]
        hierarchy = AssetHierarchy(assets).validate(on_error="ignore")
        assert list(hierarchy.duplicates) == ["1"]
        assert sum(len(assets) for assets in hierarchy.duplicates.values()) == 2
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 2 duplicates$"):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy_circular_dependency(self):
        assets = [
            Asset(name="a", external_id="1", parent_external_id="3"),
            Asset(name="a", external_id="2", parent_external_id="1"),
            Asset(name="a", external_id="3", parent_external_id="2"),
        ]
        hierarchy = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(hierarchy.cycles) == 1
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 1 cycles$"):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy_self_dependency(self):
        # Shortest cycle possible is self->self:
        assets = [Asset(name="a", external_id="2", parent_external_id="2")]
        hierarchy = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(hierarchy.cycles) == 1
        with pytest.raises(CogniteAssetHierarchyError, match=r"Issue\(s\): 1 cycles$"):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__everything_is_wrong(self):
        hierarchy = AssetHierarchy(basic_issue_assets()).validate(on_error="ignore")
        assert hierarchy.invalid and hierarchy.orphans and hierarchy.unsure_parents and hierarchy.duplicates
        with pytest.raises(CogniteAssetHierarchyError, match="^Unable to run cycle-check before"):
            hierarchy.cycles
        with pytest.raises(
            CogniteAssetHierarchyError, match=r"Issue\(s\): 3 duplicates, 1 invalid, 1 unsure_parents, 2 orphans$"
        ):
            hierarchy.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__cycles(self):
        hierarchy = AssetHierarchy(cycles_issue_assets()).validate(on_error="ignore")
        assert not hierarchy.is_valid()
        found_cycles = set(map(frozenset, hierarchy.cycles))
        exp_cycles = {frozenset(f"{s}{i}" for i in range(n**2)) for n, s in enumerate("abcdefg", 2)}
        assert found_cycles == exp_cycles

    @pytest.mark.parametrize(
        "assets, exp_output",
        (
            (basic_issue_assets(), basic_issue_output()),
            (cycles_issue_assets(), cycles_issue_output()),
        ),
    )
    def test_validate_asset_hierarchy__report_to_stdout(self, assets, exp_output):
        with redirect_stdout(io.StringIO()) as stdout:
            AssetHierarchy(assets).validate_and_report()
        # Cycle output does not have deterministic ordering due to extensive set usage
        # (correctness tested separately):
        output = stdout.getvalue()
        assert exp_output == output or output.startswith(exp_output)

    @pytest.mark.parametrize(
        "assets, exp_output",
        (
            (basic_issue_assets(), basic_issue_output()),
            (cycles_issue_assets(), cycles_issue_output()),
        ),
    )
    def test_validate_asset_hierarchy__report_to_file(self, tmp_path, assets, exp_output):
        tmp_path /= "out.txt"
        string_path = str(tmp_path)
        with pytest.raises(TypeError, match=r"^Unable to write to `output_file`, a file-like object is required"):
            AssetHierarchy(assets).validate_and_report(output_file=string_path)

        # Try again with Path instead of str:
        AssetHierarchy(assets).validate_and_report(output_file=tmp_path)
        output = tmp_path.read_text(encoding="utf-8")
        assert exp_output == output or output.startswith(exp_output)

    @pytest.mark.parametrize(
        "assets, exp_output",
        (
            (basic_issue_assets(), basic_issue_output()),
            (cycles_issue_assets(), cycles_issue_output()),
        ),
    )
    def test_validate_asset_hierarchy__arbitrart_file_obj(self, tmp_path, assets, exp_output):
        outfile = Path(tmp_path) / "report.txt"
        with outfile.open("w", encoding="utf-8") as file:
            AssetHierarchy(assets).validate_and_report(output_file=file)

        output = outfile.read_text(encoding="utf-8")
        assert exp_output == output or output.startswith(exp_output)

        with io.StringIO() as file_like:
            AssetHierarchy(assets).validate_and_report(output_file=file_like)
            output = file_like.getvalue()
        assert exp_output == output or output.startswith(exp_output)
