import io
import itertools
import random
import textwrap
import time
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock
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
        assert cognite_client.events.list.call_args == call(asset_ids=[1], limit=-1)
        assert cognite_client.events.list.call_count == 1

    def test_get_time_series(self, cognite_client):
        cognite_client.time_series.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.time_series()
        assert cognite_client.time_series.list.call_args == call(asset_ids=[1], limit=-1)
        assert cognite_client.time_series.list.call_count == 1

    def test_get_sequences(self, cognite_client):
        cognite_client.sequences.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.sequences()
        assert cognite_client.sequences.list.call_args == call(asset_ids=[1], limit=-1)
        assert cognite_client.sequences.list.call_count == 1

    def test_get_files(self, cognite_client):
        cognite_client.files.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=cognite_client)
        a.files()
        assert cognite_client.files.list.call_args == call(asset_ids=[1], limit=-1)
        assert cognite_client.files.list.call_count == 1

    @pytest.mark.parametrize(
        "resource_class, resource_list_class, method",
        [(FileMetadata, FileMetadataList, "files"), (Event, EventList, "events")],
    )
    def test_get_related_resources_should_not_return_duplicates(self, resource_class, resource_list_class, method):
        r1 = resource_class(id=1)
        r2 = resource_class(id=2)
        r3 = resource_class(id=3)
        resources_a1 = resource_list_class([r1])
        resources_a2 = resource_list_class([r2, r3])
        resources_a3 = resource_list_class([r2, r3])

        mock_cognite_client = mock.MagicMock()
        mock_method = getattr(mock_cognite_client, method)
        mock_method.list.side_effect = [resources_a1, resources_a2, resources_a3]
        mock_method._config = mock.Mock(max_workers=3)

        assets = AssetList([Asset(id=1), Asset(id=2), Asset(id=3)], cognite_client=mock_cognite_client)
        assets._retrieve_chunk_size = 1

        resources = getattr(assets, method)()
        expected = [r1, r2, r3]
        assert expected == resources

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
        "exc_type, asset",
        (
            # Invalid name:
            (AssertionError, Asset(name="", external_id="foo")),
            (CogniteAssetHierarchyError, Asset(name="", external_id="foo")),
            (AssertionError, Asset(name=None, external_id="foo")),
            (CogniteAssetHierarchyError, Asset(name=None, external_id="foo")),
            # Invalid external_id (empty str allowed):
            (AssertionError, Asset(name="a", external_id=None)),
            (CogniteAssetHierarchyError, Asset(name="a", external_id=None)),
            # Id given:
            (AssertionError, Asset(name="a", external_id="", id=123)),
            (CogniteAssetHierarchyError, Asset(name="a", external_id="", id=123)),
        ),
    )
    def test_validate_asset_hierarchy___invalid_assets(self, exc_type, asset):
        ahv = AssetHierarchy([asset]).validate(on_error="ignore")
        assert len(ahv.invalid) == 1
        with pytest.raises(exc_type, match=r"Issue\(s\): 1 invalid$"):
            ahv.is_valid(on_error="raise")

    @pytest.mark.parametrize("exc_type", (AssertionError, CogniteAssetHierarchyError))
    def test_validate_asset_hierarchy__orphans_given_ignore_false(self, exc_type):
        assets = [
            Asset(name="a", parent_external_id="1", external_id="2"),
            Asset(name="a", parent_external_id="2", external_id="3"),
        ]
        ahv = AssetHierarchy(assets, ignore_orphans=False).validate(on_error="ignore")
        assert len(ahv.orphans) == 1
        with pytest.raises(exc_type, match=r"Issue\(s\): 1 orphans$"):
            ahv.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__orphans_given_ignore_true(self):
        assets = [
            Asset(name="a", parent_external_id="1", external_id="2"),
            Asset(name="a", parent_external_id="2", external_id="3"),
        ]
        ahv = AssetHierarchy(assets, ignore_orphans=True).validate(on_error="ignore")
        assert len(ahv.orphans) == 1  # note: still marked as orphans, but no issues are raised:
        assert ahv.is_valid(on_error="raise") is True

    @pytest.mark.parametrize("exc_type", (AssertionError, CogniteAssetHierarchyError))
    def test_validate_asset_hierarchy_asset_has_parent_id_and_parent_ref_id(self, exc_type):
        assets = [
            Asset(name="a", external_id="1"),
            Asset(name="a", parent_external_id="1", parent_id=1, external_id="2"),
        ]
        ahv = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(ahv.unsure_parents) == 1
        with pytest.raises(exc_type, match=r"Issue\(s\): 1 unsure_parents$"):
            ahv.is_valid(on_error="raise")

    @pytest.mark.parametrize("exc_type", (AssertionError, CogniteAssetHierarchyError))
    def test_validate_asset_hierarchy_duplicate_ref_ids(self, exc_type):
        assets = [Asset(name="a", external_id="1"), Asset(name="a", parent_external_id="1", external_id="1")]
        ahv = AssetHierarchy(assets).validate(on_error="ignore")
        assert list(ahv.duplicates) == ["1"]
        assert sum(len(assets) for assets in ahv.duplicates.values()) == 2
        with pytest.raises(exc_type, match=r"Issue\(s\): 2 duplicates$"):
            ahv.is_valid(on_error="raise")

    @pytest.mark.parametrize("exc_type", (AssertionError, CogniteAssetHierarchyError))
    def test_validate_asset_hierarchy_circular_dependency(self, exc_type):
        assets = [
            Asset(name="a", external_id="1", parent_external_id="3"),
            Asset(name="a", external_id="2", parent_external_id="1"),
            Asset(name="a", external_id="3", parent_external_id="2"),
        ]
        ahv = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(ahv.cycles) == 1
        with pytest.raises(exc_type, match=r"Issue\(s\): 1 cycles$"):
            ahv.is_valid(on_error="raise")

    @pytest.mark.parametrize("exc_type", (AssertionError, CogniteAssetHierarchyError))
    def test_validate_asset_hierarchy_self_dependency(self, exc_type):
        # Shortest cycle possible is self->self:
        assets = [Asset(name="a", external_id="2", parent_external_id="2")]
        ahv = AssetHierarchy(assets).validate(on_error="ignore")
        assert len(ahv.cycles) == 1
        with pytest.raises(exc_type, match=r"Issue\(s\): 1 cycles$"):
            ahv.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__everything_is_wrong(self):
        ahv = AssetHierarchy(basic_issue_assets()).validate(on_error="ignore")
        assert ahv.invalid and ahv.orphans and ahv.unsure_parents and ahv.duplicates
        with pytest.raises(CogniteAssetHierarchyError, match="^Unable to run cycle-check before"):
            ahv.cycles
        with pytest.raises(
            CogniteAssetHierarchyError, match=r"Issue\(s\): 3 duplicates, 1 invalid, 1 unsure_parents, 2 orphans$"
        ):
            ahv.is_valid(on_error="raise")

    def test_validate_asset_hierarchy__cycles(self):
        ahv = AssetHierarchy(cycles_issue_assets()).validate(on_error="ignore")
        assert not ahv.is_valid()
        found_cycles = set(map(frozenset, ahv.cycles))
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
        assert exp_output == (output := stdout.getvalue()) or output.startswith(exp_output)

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
        assert exp_output == (output := tmp_path.read_text(encoding="utf-8")) or output.startswith(exp_output)

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
        assert exp_output == (output := outfile.read_text(encoding="utf-8")) or output.startswith(exp_output)

        with io.StringIO() as file_like:
            AssetHierarchy(assets).validate_and_report(output_file=file_like)
            output = file_like.getvalue()
        assert exp_output == output or output.startswith(exp_output)
