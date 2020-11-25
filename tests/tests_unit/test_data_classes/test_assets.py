from unittest import mock
from unittest.mock import call

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList, Event, EventList, FileMetadata, FileMetadataList

c = CogniteClient()


class TestAsset:
    def test_get_events(self):
        c.events.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=c)
        a.events()
        assert c.events.list.call_args == call(asset_ids=[1])
        assert c.events.list.call_count == 1

    def test_get_time_series(self):
        c.time_series.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=c)
        a.time_series()
        assert c.time_series.list.call_args == call(asset_ids=[1])
        assert c.time_series.list.call_count == 1

    def test_get_sequences(self):
        c.sequences.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=c)
        a.sequences()
        assert c.sequences.list.call_args == call(asset_ids=[1])
        assert c.sequences.list.call_count == 1

    def test_get_files(self):
        c.files.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=c)
        a.files()
        assert c.files.list.call_args == call(asset_ids=[1])
        assert c.files.list.call_count == 1

    def test_get_parent(self):
        c.assets.retrieve = mock.MagicMock()
        a1 = Asset(parent_id=1, cognite_client=c)
        a1.parent()
        assert c.assets.retrieve.call_args == call(id=1)
        assert c.assets.retrieve.call_count == 1

    def test_get_children(self):
        c.assets.list = mock.MagicMock()
        a1 = Asset(id=1, cognite_client=c)
        a1.children()
        assert c.assets.list.call_args == call(parent_ids=[1], limit=None)
        assert c.assets.list.call_count == 1

    def test_get_subtree(self):
        c.assets.retrieve_subtree = mock.MagicMock()
        a1 = Asset(id=1, cognite_client=c)
        a1.subtree(depth=1)
        assert c.assets.retrieve_subtree.call_args == call(id=1, depth=1)
        assert c.assets.retrieve_subtree.call_count == 1


class TestAssetList:
    def test_get_events(self):
        c.events.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=c)
        a.events()
        assert c.events.list.call_args == call(asset_ids=[1], limit=-1)
        assert c.events.list.call_count == 1

    def test_get_time_series(self):
        c.time_series.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=c)
        a.time_series()
        assert c.time_series.list.call_args == call(asset_ids=[1], limit=-1)
        assert c.time_series.list.call_count == 1

    def test_get_sequences(self):
        c.sequences.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=c)
        a.sequences()
        assert c.sequences.list.call_args == call(asset_ids=[1], limit=-1)
        assert c.sequences.list.call_count == 1

    def test_get_files(self):
        c.files.list = mock.MagicMock()
        a = AssetList(resources=[Asset(id=1)], cognite_client=c)
        a.files()
        assert c.files.list.call_args == call(asset_ids=[1], limit=-1)
        assert c.files.list.call_count == 1

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
    def test_to_pandas_nullable_int(self):
        import pandas as pd

        for camel_case in [False, True]:
            assert (
                pd.Int64Dtype()
                == AssetList([Asset(parent_id=123), Asset(parent_id=None)]).to_pandas(camel_case=camel_case).dtypes[0]
            )
