from unittest import mock
from unittest.mock import call

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList

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

    def test_get_files(self):
        c.files.list = mock.MagicMock()
        a = Asset(id=1, cognite_client=c)
        a.files()
        assert c.files.list.call_args == call(asset_ids=[1])
        assert c.files.list.call_count == 1
