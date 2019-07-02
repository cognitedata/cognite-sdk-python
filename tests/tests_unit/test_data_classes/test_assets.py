from unittest import mock
from unittest.mock import MagicMock, call, patch

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

    @patch("cognite.client.utils._concurrency")
    def test_assetlist_files_should_not_return_duplicates(self, mock_concurrency):
        mock_client = MagicMock()
        assets = AssetList(
            [Asset(id=1, path=[1]), Asset(id=2, path=[1, 2]), Asset(id=3, path=[1, 3])], cognite_client=mock_client
        )
        f1 = FileMetadata(name="file1", id=1)
        f2 = FileMetadata(name="file2", id=2)
        f3 = FileMetadata(name="file3", id=3)
        files_asset1 = FileMetadataList([f1])
        files_asset2 = FileMetadataList([f2, f3])
        files_asset3 = FileMetadataList([f2, f3])
        mock_concurrency.execute_tasks_concurrently.return_value.results = [files_asset1, files_asset2, files_asset3]

        files = assets.files()

        expected = [f1, f2, f3]
        assert expected == files

    @patch("cognite.client.utils._concurrency")
    def test_assetlist_events_should_not_return_duplicates(self, mock_concurrency):
        mock_client = MagicMock()
        assets = AssetList(
            [Asset(id=1, path=[1]), Asset(id=2, path=[1, 2]), Asset(id=3, path=[1, 3])], cognite_client=mock_client
        )
        e1 = Event(description="event1", id=1)
        e2 = Event(description="event2", id=2)
        e3 = Event(description="event3", id=3)
        events_asset1 = EventList([e1])
        events_asset2 = EventList([e2, e3])
        events_asset3 = EventList([e2, e3])
        mock_concurrency.execute_tasks_concurrently.return_value.results = [events_asset1, events_asset2, events_asset3]

        events = assets.events()

        expected = [e1, e2, e3]
        assert expected == events


class TestAssetHierarchyVisualization:
    def test_normal_tree(self):
        assets = AssetList(
            [Asset(id=1, path=[1]), Asset(id=2, path=[1, 2]), Asset(id=3, path=[1, 3]), Asset(id=4, path=[1, 3, 4])]
        )
        assert """
1
path: [1]
|______ 2
        path: [1, 2]
|______ 3
        path: [1, 3]
        |______ 4
                path: [1, 3, 4]
""" == str(
            assets
        )

    def test_multiple_root_nodes(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=2, path=[2]),
                Asset(id=3, path=[1, 3]),
                Asset(id=4, path=[2, 4]),
                Asset(id=5, path=[2, 4, 5]),
            ]
        )
        assert """
1
path: [1]
|______ 3
        path: [1, 3]

********************************************************************************

2
path: [2]
|______ 4
        path: [2, 4]
        |______ 5
                path: [2, 4, 5]
""" == str(
            assets
        )

    def test_parent_nodes_missing(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=2, path=[1, 2]),
                Asset(id=4, path=[1, 2, 3, 4]),
                Asset(id=6, path=[1, 5, 6]),
            ]
        )
        assert """
1
path: [1]
|______ 2
        path: [1, 2]

--------------------------------------------------------------------------------

                |______ 4
                        path: [1, 2, 3, 4]

--------------------------------------------------------------------------------

        |______ 6
                path: [1, 5, 6]
""" == str(
            assets
        )

    def test_expand_dicts(self):
        assets = AssetList([Asset(id=1, path=[1], metadata={"a": "b", "c": "d"})])
        assert """
1
metadata:
 - a: b
 - c: d
path: [1]
""" == str(
            assets
        )

    def test_all_cases_combined(self):
        assets = AssetList(
            [
                Asset(id=1, path=[1]),
                Asset(id=3, path=[2, 3], metadata={"k1": "v1", "k2": "v2"}),
                Asset(id=2, path=[2]),
                Asset(id=4, path=[10, 4]),
                Asset(id=99, path=[20, 99]),
                Asset(id=5, path=[20, 10, 5]),
            ]
        )
        assert """
1
path: [1]

********************************************************************************

2
path: [2]
|______ 3
        metadata:
         - k1: v1
         - k2: v2
        path: [2, 3]

********************************************************************************

|______ 4
        path: [10, 4]

********************************************************************************

        |______ 5
                path: [20, 10, 5]

--------------------------------------------------------------------------------

|______ 99
        path: [20, 99]
""" == str(
            assets
        )
