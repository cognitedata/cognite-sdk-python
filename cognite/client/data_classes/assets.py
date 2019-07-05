from typing import *

from cognite.client.data_classes._base import *


# GenClass: Asset, DataExternalAssetItem
class Asset(CogniteResource):
    """A representation of a physical asset, for example a factory or a piece of equipment.

    Args:
        external_id (str): The external ID provided by the client. Must be unique within the project.
        name (str): The name of the asset. Often referred to as a tag.
        parent_id (int): A JavaScript-friendly internal ID for the object.
        description (str): The description of the asset.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of the asset.
        id (int): A JavaScript-friendly internal ID for the object.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        root_id (int): A JavaScript-friendly internal ID for the object.
        parent_external_id (str): The external ID provided by the client. Must be unique within the project.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        parent_id: int = None,
        description: str = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        root_id: int = None,
        parent_external_id: str = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.source = source
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root_id = root_id
        self.parent_external_id = parent_external_id
        self._cognite_client = cognite_client

    # GenStop

    def __hash__(self):
        return hash(self.external_id)

    def parent(self) -> "Asset":
        """Returns this assets parent.

        Returns:
            Asset: The parent asset.
        """
        if self.parent_id is None:
            raise ValueError("parent_id is None")
        return self._cognite_client.assets.retrieve(id=self.parent_id)

    def children(self) -> "AssetList":
        """Returns the children of this asset.

        Returns:
            AssetList: The requested assets
        """
        return self._cognite_client.assets.list(parent_ids=[self.id], limit=None)

    def subtree(self, depth: int = None) -> "AssetList":
        """Returns the subtree of this asset up to a specified depth.

        Args:
            depth (int, optional): Retrieve assets up to this depth below the asset.

        Returns:
            AssetList: The requested assets sorted topologically.
        """
        return self._cognite_client.assets.retrieve_subtree(id=self.id, depth=depth)

    def time_series(self, **kwargs) -> "TimeSeriesList":
        """Retrieve all time series related to this asset.

        Returns:
            TimeSeriesList: All time series related to this asset.
        """
        return self._cognite_client.time_series.list(asset_ids=[self.id], **kwargs)

    def events(self, **kwargs) -> "EventList":
        """Retrieve all events related to this asset.

        Returns:
            EventList: All events related to this asset.
        """

        return self._cognite_client.events.list(asset_ids=[self.id], **kwargs)

    def files(self, **kwargs) -> "FileMetadataList":
        """Retrieve all files metadata related to this asset.

        Returns:
            FileMetadataList: Metadata about all files related to this asset.
        """
        return self._cognite_client.files.list(asset_ids=[self.id], **kwargs)


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

    Args:
        id (int): A JavaScript-friendly internal ID for the object.
        external_id (str): The external ID provided by the client. Must be unique within the project.
    """

    @property
    def external_id(self):
        return _PrimitiveAssetUpdate(self, "externalId")

    @property
    def name(self):
        return _PrimitiveAssetUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveAssetUpdate(self, "description")

    @property
    def metadata(self):
        return _ObjectAssetUpdate(self, "metadata")

    @property
    def source(self):
        return _PrimitiveAssetUpdate(self, "source")


class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> AssetUpdate:
        return self._set(value)


class _ObjectAssetUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> AssetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)


class _ListAssetUpdate(CogniteListUpdate):
    def set(self, value: List) -> AssetUpdate:
        return self._set(value)

    def add(self, value: List) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)

    # GenStop


class AssetList(CogniteResourceList):
    _RESOURCE = Asset
    _UPDATE = AssetUpdate

    def _indented_asset_str(self, asset: Asset):
        single_indent = " " * 8
        marked_indent = "|______ "
        indent = len(asset.path) - 1

        s = single_indent * (indent - 1)
        if indent > 0:
            s += marked_indent
        s += str(asset.id) + "\n"
        dumped = utils._time.convert_time_attributes_to_datetime(asset.dump())
        for key, value in sorted(dumped.items()):
            if isinstance(value, dict):
                s += single_indent * indent + "{}:\n".format(key)
                for mkey, mvalue in sorted(value.items()):
                    s += single_indent * indent + " - {}: {}\n".format(mkey, mvalue)
            elif key != "id":
                s += single_indent * indent + key + ": " + str(value) + "\n"

        return s

    def __str__(self):
        try:
            sorted_assets = sorted(self.data, key=lambda x: x.path)
        except:
            return super().__str__()

        if len(sorted_assets) == 0:
            return super().__str__()

        ids = set([asset.id for asset in sorted_assets])

        s = "\n"
        root = sorted_assets[0].path[0]
        for asset in sorted_assets:
            this_root = asset.path[0]
            if this_root != root:
                s += "\n" + "*" * 80 + "\n\n"
                root = this_root
            elif len(asset.path) > 1 and asset.path[-2] not in ids:
                s += "\n" + "-" * 80 + "\n\n"
            s += self._indented_asset_str(asset)
        return s

    def time_series(self) -> "TimeSeriesList":
        """Retrieve all time series related to these assets.

        Returns:
            TimeSeriesList: All time series related to the assets in this AssetList.
        """
        from cognite.client.data_classes import TimeSeriesList

        return self._retrieve_related_resources(TimeSeriesList, self._cognite_client.time_series)

    def events(self) -> "EventList":
        """Retrieve all events related to these assets.

        Returns:
            EventList: All events related to the assets in this AssetList.
        """
        from cognite.client.data_classes import EventList

        return self._retrieve_related_resources(EventList, self._cognite_client.events)

    def files(self) -> "FileMetadataList":
        """Retrieve all files metadata related to these assets.

        Returns:
            FileMetadataList: Metadata about all files related to the assets in this AssetList.
        """
        from cognite.client.data_classes import FileMetadataList

        return self._retrieve_related_resources(FileMetadataList, self._cognite_client.files)

    def _retrieve_related_resources(self, resource_list_class, resource_api):
        ids = [a.id for a in self.data]
        tasks = []
        chunk_size = 100
        for i in range(0, len(ids), chunk_size):
            tasks.append({"asset_ids": ids[i : i + chunk_size], "limit": -1})
        res_list = utils._concurrency.execute_tasks_concurrently(
            resource_api.list, tasks, resource_api._config.max_workers
        ).results
        resources = resource_list_class([])
        for res in res_list:
            resources.extend(res)
        return resources


# GenClass: AssetFilter.filter
class AssetFilter(CogniteFilter):
    """Filter on assets with strict matching.

    Args:
        name (str): The name of the asset. Often referred to as a tag.
        parent_ids (List[int]): No description.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of the asset.
        created_time (Dict[str, Any]): Range between two timestamps.
        last_updated_time (Dict[str, Any]): Range between two timestamps.
        root (bool): Whether the filtered assets are root assets, or not.
        external_id_prefix (str): The external ID provided by the client. Must be unique within the project.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        root: bool = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    # GenStop
