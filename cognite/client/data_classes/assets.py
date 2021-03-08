import threading

from cognite.client.data_classes._base import *
from cognite.client.data_classes.labels import Label, LabelDefinition, LabelFilter
from cognite.client.data_classes.shared import TimestampRange


class AssetAggregate(dict):
    """Aggregation group of assets

    Args:
        count (int): Size of the aggregation group
    """

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateResultItem(dict):
    """Aggregated metrics of the asset

    Args:
        child_count (int): Number of direct descendants for the asset
        depth (int): Asset path depth (number of levels below root node).
        path (List[Dict[str, Any]]): IDs of assets on the path to the asset.
    """

    def __init__(self, child_count: int = None, depth: int = None, path: List[Dict[str, Any]] = None, **kwargs):
        self.child_count = child_count
        self.depth = depth
        self.path = path
        self.update(kwargs)

    child_count = CognitePropertyClassUtil.declare_property("childCount")
    depth = CognitePropertyClassUtil.declare_property("depth")
    path = CognitePropertyClassUtil.declare_property("path")


class Asset(CogniteResource):
    """A representation of a physical asset, for example a factory or a piece of equipment.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the asset.
        parent_id (int): The parent of the node, null if it is the root node.
        parent_external_id (str): The external ID of the parent. The property is omitted if the asset doesn't have a parent or if the parent doesn't have externalId.
        description (str): The description of the asset.
        data_set_id (int): The id of the dataset this asset belongs to.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str): The source of the asset.
        labels (List[Label]): A list of the labels associated with this resource item.
        id (int): A server-generated ID for the object.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        root_id (int): ID of the root asset.
        aggregates (Union[Dict[str, Any], AggregateResultItem]): Aggregated metrics of the asset
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        parent_id: int = None,
        parent_external_id: str = None,
        description: str = None,
        data_set_id: int = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        labels: List[Union[Label, str, LabelDefinition]] = None,
        id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        root_id: int = None,
        aggregates: Union[Dict[str, Any], AggregateResultItem] = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.parent_external_id = parent_external_id
        self.description = description
        self.data_set_id = data_set_id
        self.metadata = metadata
        self.source = source
        self.labels = Label._load_list(labels)
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root_id = root_id
        self.aggregates = aggregates
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Asset, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.aggregates is not None:
                instance.aggregates = AggregateResultItem(**instance.aggregates)
        instance.labels = Label._load_list(instance.labels)
        return instance

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

    def sequences(self, **kwargs) -> "SequenceList":
        """Retrieve all sequences related to this asset.

        Returns:
            SequenceList: All sequences related to this asset.
        """
        return self._cognite_client.sequences.list(asset_ids=[self.id], **kwargs)

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

    def dump(self, camel_case: bool = False):
        result = super(Asset, self).dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        return result

    def to_pandas(
        self, expand: List[str] = ("metadata", "aggregates"), ignore: List[str] = None, camel_case: bool = True
    ):
        """Convert the instance into a pandas DataFrame.

        Args:
            expand (List[str]): List of row keys to expand, only works if the value is a Dict.
            ignore (List[str]): List of row keys to not include when converting to a data frame.
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        return super().to_pandas(expand=expand, ignore=ignore, camel_case=camel_case)


class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "AssetUpdate":
            return self._set(value)

    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "AssetUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "AssetUpdate":
            return self._remove(value)

    class _ListAssetUpdate(CogniteListUpdate):
        def set(self, value: List) -> "AssetUpdate":
            return self._set(value)

        def add(self, value: List) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "AssetUpdate":
            return self._remove(value)

    class _LabelAssetUpdate(CogniteLabelUpdate):
        def add(self, value: Union[str, List[str]]) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: Union[str, List[str]]) -> "AssetUpdate":
            return self._remove(value)

    @property
    def external_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "externalId")

    @property
    def name(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "name")

    @property
    def description(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "description")

    @property
    def data_set_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "dataSetId")

    @property
    def metadata(self):
        return AssetUpdate._ObjectAssetUpdate(self, "metadata")

    @property
    def source(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "source")

    @property
    def parent_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentId")

    @property
    def parent_external_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentExternalId")

    @property
    def labels(self):
        return AssetUpdate._LabelAssetUpdate(self, "labels")


class AssetList(CogniteResourceList):
    _RESOURCE = Asset
    _UPDATE = AssetUpdate

    def __init__(self, resources: List[Any], cognite_client=None):
        super().__init__(resources, cognite_client)
        self._retrieve_chunk_size = 100

    def time_series(self) -> "TimeSeriesList":
        """Retrieve all time series related to these assets.

        Returns:
            TimeSeriesList: All time series related to the assets in this AssetList.
        """
        from cognite.client.data_classes import TimeSeriesList

        return self._retrieve_related_resources(TimeSeriesList, self._cognite_client.time_series)

    def sequences(self) -> "SequenceList":
        """Retrieve all sequences related to these assets.

        Returns:
            SequenceList: All sequences related to the assets in this AssetList.
        """
        from cognite.client.data_classes import SequenceList

        return self._retrieve_related_resources(SequenceList, self._cognite_client.sequences)

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
        seen = set()
        lock = threading.Lock()

        def retrieve_and_deduplicate(asset_ids):
            res = resource_api.list(asset_ids=asset_ids, limit=-1)
            resources = resource_list_class([])
            with lock:
                for resource in res:
                    if resource.id not in seen:
                        resources.append(resource)
                        seen.add(resource.id)
            return resources

        ids = [a.id for a in self.data]
        tasks = []
        for i in range(0, len(ids), self._retrieve_chunk_size):
            tasks.append({"asset_ids": ids[i : i + self._retrieve_chunk_size]})
        res_list = utils._concurrency.execute_tasks_concurrently(
            retrieve_and_deduplicate, tasks, resource_api._config.max_workers
        ).results
        resources = resource_list_class([])
        for res in res_list:
            resources.extend(res)
        return resources


class AssetFilter(CogniteFilter):
    """Filter on assets with strict matching.

    Args:
        name (str): The name of the asset.
        parent_ids (List[int]): Return only the direct descendants of the specified assets.
        parent_external_ids (List[str]): Return only the direct descendants of the specified assets.
        root_ids (List[Dict[str, Any]]): This parameter is deprecated. Use assetSubtreeIds instead. Only include these root assets and their descendants.
        asset_subtree_ids (List[Dict[str, Any]]): Only include assets in subtrees rooted at the specified assets (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (List[Dict[str, Any]]): No description.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str): The source of the asset.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        root (bool): Whether the filtered assets are root assets, or not. Set to True to only list root assets.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        labels (LabelFilter): Return only the resource matching the specified label constraints.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        parent_external_ids: List[str] = None,
        root_ids: List[Dict[str, Any]] = None,
        asset_subtree_ids: List[Dict[str, Any]] = None,
        data_set_ids: List[Dict[str, Any]] = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        root: bool = None,
        external_id_prefix: str = None,
        labels: LabelFilter = None,
        cognite_client=None,
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.parent_external_ids = parent_external_ids
        self.root_ids = root_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self.labels = labels
        self._cognite_client = cognite_client

        if labels is not None and not isinstance(labels, LabelFilter):
            raise TypeError("AssetFilter.labels must be of type LabelFilter")

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(AssetFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

    def dump(self, camel_case: bool = False):
        result = super(AssetFilter, self).dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result
