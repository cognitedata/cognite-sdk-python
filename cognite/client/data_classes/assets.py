import threading

from cognite.client import utils
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.labels import Label, LabelFilter
from cognite.client.data_classes.shared import GeoLocation, TimestampRange

if TYPE_CHECKING:
    pass


class AssetAggregate(dict):
    def __init__(self, count=None, **kwargs: Any):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateResultItem(dict):
    def __init__(self, child_count=None, depth=None, path=None, **kwargs: Any):
        self.child_count = child_count
        self.depth = depth
        self.path = path
        self.update(kwargs)

    child_count = CognitePropertyClassUtil.declare_property("childCount")
    depth = CognitePropertyClassUtil.declare_property("depth")
    path = CognitePropertyClassUtil.declare_property("path")


class Asset(CogniteResource):
    def __init__(
        self,
        external_id=None,
        name=None,
        parent_id=None,
        parent_external_id=None,
        description=None,
        data_set_id=None,
        metadata=None,
        source=None,
        labels=None,
        geo_location=None,
        id=None,
        created_time=None,
        last_updated_time=None,
        root_id=None,
        aggregates=None,
        cognite_client=None,
    ):
        if (geo_location is not None) and (not isinstance(geo_location, GeoLocation)):
            raise TypeError("Asset.geo_location should be of type GeoLocation")
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.parent_external_id = parent_external_id
        self.description = description
        self.data_set_id = data_set_id
        self.metadata = metadata
        self.source = source
        self.labels = Label._load_list(labels)
        self.geo_location = geo_location
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root_id = root_id
        self.aggregates = aggregates
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.aggregates is not None:
                instance.aggregates = AggregateResultItem(**instance.aggregates)
        instance.labels = Label._load_list(instance.labels)
        if instance.geo_location is not None:
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance

    def __hash__(self):
        return hash(self.external_id)

    def parent(self):
        if self.parent_id is None:
            raise ValueError("parent_id is None")
        return self._cognite_client.assets.retrieve(id=self.parent_id)

    def children(self):
        return self._cognite_client.assets.list(parent_ids=[self.id], limit=None)

    def subtree(self, depth=None):
        return self._cognite_client.assets.retrieve_subtree(id=self.id, depth=depth)

    def time_series(self, **kwargs: Any):
        return self._cognite_client.time_series.list(asset_ids=[self.id], **kwargs)

    def sequences(self, **kwargs: Any):
        return self._cognite_client.sequences.list(asset_ids=[self.id], **kwargs)

    def events(self, **kwargs: Any):
        return self._cognite_client.events.list(asset_ids=[self.id], **kwargs)

    def files(self, **kwargs: Any):
        return self._cognite_client.files.list(asset_ids=[self.id], **kwargs)

    def dump(self, camel_case=False):
        result = super().dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        return result

    def to_pandas(self, expand=("metadata", "aggregates"), ignore=None, camel_case=False):
        return super().to_pandas(expand=expand, ignore=ignore, camel_case=camel_case)


class AssetUpdate(CogniteUpdate):
    class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListAssetUpdate(CogniteListUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelAssetUpdate(CogniteLabelUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
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

    @property
    def geo_location(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "geoLocation")


class AssetList(CogniteResourceList):
    _RESOURCE = Asset

    def __init__(self, resources, cognite_client=None):
        super().__init__(resources, cognite_client)
        self._retrieve_chunk_size = 100

    def time_series(self):
        from cognite.client.data_classes import TimeSeriesList

        return self._retrieve_related_resources(TimeSeriesList, self._cognite_client.time_series)

    def sequences(self):
        from cognite.client.data_classes import SequenceList

        return self._retrieve_related_resources(SequenceList, self._cognite_client.sequences)

    def events(self):
        from cognite.client.data_classes import EventList

        return self._retrieve_related_resources(EventList, self._cognite_client.events)

    def files(self):
        from cognite.client.data_classes import FileMetadataList

        return self._retrieve_related_resources(FileMetadataList, self._cognite_client.files)

    def _retrieve_related_resources(self, resource_list_class, resource_api):
        seen = set()
        lock = threading.Lock()

        def retrieve_and_deduplicate(asset_ids: List[int]) -> CogniteResourceList:
            res = resource_api.list(asset_ids=asset_ids, limit=(-1))
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
            tasks.append({"asset_ids": ids[i : (i + self._retrieve_chunk_size)]})
        res_list = utils._concurrency.execute_tasks(
            retrieve_and_deduplicate, tasks, resource_api._config.max_workers
        ).results
        resources = resource_list_class([])
        for res in res_list:
            resources.extend(res)
        return resources


class AssetFilter(CogniteFilter):
    def __init__(
        self,
        name=None,
        parent_ids=None,
        parent_external_ids=None,
        asset_subtree_ids=None,
        data_set_ids=None,
        metadata=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        root=None,
        external_id_prefix=None,
        labels=None,
        geo_location=None,
        cognite_client=None,
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.parent_external_ids = parent_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self.labels = labels
        self.geo_location = geo_location
        self._cognite_client = cast("CogniteClient", cognite_client)
        if (labels is not None) and (not isinstance(labels, LabelFilter)):
            raise TypeError("AssetFilter.labels must be of type LabelFilter")

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

    def dump(self, camel_case=False):
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result
