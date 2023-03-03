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
from cognite.client.data_classes.shared import GeoLocation, GeoLocationFilter, TimestampRange


class FileMetadata(CogniteResource):
    def __init__(
        self,
        external_id=None,
        name=None,
        source=None,
        mime_type=None,
        metadata=None,
        directory=None,
        asset_ids=None,
        data_set_id=None,
        labels=None,
        geo_location=None,
        source_created_time=None,
        source_modified_time=None,
        security_categories=None,
        id=None,
        uploaded=None,
        uploaded_time=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        if (geo_location is not None) and (not isinstance(geo_location, GeoLocation)):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocation")
        self.external_id = external_id
        self.name = name
        self.directory = directory
        self.source = source
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.data_set_id = data_set_id
        self.labels = Label._load_list(labels)
        self.geo_location = geo_location
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.security_categories = security_categories
        self.id = id
        self.uploaded = uploaded
        self.uploaded_time = uploaded_time
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        instance.labels = Label._load_list(instance.labels)
        if instance.geo_location is not None:
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance


class FileMetadataFilter(CogniteFilter):
    def __init__(
        self,
        name=None,
        mime_type=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        data_set_ids=None,
        labels=None,
        geo_location=None,
        asset_subtree_ids=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        uploaded_time=None,
        source_created_time=None,
        source_modified_time=None,
        external_id_prefix=None,
        directory_prefix=None,
        uploaded=None,
        cognite_client=None,
    ):
        self.name = name
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.data_set_ids = data_set_ids
        self.labels = labels
        self.geo_location = geo_location
        self.asset_subtree_ids = asset_subtree_ids
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.uploaded_time = uploaded_time
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.external_id_prefix = external_id_prefix
        self.directory_prefix = directory_prefix
        self.uploaded = uploaded
        self._cognite_client = cognite_client
        if (labels is not None) and (not isinstance(labels, LabelFilter)):
            raise TypeError("FileMetadataFilter.labels must be of type LabelFilter")
        if (geo_location is not None) and (not isinstance(geo_location, GeoLocationFilter)):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocationFilter")

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
            if instance.uploaded_time is not None:
                instance.uploaded_time = TimestampRange(**instance.uploaded_time)
            if instance.labels is not None:
                instance.labels = LabelFilter._load(instance.labels)
            if instance.geo_location is not None:
                instance.geo_location = GeoLocationFilter._load(**instance.geo_location)
        return instance

    def dump(self, camel_case=False):
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        if isinstance(self.geo_location, GeoLocationFilter):
            result["geoLocation"] = self.geo_location.dump(camel_case)
        return result


class FileMetadataUpdate(CogniteUpdate):
    class _PrimitiveFileMetadataUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _ObjectFileMetadataUpdate(CogniteObjectUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListFileMetadataUpdate(CogniteListUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelFileMetadataUpdate(CogniteLabelUpdate):
        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def external_id(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "externalId")

    @property
    def directory(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "directory")

    @property
    def source(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "source")

    @property
    def mime_type(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "mimeType")

    @property
    def metadata(self):
        return FileMetadataUpdate._ObjectFileMetadataUpdate(self, "metadata")

    @property
    def asset_ids(self):
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "assetIds")

    @property
    def source_created_time(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceCreatedTime")

    @property
    def source_modified_time(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceModifiedTime")

    @property
    def data_set_id(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "dataSetId")

    @property
    def labels(self):
        return FileMetadataUpdate._LabelFileMetadataUpdate(self, "labels")

    @property
    def geo_location(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "geoLocation")

    @property
    def security_categories(self):
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "securityCategories")


class FileAggregate(dict):
    def __init__(self, count=None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class FileMetadataList(CogniteResourceList):
    _RESOURCE = FileMetadata
