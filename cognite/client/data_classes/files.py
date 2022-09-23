from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Union, cast

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

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class FileMetadata(CogniteResource):
    """No description.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the file.
        source (str): The source of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str): Directory associated with the file. Must be an absolute, unix-style path.
        asset_ids (Sequence[int]): No description.
        data_set_id (int): The dataSet Id for the item.
        labels (Sequence[Label]): A list of the labels associated with this resource item.
        geo_location (GeoLocation): The geographic metadata of the file.
        source_created_time (int): The timestamp for when the file was originally created in the source system.
        source_modified_time (int): The timestamp for when the file was last modified in the source system.
        security_categories (Sequence[int]): The security category IDs required to access this file.
        id (int): A server-generated ID for the object.
        uploaded (bool): Whether or not the actual file is uploaded.  This field is returned only by the API, it has no effect in a post body.
        uploaded_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        directory: str = None,
        asset_ids: Sequence[int] = None,
        data_set_id: int = None,
        labels: Sequence[Label] = None,
        geo_location: GeoLocation = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        security_categories: Sequence[int] = None,
        id: int = None,
        uploaded: bool = None,
        uploaded_time: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client: "CogniteClient" = None,
    ):
        if geo_location is not None and not isinstance(geo_location, GeoLocation):
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
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: "CogniteClient" = None) -> "FileMetadata":
        instance = super(FileMetadata, cls)._load(resource, cognite_client)
        instance.labels = Label._load_list(instance.labels)
        if instance.geo_location is not None:
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance


class FileMetadataFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Name of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (Sequence[int]): Only include files that reference these specific asset IDs.
        asset_external_ids (Sequence[str]): Only include files that reference these specific asset external IDs.
        data_set_ids (Sequence[Dict[str, Any]]): Only include files that belong to these datasets.
        labels (LabelFilter): Return only the files matching the specified label(s).
        geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
        asset_subtree_ids (Sequence[Dict[str, Any]]): Only include files that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        source (str): The source of this event.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        uploaded_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        source_created_time (Dict[str, Any]): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
        source_modified_time (Dict[str, Any]): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        directory_prefix (str): Filter by this (case-sensitive) prefix for the directory provided by the client.
        uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[Dict[str, Any]] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        asset_subtree_ids: Sequence[Dict[str, Any]] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        uploaded_time: Union[Dict[str, Any], TimestampRange] = None,
        source_created_time: Dict[str, Any] = None,
        source_modified_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        directory_prefix: str = None,
        uploaded: bool = None,
        cognite_client: "CogniteClient" = None,
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
        self._cognite_client = cast("CogniteClient", cognite_client)

        if labels is not None and not isinstance(labels, LabelFilter):
            raise TypeError("FileMetadataFilter.labels must be of type LabelFilter")
        if geo_location is not None and not isinstance(geo_location, GeoLocationFilter):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocationFilter")

    @classmethod
    def _load(cls, resource: Union[Dict, str]) -> "FileMetadataFilter":
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

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        result = super(FileMetadataFilter, self).dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        if isinstance(self.geo_location, GeoLocationFilter):
            result["geoLocation"] = self.geo_location.dump(camel_case)
        return result


class FileMetadataUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
    """

    class _PrimitiveFileMetadataUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "FileMetadataUpdate":
            return self._set(value)

    class _ObjectFileMetadataUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "FileMetadataUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: List) -> "FileMetadataUpdate":
            return self._remove(value)

    class _ListFileMetadataUpdate(CogniteListUpdate):
        def set(self, value: List) -> "FileMetadataUpdate":
            return self._set(value)

        def add(self, value: List) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: List) -> "FileMetadataUpdate":
            return self._remove(value)

    class _LabelFileMetadataUpdate(CogniteLabelUpdate):
        def add(self, value: Union[str, List[str]]) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: Union[str, List[str]]) -> "FileMetadataUpdate":
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "externalId")

    @property
    def directory(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "directory")

    @property
    def source(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "source")

    @property
    def mime_type(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "mimeType")

    @property
    def metadata(self) -> _ObjectFileMetadataUpdate:
        return FileMetadataUpdate._ObjectFileMetadataUpdate(self, "metadata")

    @property
    def asset_ids(self) -> _ListFileMetadataUpdate:
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "assetIds")

    @property
    def source_created_time(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceCreatedTime")

    @property
    def source_modified_time(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceModifiedTime")

    @property
    def data_set_id(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "dataSetId")

    @property
    def labels(self) -> _LabelFileMetadataUpdate:
        return FileMetadataUpdate._LabelFileMetadataUpdate(self, "labels")

    @property
    def geo_location(self) -> _PrimitiveFileMetadataUpdate:
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "geoLocation")

    @property
    def security_categories(self) -> _ListFileMetadataUpdate:
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "securityCategories")


class FileAggregate(dict):
    """Aggregation results for files

    Args:
        count (int): Number of filtered items included in aggregation
    """

    def __init__(self, count: int = None, **kwargs: Any) -> None:
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class FileMetadataList(CogniteResourceList):
    _RESOURCE = FileMetadata
