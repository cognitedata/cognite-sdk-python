from typing import *
from typing import Dict, List

from cognite.client.data_classes._base import *


# GenClass: FilesMetadata
class FileMetadata(CogniteResource):
    """No description.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the file.
        source (str): The source of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (List[int]): No description.
        source_created_time (int): The timestamp for when the file was originally created in the source system.
        source_modified_time (int): The timestamp for when the file was last modified in the source system.
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
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        id: int = None,
        uploaded: bool = None,
        uploaded_time: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.source = source
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.id = id
        self.uploaded = uploaded
        self.uploaded_time = uploaded_time
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenClass: FilesSearchFilter.filter
class FileMetadataFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Name of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (List[int]): Only include files that reference these specific asset IDs.
        root_asset_ids (List[Union[Dict[str, Any], Dict[str, Any]]]): Internal or external IDs of the root assets that the related assets should be a descendant of (or match).
        source (str): The source of this event.
        created_time (Dict[str, Any]): Range between two timestamps.
        last_updated_time (Dict[str, Any]): Range between two timestamps.
        uploaded_time (Dict[str, Any]): Range between two timestamps.
        source_created_time (Dict[str, Any]): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
        source_modified_time (Dict[str, Any]): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        root_asset_ids: List[Union[Dict[str, Any], Dict[str, Any]]] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        uploaded_time: Dict[str, Any] = None,
        source_created_time: Dict[str, Any] = None,
        source_modified_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        uploaded: bool = None,
        cognite_client=None,
    ):
        self.name = name
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.root_asset_ids = root_asset_ids
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.uploaded_time = uploaded_time
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.external_id_prefix = external_id_prefix
        self.uploaded = uploaded
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: FileChange
class FileMetadataUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
    """

    @property
    def external_id(self):
        return _PrimitiveFileMetadataUpdate(self, "externalId")

    @property
    def source(self):
        return _PrimitiveFileMetadataUpdate(self, "source")

    @property
    def metadata(self):
        return _ObjectFileMetadataUpdate(self, "metadata")

    @property
    def asset_ids(self):
        return _ListFileMetadataUpdate(self, "assetIds")

    @property
    def source_created_time(self):
        return _PrimitiveFileMetadataUpdate(self, "sourceCreatedTime")

    @property
    def source_modified_time(self):
        return _PrimitiveFileMetadataUpdate(self, "sourceModifiedTime")


class _PrimitiveFileMetadataUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> FileMetadataUpdate:
        return self._set(value)


class _ObjectFileMetadataUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> FileMetadataUpdate:
        return self._set(value)

    def add(self, value: Dict) -> FileMetadataUpdate:
        return self._add(value)

    def remove(self, value: List) -> FileMetadataUpdate:
        return self._remove(value)


class _ListFileMetadataUpdate(CogniteListUpdate):
    def set(self, value: List) -> FileMetadataUpdate:
        return self._set(value)

    def add(self, value: List) -> FileMetadataUpdate:
        return self._add(value)

    def remove(self, value: List) -> FileMetadataUpdate:
        return self._remove(value)

    # GenStop


class FileMetadataList(CogniteResourceList):
    _RESOURCE = FileMetadata
    _UPDATE = FileMetadataUpdate
