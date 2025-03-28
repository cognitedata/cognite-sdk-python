from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, BinaryIO, Literal, TextIO, TypeVar, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.labels import Label, LabelFilter
from cognite.client.data_classes.shared import GeoLocation, GeoLocationFilter, TimestampRange
from cognite.client.exceptions import CogniteFileUploadError
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class FileMetadataCore(WriteableCogniteResource["FileMetadataWrite"], ABC):
    """No description.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        instance_id (NodeId | None): The instance ID for the file. (Only applicable for files created in DMS)
        name (str | None): Name of the file.
        source (str | None): The source of the file.
        mime_type (str | None): File type. E.g., text/plain, application/pdf, ...
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str | None): Directory associated with the file. It must be an absolute, unix-style path.
        asset_ids (Sequence[int] | None): No description.
        data_set_id (int | None): The dataSet ID for the item.
        labels (Sequence[Label] | None): A list of the labels associated with this resource item.
        geo_location (GeoLocation | None): The geographic metadata of the file.
        source_created_time (int | None): The timestamp for when the file was originally created in the source system.
        source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
        security_categories (Sequence[int] | None): The security category IDs required to access this file.
    """

    def __init__(
        self,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        name: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        security_categories: Sequence[int] | None = None,
    ) -> None:
        if geo_location is not None:
            if isinstance(geo_location, dict):
                geo_location = GeoLocation.load(geo_location)
            if not isinstance(geo_location, GeoLocation):
                raise TypeError("FileMetadata.geo_location should be of type GeoLocation")
        self.external_id = external_id
        self.instance_id = instance_id
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

    @classmethod
    def _load(cls: type[T_FileMetadata], resource: dict, cognite_client: CogniteClient | None = None) -> T_FileMetadata:
        instance = super()._load(resource, cognite_client)
        instance.labels = Label._load_list(instance.labels)
        if isinstance(instance.geo_location, dict):
            instance.geo_location = GeoLocation._load(instance.geo_location)
        if isinstance(instance.instance_id, dict):
            instance.instance_id = NodeId.load(instance.instance_id)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        if self.geo_location:
            result["geoLocation" if camel_case else "geo_location"] = self.geo_location.dump(camel_case)
        if self.instance_id is not None:
            result["instanceId" if camel_case else "instance_id"] = self.instance_id.dump(
                camel_case=camel_case, include_instance_type=False
            )
        return result


T_FileMetadata = TypeVar("T_FileMetadata", bound=FileMetadataCore)


class FileMetadata(FileMetadataCore):
    """This represents the metadata for a file. It does not contain the actual file itself.
    This is the reading version of FileMetadata, and it is used when retrieving from CDF.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        instance_id (NodeId | None): The Instance ID for the file. (Only applicable for files created in DMS)
        name (str | None): Name of the file.
        source (str | None): The source of the file.
        mime_type (str | None): File type. E.g., text/plain, application/pdf, ...
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str | None): Directory associated with the file. It must be an absolute, unix-style path.
        asset_ids (Sequence[int] | None): No description.
        data_set_id (int | None): The dataSet ID for the item.
        labels (Sequence[Label] | None): A list of the labels associated with this resource item.
        geo_location (GeoLocation | None): The geographic metadata of the file.
        source_created_time (int | None): The timestamp for when the file was originally created in the source system.
        source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
        security_categories (Sequence[int] | None): The security category IDs required to access this file.
        id (int | None): A server-generated ID for the object.
        uploaded (bool | None): Whether the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        uploaded_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        name: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        security_categories: Sequence[int] | None = None,
        id: int | None = None,
        uploaded: bool | None = None,
        uploaded_time: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            instance_id=instance_id,
            name=name,
            directory=directory,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self.uploaded = uploaded
        self.uploaded_time = uploaded_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> FileMetadataWrite:
        """Returns this FileMetadata in its writing format."""
        if self.name is None:
            raise ValueError("FileMetadata must have a name to be written")

        return FileMetadataWrite(
            external_id=self.external_id,
            instance_id=self.instance_id,
            name=self.name,
            directory=self.directory,
            source=self.source,
            mime_type=self.mime_type,
            metadata=self.metadata,
            asset_ids=self.asset_ids,
            data_set_id=self.data_set_id,
            labels=self.labels,
            geo_location=self.geo_location,
            source_created_time=self.source_created_time,
            source_modified_time=self.source_modified_time,
            security_categories=self.security_categories,
        )


class FileMetadataWrite(FileMetadataCore):
    """This represents the metadata for a file. It does not contain the actual file itself.
    This is the writing version of FileMetadata, and it is used when inserting or updating files.

    Args:
        name (str): Name of the file.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        instance_id (NodeId | None): The Instance ID for the file. (Only applicable for files created in DMS)
        source (str | None): The source of the file.
        mime_type (str | None): File type. E.g., text/plain, application/pdf, ...
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str | None): Directory associated with the file. It must be an absolute, unix-style path.
        asset_ids (Sequence[int] | None): No description.
        data_set_id (int | None): The dataSet ID for the item.
        labels (Sequence[Label] | None): A list of the labels associated with this resource item.
        geo_location (GeoLocation | None): The geographic metadata of the file.
        source_created_time (int | None): The timestamp for when the file was originally created in the source system.
        source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
        security_categories (Sequence[int] | None): The security category IDs required to access this file.
    """

    def __init__(
        self,
        name: str,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        security_categories: Sequence[int] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            instance_id=instance_id,
            directory=directory,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> FileMetadataWrite:
        return cls(
            name=resource["name"],
            external_id=resource.get("externalId"),
            instance_id=(instance_id := resource.get("instanceId")) and NodeId.load(instance_id),
            directory=resource.get("directory"),
            source=resource.get("source"),
            mime_type=resource.get("mimeType"),
            metadata=resource.get("metadata"),
            asset_ids=resource.get("assetIds"),
            data_set_id=resource.get("dataSetId"),
            labels=(labels := resource.get("labels")) and Label._load_list(labels),
            geo_location=(geo_location := resource.get("geoLocation")) and GeoLocation._load(geo_location),
            source_created_time=resource.get("sourceCreatedTime"),
            source_modified_time=resource.get("sourceModifiedTime"),
            security_categories=resource.get("securityCategories"),
        )

    def as_write(self) -> FileMetadataWrite:
        """Returns self."""
        return self


class FileMetadataFilter(CogniteFilter):
    """No description.

    Args:
        name (str | None): Name of the file.
        mime_type (str | None): File type. E.g. text/plain, application/pdf, ..
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (Sequence[int] | None): Only include files that reference these specific asset IDs.
        asset_external_ids (SequenceNotStr[str] | None): Only include files that reference these specific asset external IDs.
        data_set_ids (Sequence[dict[str, Any]] | None): Only include files that belong to these datasets.
        labels (LabelFilter | None): Return only the files matching the specified label(s).
        geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
        asset_subtree_ids (Sequence[dict[str, Any]] | None): Only include files that have a related asset in a subtree rooted at any of these asset IDs or external IDs. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        source (str | None): The source of this event.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        uploaded_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        source_created_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
        source_modified_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        directory_prefix (str | None): Filter by this (case-sensitive) prefix for the directory provided by the client.
        uploaded (bool | None): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
    """

    def __init__(
        self,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        data_set_ids: Sequence[dict[str, Any]] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        asset_subtree_ids: Sequence[dict[str, Any]] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        uploaded_time: dict[str, Any] | TimestampRange | None = None,
        source_created_time: dict[str, Any] | TimestampRange | None = None,
        source_modified_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
    ) -> None:
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

        if labels is not None and not isinstance(labels, LabelFilter):
            raise TypeError("FileMetadataFilter.labels must be of type LabelFilter")
        if geo_location is not None and not isinstance(geo_location, GeoLocationFilter):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocationFilter")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        if isinstance(self.geo_location, GeoLocationFilter):
            result["geoLocation" if camel_case else "geo_location"] = self.geo_location.dump(camel_case)
        keys = (
            ["createdTime", "lastUpdatedTime", "uploadedTime", "sourceCreatedTime", "sourceModifiedTime"]
            if camel_case
            else ["created_time", "last_updated_time", "uploaded_time", "source_created_time", "source_modified_time"]
        )
        for key in keys:
            if key in result and isinstance(result[key], TimestampRange):
                result[key] = result[key].dump(camel_case)
        return result


class FileMetadataUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        instance_id (NodeId | None): The ID of the file.
    """

    def __init__(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> None:
        if instance_id is not None and (id is not None or external_id is not None):
            raise ValueError("Exactly one of 'id', 'external_id' or 'instance_id' must be provided.")

        super().__init__(id=id, external_id=external_id)
        self.instance_id = instance_id

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.instance_id is not None:
            output["instanceId" if camel_case else "instance_id"] = self.instance_id.dump(
                camel_case=camel_case, include_instance_type=False
            )
        return output

    class _PrimitiveFileMetadataUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> FileMetadataUpdate:
            return self._set(value)

    class _ObjectFileMetadataUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> FileMetadataUpdate:
            return self._set(value)

        def add(self, value: dict) -> FileMetadataUpdate:
            return self._add(value)

        def remove(self, value: list) -> FileMetadataUpdate:
            return self._remove(value)

    class _ListFileMetadataUpdate(CogniteListUpdate):
        def set(self, value: list) -> FileMetadataUpdate:
            return self._set(value)

        def add(self, value: list) -> FileMetadataUpdate:
            return self._add(value)

        def remove(self, value: list) -> FileMetadataUpdate:
            return self._remove(value)

    class _LabelFileMetadataUpdate(CogniteLabelUpdate):
        def add(self, value: str | list[str]) -> FileMetadataUpdate:
            return self._add(value)

        def remove(self, value: str | list[str]) -> FileMetadataUpdate:
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

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        if isinstance(item, (FileMetadata, FileMetadataWrite)) and item.instance_id:
            return [
                # If Instance ID is set, the file was created in DMS. Then, it is
                # limited which properties can be updated. (Only the ones that are not in DMS + security categories)
                PropertySpec("external_id"),
                PropertySpec("metadata", is_object=True),
                PropertySpec("asset_ids", is_list=True),
                PropertySpec("data_set_id"),
                PropertySpec("labels", is_list=True),
                PropertySpec("geo_location"),
            ]

        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("directory"),
            PropertySpec("source"),
            PropertySpec("mime_type"),
            PropertySpec("metadata", is_object=True),
            PropertySpec("asset_ids", is_list=True),
            PropertySpec("source_created_time"),
            PropertySpec("source_modified_time"),
            PropertySpec("data_set_id"),
            PropertySpec("security_categories", is_list=True),
            PropertySpec("labels", is_list=True),
            PropertySpec("geo_location"),
        ]


class FileMetadataWriteList(CogniteResourceList[FileMetadataWrite], ExternalIDTransformerMixin):
    _RESOURCE = FileMetadataWrite


class FileMetadataList(WriteableCogniteResourceList[FileMetadataWrite, FileMetadata], IdTransformerMixin):
    _RESOURCE = FileMetadata

    def as_write(self) -> FileMetadataWriteList:
        """Returns this FileMetadataList in its writing format."""
        return FileMetadataWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())


class FileMultipartUploadSession:
    """Result of a call to `multipart_upload_session`

    Args:
        file_metadata (FileMetadata): The created file in CDF.
        upload_urls (list[str]): List of upload URLs for the file upload.
        upload_id (str): ID of the multipart upload, needed to complete the upload.
        cognite_client (CogniteClient): Cognite client to use for completing the upload.
    """

    def __init__(
        self, file_metadata: FileMetadata, upload_urls: list[str], upload_id: str, cognite_client: CogniteClient
    ) -> None:
        self.file_metadata = file_metadata
        self._upload_urls = upload_urls
        self._upload_id = upload_id
        self._uploaded_urls = [False for _ in upload_urls]
        self._in_context = False
        self._cognite_client = cognite_client

    def upload_part(self, part_no: int, content: str | bytes | TextIO | BinaryIO) -> None:
        """Upload part of a file.
        Note that if `content` does not somehow expose its length, this method may not work
        on Azure. See `requests.utils.super_len`.

        Args:
            part_no (int): Which part number this is, must be between 0 and `parts` given to `multipart_upload_session`
            content (str | bytes | TextIO | BinaryIO): The content to upload.
        """
        if part_no < 0 or part_no > len(self._uploaded_urls):
            raise ValueError(f"Index out of range: {part_no}, must be between 0 and {len(self._uploaded_urls)}")
        if self._uploaded_urls[part_no]:
            raise CogniteFileUploadError(message="Attempted to upload an already uploaded part", code=400)
        self._cognite_client.files._upload_multipart_part(self._upload_urls[part_no], content)
        self._uploaded_urls[part_no] = True

    def __enter__(self) -> FileMultipartUploadSession:
        self.in_context = True
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool:
        self.in_context = False
        # If we failed, do not call complete
        if exc_type is not None:
            return False

        if not all(self._uploaded_urls):
            raise CogniteFileUploadError(message="Did not upload all parts of file during multipart upload", code=400)

        self._cognite_client.files._complete_multipart_upload(self)

        return True
