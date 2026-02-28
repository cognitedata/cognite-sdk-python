"""
===============================================================================
66074a72c742cc8bdcd3dbdfc5dc64a6
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Sequence
from pathlib import Path
from typing import Any, BinaryIO, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataFilter,
    FileMetadataList,
    FileMetadataUpdate,
    FileMetadataWrite,
    FileMultipartUploadSession,
    GeoLocation,
    GeoLocationFilter,
    Label,
    LabelFilter,
    TimestampRange,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncFilesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        source_created_time: dict[str, Any] | TimestampRange | None = None,
        source_modified_time: dict[str, Any] | TimestampRange | None = None,
        uploaded_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[FileMetadata]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        source_created_time: dict[str, Any] | TimestampRange | None = None,
        source_modified_time: dict[str, Any] | TimestampRange | None = None,
        uploaded_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[FileMetadataList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        source_created_time: dict[str, Any] | TimestampRange | None = None,
        source_modified_time: dict[str, Any] | TimestampRange | None = None,
        uploaded_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[FileMetadata] | Iterator[FileMetadataList]:
        """
        Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size (int | None): Number of files to return in each chunk. Defaults to yielding one event a time.
            name (str | None): Name of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf, ..
            metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value
            asset_ids (Sequence[int] | None): Only include files that reference these specific asset IDs.
            asset_external_ids (SequenceNotStr[str] | None): No description.
            asset_subtree_ids (int | Sequence[int] | None): Only include files that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include files that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only files in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only files in the specified data set(s) with this external id / these external ids.
            labels (LabelFilter | None): Return only the files matching the specified label(s).
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            source_created_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            uploaded_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project.
            directory_prefix (str | None): Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded (bool | None): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit (int | None): Maximum number of files to return. Defaults to return all items.

        Yields:
            FileMetadata | FileMetadataList: yields FileMetadata one by one if chunk_size is not specified, else FileMetadataList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.files(
                chunk_size=chunk_size,
                name=name,
                mime_type=mime_type,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                labels=labels,
                geo_location=geo_location,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                uploaded_time=uploaded_time,
                external_id_prefix=external_id_prefix,
                directory_prefix=directory_prefix,
                uploaded=uploaded,
                limit=limit,
            )
        )  # type: ignore [misc]

    def create(
        self, file_metadata: FileMetadata | FileMetadataWrite, overwrite: bool = False
    ) -> tuple[FileMetadata, str]:
        """
        Create file without uploading content.

        Args:
            file_metadata (FileMetadata | FileMetadataWrite): File metadata for the file to create.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            tuple[FileMetadata, str]: Tuple containing the file metadata and upload url of the created file.

        Examples:

            Create a file:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_metadata = FileMetadataWrite(name="MyFile")
                >>> res = client.files.create(file_metadata)
        """
        return run_sync(self.__async_client.files.create(file_metadata=file_metadata, overwrite=overwrite))

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata | None:
        """
        `Retrieve a single file metadata by id. <https://api-docs.cognite.com/20230101/tag/Files/operation/getFileByInternalId>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            FileMetadata | None: Requested file metadata or None if it does not exist.

        Examples:

            Get file metadata by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.retrieve(id=1)

            Get file metadata by external id:

                >>> res = client.files.retrieve(external_id="1")
        """
        return run_sync(self.__async_client.files.retrieve(id=id, external_id=external_id, instance_id=instance_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        """
        `Retrieve multiple file metadatas by id. <https://api-docs.cognite.com/20230101/tag/Files/operation/byIdsFiles>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FileMetadataList: The requested file metadatas.

        Examples:

            Get file metadatas by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.retrieve_multiple(ids=[1, 2, 3])

            Get file_metadatas by external id:

                >>> res = client.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        return run_sync(
            self.__async_client.files.retrieve_multiple(
                ids=ids, external_ids=external_ids, instance_ids=instance_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def aggregate_count(self, filter: FileMetadataFilter | dict[str, Any] | None = None) -> int:
        """
        `Aggregate files <https://api-docs.cognite.com/20230101/tag/Files/operation/aggregateFiles>`_

        Args:
            filter (FileMetadataFilter | dict[str, Any] | None): Filter on file metadata filter with exact match

        Returns:
            int: Count of files matching the filter.

        Examples:

            Get the count of files that have been uploaded:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> aggregate_uploaded = client.files.aggregate_count(filter={"uploaded": True})
        """
        return run_sync(self.__async_client.files.aggregate_count(filter=filter))

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """
        `Delete files <https://api-docs.cognite.com/20230101/tag/Files/operation/deleteFiles>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): str or list of str
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete files by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.delete(id=[1,2,3], external_id="3")
        """
        return run_sync(
            self.__async_client.files.delete(id=id, external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    @overload
    def update(
        self,
        item: FileMetadata | FileMetadataWrite | FileMetadataUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadata: ...

    @overload
    def update(
        self,
        item: Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadataList: ...

    def update(
        self,
        item: FileMetadata
        | FileMetadataWrite
        | FileMetadataUpdate
        | Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadata | FileMetadataList:
        """
        `Update files <https://api-docs.cognite.com/20230101/tag/Files/operation/updateFiles>`_
        Currently, a full replacement of labels on a file is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (FileMetadata | FileMetadataWrite | FileMetadataUpdate | Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate]): file(s) to update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (FilesMetadata or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            FileMetadata | FileMetadataList: The updated files.

        Examples:

            Update file metadata that you have fetched. This will perform a full update of the file metadata:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_metadata = client.files.retrieve(id=1)
                >>> file_metadata.description = "New description"
                >>> res = client.files.update(file_metadata)

            Perform a partial update on file metadata, updating the source and adding a new field to metadata:

                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> my_update = FileMetadataUpdate(id=1).source.set("new source").metadata.add({"key": "value"})
                >>> res = client.files.update(my_update)

            Attach labels to a files:

                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> my_update = FileMetadataUpdate(id=1).labels.add(["PUMP", "VERIFIED"])
                >>> res = client.files.update(my_update)

            Detach a single label from a file:

                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> my_update = FileMetadataUpdate(id=1).labels.remove("PUMP")
                >>> res = client.files.update(my_update)
        """
        return run_sync(self.__async_client.files.update(item=item, mode=mode))

    def search(
        self,
        name: str | None = None,
        filter: FileMetadataFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> FileMetadataList:
        """
        `Search for files. <https://api-docs.cognite.com/20230101/tag/Files/operation/searchFiles>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            filter (FileMetadataFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Max number of results to return.

        Returns:
            FileMetadataList: List of requested files metadata.

        Examples:

            Search for a file:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.search(name="some name")

            Search for an asset with an attached label:

                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG"])
                >>> res = client.assets.search(name="xyz",filter=FileMetadataFilter(labels=my_label_filter))
        """
        return run_sync(self.__async_client.files.search(name=name, filter=filter, limit=limit))

    def upload_content(
        self, path: Path | str, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata:
        """
        `Upload a file content <https://api-docs.cognite.com/20230101/tag/Files/operation/getUploadLink>`_

        Args:
            path (Path | str): Path to the file you wish to upload.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.
        Returns:
            FileMetadata: No description.
        """
        return run_sync(
            self.__async_client.files.upload_content(path=path, external_id=external_id, instance_id=instance_id)
        )

    def upload(
        self,
        path: Path | str,
        external_id: str | None = None,
        name: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        security_categories: Sequence[int] | None = None,
        recursive: bool = False,
        overwrite: bool = False,
    ) -> FileMetadata | FileMetadataList:
        """
        `Upload a file <https://api-docs.cognite.com/20230101/tag/Files/operation/initFileUpload>`_

        Args:
            path (Path | str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            name (str | None): Name of the file.
            source (str | None): The source of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf, ...
            metadata (dict[str, str] | None): Customizable extra data about the file. String key -> String value.
            directory (str | None): The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids (Sequence[int] | None): No description.
            source_created_time (int | None): The timestamp for when the file was originally created in the source system.
            source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
            data_set_id (int | None): ID of the data set.
            labels (Sequence[Label] | None): A list of the labels associated with this resource item.
            geo_location (GeoLocation | None): The geographic metadata of the file.
            security_categories (Sequence[int] | None): Security categories to attach to this file.
            recursive (bool): If path is a directory, upload all contained files recursively.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            FileMetadata | FileMetadataList: The file metadata of the uploaded file(s).

        Examples:

            Upload a file in a given path:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from pathlib import Path
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> my_file = Path("/path/to/file.txt")
                >>> res = client.files.upload(my_file, name="my_file")

            If name is omitted, this method will use the name of the file (file.txt in the example above):

                >>> res = client.files.upload(my_file)

            You can also upload all files in a directory by setting path to the path of a directory
            (filenames will be automatically used for `name`):

                >>> upload_dir = Path("/path/to/my/directory")
                >>> res = client.files.upload(upload_dir)

            You can also upload all files in a directory recursively by passing `recursive=True`:

                >>> res = client.files.upload(upload_dir, recursive=True)

            Upload a file with a label:

                >>> from cognite.client.data_classes import Label
                >>> res = client.files.upload(my_file, name="my_file", labels=[Label(external_id="WELL LOG")])

            Upload a file with a geo_location:

                >>> from cognite.client.data_classes import GeoLocation, Geometry
                >>> geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
                >>> res = client.files.upload(my_file, geo_location=GeoLocation(type="Feature", geometry=geometry))
        """
        return run_sync(
            self.__async_client.files.upload(
                path=path,
                external_id=external_id,
                name=name,
                source=source,
                mime_type=mime_type,
                metadata=metadata,
                directory=directory,
                asset_ids=asset_ids,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                data_set_id=data_set_id,
                labels=labels,
                geo_location=geo_location,
                security_categories=security_categories,
                recursive=recursive,
                overwrite=overwrite,
            )
        )

    def upload_content_bytes(
        self, content: str | bytes | BinaryIO, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata:
        """
        Upload bytes or string (UTF-8 assumed).

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_content_session`.

        Args:
            content (str | bytes | BinaryIO): The content to upload.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.

        Returns:
            FileMetadata: No description.

        Examples:

            Finish a file creation by uploading the content using external_id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.upload_content_bytes(
                ...     b"some content", external_id="my_file_xid")

            ...or by using instance_id:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> res = client.files.upload_content_bytes(
                ...     b"some content", instance_id=NodeId("my-space", "my_file_xid"))
        """
        return run_sync(
            self.__async_client.files.upload_content_bytes(
                content=content, external_id=external_id, instance_id=instance_id
            )
        )

    def upload_bytes(
        self,
        content: str | bytes | BinaryIO | AsyncIterator[bytes],
        name: str,
        external_id: str | None = None,
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
        overwrite: bool = False,
    ) -> FileMetadata:
        """
        Upload bytes or string.

        You can also pass a file handle to 'content'. The file must be opened in binary mode or an error will be raised.

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_session`.

        Args:
            content (str | bytes | BinaryIO | AsyncIterator[bytes]): The content to upload.
            name (str): Name of the file.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            source (str | None): The source of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf,...
            metadata (dict[str, str] | None): Customizable extra data about the file. String key -> String value.
            directory (str | None): The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids (Sequence[int] | None): No description.
            data_set_id (int | None): Id of the data set.
            labels (Sequence[Label] | None): A list of the labels associated with this resource item.
            geo_location (GeoLocation | None): The geographic metadata of the file.
            source_created_time (int | None): The timestamp for when the file was originally created in the source system.
            source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
            security_categories (Sequence[int] | None): Security categories to attach to this file.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            FileMetadata: The metadata of the uploaded file.

        Examples:

            Upload a file from memory:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.upload_bytes(b"some content", name="my_file", asset_ids=[1,2,3])
        """
        return run_sync(
            self.__async_client.files.upload_bytes(
                content=content,
                name=name,
                external_id=external_id,
                source=source,
                mime_type=mime_type,
                metadata=metadata,
                directory=directory,
                asset_ids=asset_ids,
                data_set_id=data_set_id,
                labels=labels,
                geo_location=geo_location,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                security_categories=security_categories,
                overwrite=overwrite,
            )
        )

    def multipart_upload_session(
        self,
        name: str,
        parts: int,
        external_id: str | None = None,
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
        overwrite: bool = False,
    ) -> FileMultipartUploadSession:
        """
        Begin uploading a file in multiple parts. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context manager you must enter (using the `with` keyword), then call `upload_part`
        for each part before exiting. It also supports async usage with `async with`, then calling `await upload_part_async`.

        Args:
            name (str): Name of the file.
            parts (int): The number of parts to upload, must be between 1 and 250.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            source (str | None): The source of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf,...
            metadata (dict[str, str] | None): Customizable extra data about the file. String key -> String value.
            directory (str | None): The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids (Sequence[int] | None): No description.
            data_set_id (int | None): Id of the data set.
            labels (Sequence[Label] | None): A list of the labels associated with this resource item.
            geo_location (GeoLocation | None): The geographic metadata of the file.
            source_created_time (int | None): The timestamp for when the file was originally created in the source system.
            source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
            security_categories (Sequence[int] | None): Security categories to attach to this file.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            FileMultipartUploadSession: Object containing metadata about the created file, and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once all parts are uploaded.

        Examples:

            Upload binary data in two chunks:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> with client.files.multipart_upload_session("my_file.txt", parts=2) as session:
                ...     # Note that the minimum chunk size is 5 MiB.
                ...     session.upload_part(0, "hello" * 1_200_000)
                ...     session.upload_part(1, " world")
        """
        return run_sync(
            self.__async_client.files.multipart_upload_session(
                name=name,
                parts=parts,
                external_id=external_id,
                source=source,
                mime_type=mime_type,
                metadata=metadata,
                directory=directory,
                asset_ids=asset_ids,
                data_set_id=data_set_id,
                labels=labels,
                geo_location=geo_location,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                security_categories=security_categories,
                overwrite=overwrite,
            )
        )

    def multipart_upload_content_session(
        self, parts: int, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMultipartUploadSession:
        """
        Begin uploading a file in multiple parts whose metadata is already created in CDF. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context manager you must enter (using the `with` keyword), then call `upload_part`
        for each part before exiting. It also supports async usage with `async with`, then calling `await upload_part_async`.

        Args:
            parts (int): The number of parts to upload, must be between 1 and 250.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.

        Returns:
            FileMultipartUploadSession: Object containing metadata about the created file, and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once all parts are uploaded.

        Examples:

            Upload binary data in two chunks:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> with client.files.multipart_upload_content_session(external_id="external-id", parts=2) as session:
                ...     # Note that the minimum chunk size is 5 MiB.
                ...     session.upload_part(0, "hello" * 1_200_000)
                ...     session.upload_part(1, " world")
        """
        return run_sync(
            self.__async_client.files.multipart_upload_content_session(
                parts=parts, external_id=external_id, instance_id=instance_id
            )
        )

    def retrieve_download_urls(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        extended_expiration: bool = False,
    ) -> dict[int | str | NodeId, str]:
        """
        Get download links by id or external id

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External id or list of external ids.
            instance_id (NodeId | Sequence[NodeId] | None): Instance id or list of instance ids.
            extended_expiration (bool): Extend expiration time of download url to 1 hour. Defaults to false.

        Returns:
            dict[int | str | NodeId, str]: Dictionary containing download urls.
        """
        return run_sync(
            self.__async_client.files.retrieve_download_urls(
                id=id, external_id=external_id, instance_id=instance_id, extended_expiration=extended_expiration
            )
        )

    def download(
        self,
        directory: str | Path,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
    ) -> None:
        """
        `Download files by id or external id. <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_

        This method will stream all files to disk, never keeping more than 2MB in memory per worker.
        The files will be stored in the provided directory using the file name retrieved from the file metadata in CDF.
        You can also choose to keep the directory structure from CDF so that the files will be stored in subdirectories
        matching the directory attribute on the files. When missing, the (root) directory is used.
        By default, duplicate file names to the same local folder will be resolved by only keeping one of the files.
        You can choose to resolve this by appending a number to the file name using the resolve_duplicate_file_names argument.

        Warning:
            If you are downloading several files at once, be aware that file name collisions lead to all-but-one of
            the files missing. A warning is issued when this happens, listing the affected files.

        Args:
            directory (str | Path): Directory to download the file(s) to.
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.
            instance_id (NodeId | Sequence[NodeId] | None): Instance ID or list of instance ids.
            keep_directory_structure (bool): Whether or not to keep the directory hierarchy in CDF,
                creating subdirectories as needed below the given directory.
            resolve_duplicate_file_names (bool): Whether or not to resolve duplicate file names by appending a number on duplicate file names

        Examples:

            Download files by id and external id into directory 'my_directory':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])

            Download files by id to the current directory:

                >>> client.files.download(directory=".", id=[1,2,3])
        """
        return run_sync(
            self.__async_client.files.download(
                directory=directory,
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                keep_directory_structure=keep_directory_structure,
                resolve_duplicate_file_names=resolve_duplicate_file_names,
            )
        )

    def download_to_path(
        self, path: Path | str, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> None:
        """
        Download a file to a specific target.

        Args:
            path (Path | str): Download to this path.
            id (int | None): Id of of the file to download.
            external_id (str | None): External id of the file to download.
            instance_id (NodeId | None): Instance id of the file to download.

        Examples:

            Download a file by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.download_to_path("~/mydir/my_downloaded_file.txt", id=123)
        """
        return run_sync(
            self.__async_client.files.download_to_path(
                path=path, id=id, external_id=external_id, instance_id=instance_id
            )
        )

    def download_bytes(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> bytes:
        """
        Download a file as bytes.

        Args:
            id (int | None): Id of the file
            external_id (str | None): External id of the file
            instance_id (NodeId | None): Instance id of the file

        Examples:

            Download a file's content into memory:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_content = client.files.download_bytes(id=1)

        Returns:
            bytes: The file in binary format
        """
        return run_sync(
            self.__async_client.files.download_bytes(id=id, external_id=external_id, instance_id=instance_id)
        )

    def list(
        self,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        source_created_time: dict[str, Any] | TimestampRange | None = None,
        source_modified_time: dict[str, Any] | TimestampRange | None = None,
        uploaded_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
    ) -> FileMetadataList:
        """
        `List files <https://api-docs.cognite.com/20230101/tag/Files/operation/advancedListFiles>`_

        Args:
            name (str | None): Name of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf, ..
            metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value
            asset_ids (Sequence[int] | None): Only include files that reference these specific asset IDs.
            asset_external_ids (SequenceNotStr[str] | None): No description.
            asset_subtree_ids (int | Sequence[int] | None): Only include files that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include files that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only files in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only files in the specified data set(s) with this external id / these external ids.
            labels (LabelFilter | None): Return only the files matching the specified label filter(s).
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            source_created_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time (dict[str, Any] | TimestampRange | None): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            uploaded_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project.
            directory_prefix (str | None): Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded (bool | None): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit (int | None): Max number of files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).

        Returns:
            FileMetadataList: The requested files.

        Examples:

            List files metadata and filter on external id prefix:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_list = client.files.list(limit=5, external_id_prefix="prefix")

            Iterate over files metadata, one-by-one:

                >>> for file_metadata in client.files():
                ...     file_metadata  # do something with the file metadata

            Iterate over chunks of files metadata to reduce memory load:

                >>> for file_list in client.files(chunk_size=2500):
                ...     file_list # do something with the files

            Filter files based on labels:

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
                >>> file_list = client.files.list(labels=my_label_filter)

            Filter files based on geoLocation:

                >>> from cognite.client.data_classes import GeoLocationFilter, GeometryFilter
                >>> my_geo_location_filter = GeoLocationFilter(relation="intersects", shape=GeometryFilter(type="Point", coordinates=[35,10]))
                >>> file_list = client.files.list(geo_location=my_geo_location_filter)
        """
        return run_sync(
            self.__async_client.files.list(
                name=name,
                mime_type=mime_type,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                labels=labels,
                geo_location=geo_location,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                uploaded_time=uploaded_time,
                external_id_prefix=external_id_prefix,
                directory_prefix=directory_prefix,
                uploaded=uploaded,
                limit=limit,
                partitions=partitions,
            )
        )
