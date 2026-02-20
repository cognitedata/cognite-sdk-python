from __future__ import annotations

import copy
import warnings
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any, BinaryIO, Literal, overload
from urllib.parse import urlparse

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
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
from cognite.client.exceptions import CogniteAPIError, CogniteAuthorizationError, CogniteFileUploadError
from cognite.client.utils._auxiliary import append_url_path, find_duplicates, unpack_items
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._uploading import prepare_content_for_upload
from cognite.client.utils._validation import process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"

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
    ) -> AsyncIterator[FileMetadata]: ...

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
    ) -> AsyncIterator[FileMetadataList]: ...

    async def __call__(
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
    ) -> AsyncIterator[FileMetadata] | AsyncIterator[FileMetadataList]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size: Number of files to return in each chunk. Defaults to yielding one event a time.
            name: Name of the file.
            mime_type: File type. E.g. text/plain, application/pdf, ..
            metadata: Custom, application specific metadata. String key -> String value
            asset_ids: Only include files that reference these specific asset IDs.
            asset_external_ids: No description.
            asset_subtree_ids: Only include files that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids: Only include files that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids: Return only files in the specified data set(s) with this id / these ids.
            data_set_external_ids: Return only files in the specified data set(s) with this external id / these external ids.
            labels: Return only the files matching the specified label(s).
            geo_location: Only include files matching the specified geographic relation.
            source: The source of this event.
            created_time:  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time:  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            source_created_time: Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time: Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            uploaded_time: Range between two timestamps
            external_id_prefix: External Id provided by client. Should be unique within the project.
            directory_prefix: Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded: Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit: Maximum number of files to return. Defaults to return all items.

        Yields:
            yields FileMetadata one by one if chunk_size is not specified, else FileMetadataList objects.
        """  # noqa: DOC404
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            labels=labels,
            geo_location=geo_location,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            uploaded_time=uploaded_time,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            external_id_prefix=external_id_prefix,
            directory_prefix=directory_prefix,
            uploaded=uploaded,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)

        async for item in self._list_generator(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
        ):
            yield item

    async def create(
        self, file_metadata: FileMetadata | FileMetadataWrite, overwrite: bool = False
    ) -> tuple[FileMetadata, str]:
        """Create file without uploading content.

        Args:
            file_metadata: File metadata for the file to create.
            overwrite: If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            Tuple containing the file metadata and upload url of the created file.

        Examples:

            Create a file:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_metadata = FileMetadataWrite(name="MyFile")
                >>> res = client.files.create(file_metadata)

        """
        if isinstance(file_metadata, FileMetadata):
            file_metadata = file_metadata.as_write()
        res = await self._post(
            url_path=self._RESOURCE_PATH,
            json=file_metadata.dump(camel_case=True),
            params={"overwrite": overwrite},
            semaphore=self._get_semaphore("write"),
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        file_metadata = FileMetadata._load(returned_file_metadata)
        return file_metadata, upload_url

    async def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata | None:
        """`Retrieve a single file metadata by id. <https://developer.cognite.com/api#tag/Files/operation/getFileByInternalId>`_

        Args:
            id: ID
            external_id: External ID
            instance_id: Instance ID

        Returns:
            Requested file metadata or None if it does not exist.

        Examples:

            Get file metadata by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.retrieve(id=1)

            Get file metadata by external id:

                >>> res = client.files.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=FileMetadataList, resource_cls=FileMetadata, identifiers=identifiers
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        """`Retrieve multiple file metadatas by id. <https://developer.cognite.com/api#tag/Files/operation/byIdsFiles>`_

        Args:
            ids: IDs
            external_ids: External IDs
            instance_ids: Instance IDs
            ignore_unknown_ids: Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            The requested file metadatas.

        Examples:

            Get file metadatas by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.retrieve_multiple(ids=[1, 2, 3])

            Get file_metadatas by external id:

                >>> res = client.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return await self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def aggregate_count(self, filter: FileMetadataFilter | dict[str, Any] | None = None) -> int:
        """`Aggregate files <https://developer.cognite.com/api#tag/Files/operation/aggregateFiles>`_

        Args:
            filter: Filter on file metadata filter with exact match

        Returns:
            Count of files matching the filter.

        Examples:

            Get the count of files that have been uploaded:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> aggregate_uploaded = client.files.aggregate_count(filter={"uploaded": True})
        """
        return await self._aggregate_count(filter=filter)

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete files <https://developer.cognite.com/api#tag/Files/operation/deleteFiles>`_

        Args:
            id: Id or list of ids
            external_id: str or list of str
            ignore_unknown_ids: Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete files by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.delete(id=[1,2,3], external_id="3")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(
        self,
        item: FileMetadata | FileMetadataWrite | FileMetadataUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadata: ...

    @overload
    async def update(
        self,
        item: Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadataList: ...

    async def update(
        self,
        item: FileMetadata
        | FileMetadataWrite
        | FileMetadataUpdate
        | Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> FileMetadata | FileMetadataList:
        """`Update files <https://developer.cognite.com/api#tag/Files/operation/updateFiles>`_
        Currently, a full replacement of labels on a file is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item: file(s) to update.
            mode: How to update data when a non-update object is given (FilesMetadata or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            The updated files.

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
        return await self._update_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            update_cls=FileMetadataUpdate,
            resource_path=self._RESOURCE_PATH,
            items=item,
            mode=mode,
        )

    async def search(
        self,
        name: str | None = None,
        filter: FileMetadataFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> FileMetadataList:
        """`Search for files. <https://developer.cognite.com/api#tag/Files/operation/searchFiles>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name: Prefix and fuzzy search on name.
            filter: Filter to apply. Performs exact match on these fields.
            limit: Max number of results to return.

        Returns:
            List of requested files metadata.

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
        return await self._search(list_cls=FileMetadataList, search={"name": name}, filter=filter or {}, limit=limit)

    async def upload_content(
        self,
        path: Path | str,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMetadata:
        """`Upload a file content <https://developer.cognite.com/api#tag/Files/operation/getUploadLink>`_

        Args:
            path: Path to the file you wish to upload.
            external_id: The external ID provided by the client. Must be unique within the project.
            instance_id: Instance ID of the file.
        Returns:
            No description.
        """
        path = Path(path)
        if path.is_file():
            with path.open("rb") as fh:
                return await self.upload_content_bytes(fh, external_id=external_id, instance_id=instance_id)
        elif path.is_dir():
            raise IsADirectoryError(path)
        raise FileNotFoundError(path)

    async def upload(
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
        """`Upload a file <https://developer.cognite.com/api#tag/Files/operation/initFileUpload>`_

        Args:
            path: Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            external_id: The external ID provided by the client. Must be unique within the project.
            name: Name of the file.
            source: The source of the file.
            mime_type: File type. E.g. text/plain, application/pdf, ...
            metadata: Customizable extra data about the file. String key -> String value.
            directory: The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids: No description.
            source_created_time: The timestamp for when the file was originally created in the source system.
            source_modified_time: The timestamp for when the file was last modified in the source system.
            data_set_id: ID of the data set.
            labels: A list of the labels associated with this resource item.
            geo_location: The geographic metadata of the file.
            security_categories: Security categories to attach to this file.
            recursive: If path is a directory, upload all contained files recursively.
            overwrite: If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            The file metadata of the uploaded file(s).

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
        file_metadata = FileMetadataWrite(
            # If a file is provided, we set name below based on the file name
            name=name or "",
            directory=directory,
            external_id=external_id,
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
        path = Path(path)
        if path.is_file():
            if not name:
                file_metadata.name = path.name
            return await self._upload_file_from_path(file_metadata, path, overwrite)

        elif not path.is_dir():
            raise FileNotFoundError(path)

        tasks: list[AsyncSDKTask] = []
        file_iter = path.rglob("*") if recursive else path.iterdir()
        for file in file_iter:
            if file.is_file():
                file_metadata = copy.copy(file_metadata)
                file_metadata.name = file.name
                tasks.append(AsyncSDKTask(self._upload_file_from_path, file_metadata, file, overwrite))

        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=lambda task: task[0].name)
        return FileMetadataList(tasks_summary.results)

    async def _upload_file_from_path(
        self, file_metadata: FileMetadataWrite, path: Path, overwrite: bool
    ) -> FileMetadata:
        with path.open("rb") as fh:
            return await self.upload_bytes(fh, overwrite=overwrite, **file_metadata.dump(camel_case=False))

    async def upload_content_bytes(
        self,
        content: str | bytes | BinaryIO,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMetadata:
        """Upload bytes or string (UTF-8 assumed).

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_content_session`.

        Args:
            content: The content to upload.
            external_id: The external ID provided by the client. Must be unique within the project.
            instance_id: Instance ID of the file.

        Returns:
            No description.

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
        identifiers = IdentifierSequence.load(external_ids=external_id, instance_ids=instance_id).as_singleton()

        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/uploadlink",
                json={"items": identifiers.as_dicts()},
                semaphore=self._get_semaphore("write"),
            )
        except CogniteAPIError as e:
            if e.code == 403:
                raise CogniteAuthorizationError(
                    message=e.message,
                    code=e.code,
                    x_request_id=e.x_request_id,
                    cluster=self._config.cdf_cluster,
                    project=self._config.project,
                ) from e
            raise

        return await self._upload_bytes(content, res.json()["items"][0])

    async def _upload_bytes(
        self, content: str | bytes | BinaryIO | AsyncIterator[bytes], returned_file_metadata: dict
    ) -> FileMetadata:
        upload_url = returned_file_metadata["uploadUrl"]
        if urlparse(upload_url).netloc:
            full_upload_url = upload_url
        else:
            full_upload_url = append_url_path(self._config.base_url, upload_url)

        headers = {"accept": "*/*"}
        file_metadata = FileMetadata._load(returned_file_metadata)
        if file_metadata.mime_type is not None:
            headers["Content-Type"] = file_metadata.mime_type

        file_size, file_content = prepare_content_for_upload(content)
        if file_size is not None:
            headers["Content-Length"] = str(file_size)

        upload_response = await self._request(
            "PUT",
            full_url=full_upload_url,
            content=file_content,
            headers=headers,
            timeout=self._config.file_transfer_timeout,
            semaphore=self._get_semaphore("write"),
        )
        if not upload_response.is_success:
            raise CogniteFileUploadError(message=upload_response.text, code=upload_response.status_code)
        return file_metadata

    async def upload_bytes(
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
        """Upload bytes or string.

        You can also pass a file handle to 'content'. The file must be opened in binary mode or an error will be raised.

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_session`.

        Args:
            content: The content to upload.
            name: Name of the file.
            external_id: The external ID provided by the client. Must be unique within the project.
            source: The source of the file.
            mime_type: File type. E.g. text/plain, application/pdf,...
            metadata: Customizable extra data about the file. String key -> String value.
            directory: The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids: No description.
            data_set_id: Id of the data set.
            labels: A list of the labels associated with this resource item.
            geo_location: The geographic metadata of the file.
            source_created_time: The timestamp for when the file was originally created in the source system.
            source_modified_time: The timestamp for when the file was last modified in the source system.
            security_categories: Security categories to attach to this file.
            overwrite: If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            The metadata of the uploaded file.

        Examples:

            Upload a file from memory:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.files.upload_bytes(b"some content", name="my_file", asset_ids=[1,2,3])
        """
        file_metadata = FileMetadataWrite(
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
        )
        try:
            res = await self._post(
                url_path=self._RESOURCE_PATH,
                json=file_metadata.dump(camel_case=True),
                params={"overwrite": overwrite},
                semaphore=self._get_semaphore("write"),
            )
        except CogniteAPIError as e:
            if e.code == 403 and "insufficient access rights" in e.message:
                dsid_notice = " Try to provide a data_set_id." if data_set_id is None else ""
                msg = f"Could not create a file due to insufficient access rights.{dsid_notice}"
                raise CogniteAuthorizationError(
                    message=msg,
                    code=e.code,
                    x_request_id=e.x_request_id,
                    cluster=self._config.cdf_cluster,
                    project=self._config.project,
                ) from e
            raise

        return await self._upload_bytes(content, res.json())

    async def multipart_upload_session(
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
        """Begin uploading a file in multiple parts. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context manager you must enter (using the `with` keyword), then call `upload_part`
        for each part before exiting. It also supports async usage with `async with`, then calling `await upload_part_async`.

        Args:
            name: Name of the file.
            parts: The number of parts to upload, must be between 1 and 250.
            external_id: The external ID provided by the client. Must be unique within the project.
            source: The source of the file.
            mime_type: File type. E.g. text/plain, application/pdf,...
            metadata: Customizable extra data about the file. String key -> String value.
            directory: The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids: No description.
            data_set_id: Id of the data set.
            labels: A list of the labels associated with this resource item.
            geo_location: The geographic metadata of the file.
            source_created_time: The timestamp for when the file was originally created in the source system.
            source_modified_time: The timestamp for when the file was last modified in the source system.
            security_categories: Security categories to attach to this file.
            overwrite: If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. The actual file will be overwritten after successful upload. If there is no successful upload, the current file contents will be kept. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            Object containing metadata about the created file, and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once all parts are uploaded.

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
        file_metadata = FileMetadataWrite(
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
        )
        try:
            res = await self._post(
                url_path=self._RESOURCE_PATH + "/initmultipartupload",
                json=file_metadata.dump(camel_case=True),
                params={"overwrite": overwrite, "parts": parts},
                semaphore=self._get_semaphore("write"),
            )
        except CogniteAPIError as e:
            if e.code == 403 and "insufficient access rights" in e.message:
                dsid_notice = " Try to provide a data_set_id." if data_set_id is None else ""
                msg = f"Could not create a file due to insufficient access rights.{dsid_notice}"
                raise CogniteAuthorizationError(
                    message=msg,
                    code=e.code,
                    x_request_id=e.x_request_id,
                    cluster=self._config.cdf_cluster,
                    project=self._config.project,
                ) from e
            raise

        returned_file_metadata = res.json()
        upload_urls = returned_file_metadata["uploadUrls"]
        upload_id = returned_file_metadata["uploadId"]

        return FileMultipartUploadSession(
            FileMetadata._load(returned_file_metadata), upload_urls, upload_id, self._cognite_client
        )

    async def multipart_upload_content_session(
        self,
        parts: int,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMultipartUploadSession:
        """Begin uploading a file in multiple parts whose metadata is already created in CDF. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context manager you must enter (using the `with` keyword), then call `upload_part`
        for each part before exiting. It also supports async usage with `async with`, then calling `await upload_part_async`.

        Args:
            parts: The number of parts to upload, must be between 1 and 250.
            external_id: The external ID provided by the client. Must be unique within the project.
            instance_id: Instance ID of the file.

        Returns:
            Object containing metadata about the created file, and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once all parts are uploaded.

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
        identifiers = IdentifierSequence.load(external_ids=external_id, instance_ids=instance_id).as_singleton()
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/multiuploadlink",
                json={"items": identifiers.as_dicts()},
                params={"parts": parts},
                semaphore=self._get_semaphore("write"),
            )
        except CogniteAPIError as e:
            if e.code == 403:
                raise CogniteAuthorizationError(
                    message=e.message,
                    code=e.code,
                    x_request_id=e.x_request_id,
                    cluster=self._config.cdf_cluster,
                    project=self._config.project,
                ) from e
            raise

        returned_file_metadata = res.json()["items"][0]
        upload_urls = returned_file_metadata["uploadUrls"]
        upload_id = returned_file_metadata["uploadId"]

        return FileMultipartUploadSession(
            FileMetadata._load(returned_file_metadata), upload_urls, upload_id, self._cognite_client
        )

    async def _upload_multipart_part(self, upload_url: str, content: str | bytes | BinaryIO) -> None:
        """Upload part of a file to an upload URL returned from `multipart_upload_session`.

        Note:
            If `content` does not somehow expose its length, this method may not work on Azure or AWS.

        Args:
            upload_url: URL to upload file chunk to.
            content: The content to upload.
        """
        headers = {"accept": "*/*"}
        file_size, file_content = prepare_content_for_upload(content)
        if file_size is not None:
            headers["Content-Length"] = str(file_size)

        upload_response = await self._request(
            "PUT",
            full_url=upload_url,
            content=file_content,
            headers=headers,
            timeout=self._config.file_transfer_timeout,
            semaphore=self._get_semaphore("write"),
        )
        if not upload_response.is_success:
            raise CogniteFileUploadError(message=upload_response.text, code=upload_response.status_code)

    async def _complete_multipart_upload(self, session: FileMultipartUploadSession) -> None:
        """Complete a multipart upload. Once this returns the file can be downloaded.

        Args:
            session: Multipart upload session returned from
        """
        await self._post(
            self._RESOURCE_PATH + "/completemultipartupload",
            json={"id": session.file_metadata.id, "uploadId": session._upload_id},
            semaphore=self._get_semaphore("write"),
        )

    async def retrieve_download_urls(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        extended_expiration: bool = False,
    ) -> dict[int | str | NodeId, str]:
        """Get download links by id or external id

        Args:
            id: Id or list of ids.
            external_id: External id or list of external ids.
            instance_id: Instance id or list of instance ids.
            extended_expiration: Extend expiration time of download url to 1 hour. Defaults to false.

        Returns:
            Dictionary containing download urls.
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id)

        query_params = {}
        if extended_expiration:
            query_params["extendedExpiration"] = True
        tasks = [
            AsyncSDKTask(
                self._post,
                url_path=f"{self._RESOURCE_PATH}/downloadlink",
                json={"items": batch.as_dicts()},
                params=query_params,
                semaphore=self._get_semaphore("read"),
            )
            for batch in identifiers.chunked(100)
        ]
        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        results = tasks_summary.joined_results(unpack_items)
        return {
            result.get("id") or result.get("externalId") or NodeId.load(result["instanceId"]): result["downloadUrl"]
            for result in results
        }

    @staticmethod
    def _create_unique_file_names(file_names_in: list[str] | list[Path]) -> list[Path]:
        """Create unique file names by appending a number to the base file name."""
        file_names: list[str] = [str(file_name) for file_name in file_names_in]
        unique_original = set(file_names)
        unique_created = []
        original_count: defaultdict = defaultdict(int)

        for file_name in file_names:
            if file_name not in unique_created:
                unique_created.append(file_name)
                continue

            file_suffixes = Path(file_name).suffixes
            file_postfix = "".join(file_suffixes)
            file_base = file_name.removesuffix(file_postfix)

            new_name = file_name
            while (new_name in unique_created) or (new_name in unique_original):
                original_count[file_name] += 1
                new_name = f"{file_base}({original_count[file_name]}){file_postfix}"

            unique_created.append(new_name)

        return list(map(Path, unique_created))

    async def download(
        self,
        directory: str | Path,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
    ) -> None:
        """`Download files by id or external id. <https://developer.cognite.com/api#tag/Files/operation/downloadLinks>`_

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
            directory: Directory to download the file(s) to.
            id: Id or list of ids
            external_id: External ID or list of external ids.
            instance_id: Instance ID or list of instance ids.
            keep_directory_structure: Whether or not to keep the directory hierarchy in CDF, creating subdirectories as needed below the given directory.
            resolve_duplicate_file_names: Whether or not to resolve duplicate file names by appending a number on duplicate file names

        Examples:

            Download files by id and external id into directory 'my_directory':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])

            Download files by id to the current directory:

                >>> client.files.download(directory=".", id=[1,2,3])
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id)

        directory = Path(directory)
        if not directory.is_dir():
            raise NotADirectoryError(directory)

        all_identifiers = identifiers.as_dicts()
        id_to_metadata = await self._get_id_to_metadata_map(all_identifiers)

        all_ids, filepaths, directories = self._get_ids_filepaths_directories(
            directory, id_to_metadata, keep_directory_structure
        )
        if keep_directory_structure:
            for file_folder in set(directories):
                file_folder.mkdir(parents=True, exist_ok=True)

        if resolve_duplicate_file_names:
            filepaths = self._create_unique_file_names(filepaths)

        await self._download_files_to_directory(
            directory=directory, all_ids=all_ids, id_to_metadata=id_to_metadata, filepaths=filepaths
        )

    @staticmethod
    def _get_ids_filepaths_directories(
        directory: Path,
        id_to_metadata: dict[int, FileMetadata],
        keep_directory_structure: bool = False,
    ) -> tuple[list[int], list[Path], list[Path]]:
        ids: list[int] = []
        filepaths, file_directories = [], []
        for identifier, metadata in id_to_metadata.items():
            if not isinstance(identifier, int):
                continue
            file_directory = directory
            if metadata.directory and keep_directory_structure:
                # CDF enforces absolute, unix-style paths (i.e. always stating with '/'). We strip to make it relative:
                file_directory /= metadata.directory[1:]

            ids.append(identifier)
            file_directories.append(file_directory)
            filepaths.append(file_directory / metadata.name)

        return ids, filepaths, file_directories

    @staticmethod
    def _warn_on_duplicate_filenames(filepaths: list[Path]) -> None:
        if duplicates := sorted(find_duplicates(filepaths)):
            warnings.warn(
                (
                    f"There are {len(duplicates)} duplicate file name(s). Only one of each duplicate will be "
                    f"downloaded, per directory. The affected files: {list(map(str, duplicates))}"
                ),
                UserWarning,
                stacklevel=2,
            )

    async def _get_id_to_metadata_map(self, all_ids: Sequence[dict]) -> dict[int, FileMetadata]:
        ids = [id["id"] for id in all_ids if "id" in id]
        external_ids = [id["externalId"] for id in all_ids if "externalId" in id]
        instance_ids = [id["instanceId"] for id in all_ids if "instanceId" in id]
        resource_lst = await self.retrieve_multiple(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return resource_lst._id_to_item

    async def _download_files_to_directory(
        self,
        directory: Path,
        all_ids: Sequence[int],
        id_to_metadata: dict[int, FileMetadata],
        filepaths: list[Path],
    ) -> None:
        self._warn_on_duplicate_filenames(filepaths)
        tasks = [
            AsyncSDKTask(self._process_file_download, directory, identifier={"id": id_}, path=filepath)
            for id_, filepath in zip(all_ids, filepaths)
        ]
        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: id_to_metadata[task["identifier"]["id"]]
        )

    async def _get_download_link(self, identifier: dict[str, int | str]) -> str:
        response = await self._post(
            url_path=f"{self._RESOURCE_PATH}/downloadlink",
            json={"items": [identifier]},
            semaphore=self._get_semaphore("read"),
        )
        return unpack_items(response)[0]["downloadUrl"]

    async def _process_file_download(
        self,
        directory: Path,
        identifier: dict[str, int | str],
        path: Path,
    ) -> None:
        file_path = path.resolve()
        if not file_path.is_relative_to(directory.resolve()):
            raise RuntimeError(f"Resolved file path '{file_path}' is not inside download directory")

        download_link = await self._get_download_link(identifier)
        await self._download_file_to_path(download_link, file_path)

    async def _download_file_to_path(self, download_link: str, path: Path) -> None:
        from cognite.client import global_config

        stream = self._stream(
            "GET",
            full_url=download_link,
            full_headers={"accept": "*/*"},
            timeout=self._config.file_transfer_timeout,
            semaphore=self._get_semaphore("read"),
        )
        with path.open("wb") as file:
            async with stream as response:
                async for chunk in response.aiter_bytes(chunk_size=global_config.file_download_chunk_size):
                    file.write(chunk)

    async def download_to_path(
        self, path: Path | str, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> None:
        """Download a file to a specific target.

        Args:
            path: Download to this path.
            id: Id of of the file to download.
            external_id: External id of the file to download.
            instance_id: Instance id of the file to download.

        Examples:

            Download a file by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.files.download_to_path("~/mydir/my_downloaded_file.txt", id=123)
        """
        path = Path(path)
        if not path.parent.is_dir():
            raise NotADirectoryError(path.parent)

        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        download_link = await self._get_download_link(identifier)
        await self._download_file_to_path(download_link, path)

    async def download_bytes(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> bytes:
        """Download a file as bytes.

        Args:
            id: Id of the file
            external_id: External id of the file
            instance_id: Instance id of the file

        Examples:

            Download a file's content into memory:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> file_content = client.files.download_bytes(id=1)

        Returns:
            The file in binary format
        """
        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        download_link = await self._get_download_link(identifier)
        return await self._download_file(download_link)

    async def _download_file(self, download_link: str) -> bytes:
        response = await self._request(
            "GET",
            full_url=download_link,
            headers={"accept": "*/*"},
            timeout=self._config.file_transfer_timeout,
            semaphore=self._get_semaphore("read"),
        )
        return response.content

    async def list(
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
        """`List files <https://developer.cognite.com/api#tag/Files/operation/advancedListFiles>`_

        Args:
            name: Name of the file.
            mime_type: File type. E.g. text/plain, application/pdf, ..
            metadata: Custom, application specific metadata. String key -> String value
            asset_ids: Only include files that reference these specific asset IDs.
            asset_external_ids: No description.
            asset_subtree_ids: Only include files that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids: Only include files that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids: Return only files in the specified data set(s) with this id / these ids.
            data_set_external_ids: Return only files in the specified data set(s) with this external id / these external ids.
            labels: Return only the files matching the specified label filter(s).
            geo_location: Only include files matching the specified geographic relation.
            source: The source of this event.
            created_time:  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time:  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            source_created_time: Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time: Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            uploaded_time: Range between two timestamps
            external_id_prefix: External Id provided by client. Should be unique within the project.
            directory_prefix: Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded: Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit: Max number of files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions: Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).

        Returns:
            The requested files.

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
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            labels=labels,
            geo_location=geo_location,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            uploaded_time=uploaded_time,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            external_id_prefix=external_id_prefix,
            directory_prefix=directory_prefix,
            uploaded=uploaded,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
        )
