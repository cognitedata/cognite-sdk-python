from __future__ import annotations

import copy
import os
import warnings
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator, Sequence
from io import BufferedReader
from pathlib import Path
from typing import Any, BinaryIO, Literal, TextIO, cast, overload
from urllib.parse import urljoin, urlparse

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import _RUNNING_IN_BROWSER, DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
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
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import execute_tasks_async
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._validation import process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncFilesAPI(AsyncAPIClient):
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
        partitions: int | None = None,
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
        partitions: int | None = None,
    ) -> AsyncIterator[FileMetadataList]: ...

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
        partitions: int | None = None,
    ) -> AsyncIterator[FileMetadata] | AsyncIterator[FileMetadataList]:
        """Async iterator over files metadata."""
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
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
        ).dump(camel_case=True)

        return self._list_generator(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter,
            partitions=partitions,
        )

    def __aiter__(self) -> AsyncIterator[FileMetadata]:
        """Async iterate over all files metadata."""
        return self.__call__()

    @overload
    async def create(self, file_metadata: FileMetadata | FileMetadataWrite) -> FileMetadata: ...

    @overload
    async def create(self, file_metadata: Sequence[FileMetadata | FileMetadataWrite]) -> FileMetadataList: ...

    async def create(
        self, file_metadata: FileMetadata | FileMetadataWrite | Sequence[FileMetadata | FileMetadataWrite]
    ) -> FileMetadata | FileMetadataList:
        """`Create file metadata <https://developer.cognite.com/api#tag/Files/operation/createFiles>`_

        Args:
            file_metadata (FileMetadata | FileMetadataWrite | Sequence[FileMetadata | FileMetadataWrite]): File metadata to create.

        Returns:
            FileMetadata | FileMetadataList: The created file metadata.

        Examples:

            Create file metadata::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import FileMetadata
                >>> client = AsyncCogniteClient()
                >>> files = [FileMetadata(name="file1.txt"), FileMetadata(name="file2.txt")]
                >>> res = await client.files.create(files)
        """
        return await self._create_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            items=file_metadata,
        )

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> FileMetadata | None:
        """`Retrieve a file by id <https://developer.cognite.com/api#tag/Files/operation/byIdsFiles>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            FileMetadata | None: Requested file or None if it does not exist.

        Examples:

            Get file by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.files.retrieve(id=1)

            Get file by external id::

                >>> res = await client.files.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        """`Retrieve multiple files by id <https://developer.cognite.com/api#tag/Files/operation/byIdsFiles>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FileMetadataList: The retrieved files.

        Examples:

            Get files by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.files.retrieve_multiple(ids=[1, 2, 3])

            Get files by external id::

                >>> res = await client.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def aggregate(self, filter: FileMetadataFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate files <https://developer.cognite.com/api#tag/Files/operation/aggregateFiles>`_

        Args:
            filter (FileMetadataFilter | dict[str, Any] | None): Filter on file metadata

        Returns:
            list[CountAggregate]: List of file aggregates

        Examples:

            Aggregate files::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> aggregate_uploaded = await client.files.aggregate(filter={"uploaded": True})
        """
        return await self._aggregate(
            cls=CountAggregate,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete files <https://developer.cognite.com/api#tag/Files/operation/deleteFiles>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

        Examples:

            Delete files by id or external id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> await client.files.delete(id=[1,2,3], external_id="3")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[FileMetadata | FileMetadataUpdate]) -> FileMetadataList: ...

    @overload
    async def update(self, item: FileMetadata | FileMetadataUpdate) -> FileMetadata: ...

    async def update(self, item: FileMetadata | FileMetadataUpdate | Sequence[FileMetadata | FileMetadataUpdate]) -> FileMetadata | FileMetadataList:
        """`Update files <https://developer.cognite.com/api#tag/Files/operation/updateFiles>`_

        Args:
            item (FileMetadata | FileMetadataUpdate | Sequence[FileMetadata | FileMetadataUpdate]): File(s) to update

        Returns:
            FileMetadata | FileMetadataList: Updated file(s)

        Examples:

            Update a file that you have fetched. This will perform a full update of the file::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> file = await client.files.retrieve(id=1)
                >>> file.name = "new_name.txt"
                >>> res = await client.files.update(file)

            Perform a partial update on a file::

                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> my_update = FileMetadataUpdate(id=1).name.set("new_name.txt")
                >>> res = await client.files.update(my_update)
        """
        return await self._update_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            update_cls=FileMetadataUpdate,
            items=item,
        )

    async def search(
        self,
        name: str | None = None,
        filter: FileMetadataFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> FileMetadataList:
        """`Search for files <https://developer.cognite.com/api#tag/Files/operation/searchFiles>`_

        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and
        ordering may change over time. Use the `list` method for stable and performant iteration over all files.

        Args:
            name (str | None): Fuzzy match on name.
            filter (FileMetadataFilter | dict[str, Any] | None): Filter to apply.
            limit (int): Maximum number of results to return.

        Returns:
            FileMetadataList: Search results

        Examples:

            Search for files::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.files.search(name="some name")
        """
        return await self._search(
            list_cls=FileMetadataList,
            search={"name": name},
            filter=filter or {},
            limit=limit,
        )

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
            name (str | None): Name of the file.
            mime_type (str | None): File type. E.g. text/plain, application/pdf, ..
            metadata (dict[str, str] | None): Custom, application specific metadata.
            asset_ids (Sequence[int] | None): Only include files that reference these specific asset IDs.
            asset_external_ids (SequenceNotStr[str] | None): Asset external IDs.
            asset_subtree_ids (int | Sequence[int] | None): Only include files that have a related asset in a subtree.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include files that have a related asset in a subtree.
            data_set_ids (int | Sequence[int] | None): Return only files in the specified data sets.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only files in the specified data sets.
            labels (LabelFilter | None): Return only the files matching the specified label filter.
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            source_created_time (dict[str, Any] | TimestampRange | None): Filter for files where sourceCreatedTime is set.
            source_modified_time (dict[str, Any] | TimestampRange | None): Filter for files where sourceModifiedTime is set.
            uploaded_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            external_id_prefix (str | None): External Id provided by client.
            directory_prefix (str | None): Filter by directory prefix.
            uploaded (bool | None): Whether or not the actual file is uploaded.
            limit (int | None): Max number of files to return.
            partitions (int | None): Retrieve resources in parallel using this number of workers.

        Returns:
            FileMetadataList: The requested files.

        Examples:

            List files metadata::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> file_list = await client.files.list(limit=5)

            Filter files based on labels::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
                >>> file_list = await client.files.list(labels=my_label_filter)
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
            data_set_ids=data_set_ids_processed,
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
        ).dump(camel_case=True)

        return await self._list(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
        )

    # NOTE: File upload/download methods are not implemented yet in this async version
    # These would require async file I/O operations with aiofiles or similar
    # For now, this covers the basic CRUD operations for file metadata

    async def retrieve_download_urls(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        extended_expiration: bool = False,
    ) -> dict[int | str, str]:
        """`Retrieve download URLs for files <https://developer.cognite.com/api#tag/Files/operation/downloadLinks>`_

        Args:
            id (int | Sequence[int] | None): A single file ID or list of file IDs to retrieve download URLs for.
            external_id (str | SequenceNotStr[str] | None): A single file external ID or list of file external IDs to retrieve download URLs for.
            extended_expiration (bool): Extend expiration time of download url to 1 hour. Defaults to False.

        Returns:
            dict[int | str, str]: Dictionary mapping file IDs/external IDs to download URLs.

        Examples:

            Get download URLs by ID::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> urls = await client.files.retrieve_download_urls(id=[1, 2, 3])

            Get download URLs by external ID::

                >>> urls = await client.files.retrieve_download_urls(external_id=["file1", "file2"])
        """
        identifiers = IdentifierSequence.load(id, external_id)
        
        tasks = [
            {
                "url_path": f"{self._RESOURCE_PATH}/downloadlink",
                "json": {
                    "items": chunk.as_dicts(),
                    "extendedExpiration": extended_expiration,
                },
            }
            for chunk in identifiers.chunked(self._RETRIEVE_LIMIT)
        ]
        
        summary = await execute_tasks_async(
            self._post,
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
        )
        summary.raise_compound_exception_if_failed_tasks()
        
        # Combine results from all chunks
        url_mapping = {}
        for response in summary.results:
            for item in response.json()["items"]:
                # Map both ID and external_id if available to the download URL
                if "id" in item:
                    url_mapping[item["id"]] = item["downloadUrl"]
                if "externalId" in item:
                    url_mapping[item["externalId"]] = item["downloadUrl"]
        
        return url_mapping

    # TODO: Implement async file upload/download methods
    # - upload_content
    # - upload  
    # - upload_bytes
    # - download
    # - download_content
    # - multipart_upload_session
    # These will require async file I/O operations