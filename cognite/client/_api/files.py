from __future__ import annotations

import copy
import os
import warnings
from collections import defaultdict
from collections.abc import Iterator, Sequence
from io import BufferedReader
from pathlib import Path
from typing import Any, BinaryIO, Literal, TextIO, cast, overload
from urllib.parse import urljoin, urlparse

from cognite.client._api_client import APIClient
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
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._validation import process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"

    @staticmethod
    def _warn_alpha() -> None:
        FeaturePreviewWarning(api_maturity="alpha", feature_name="Files with Instance ID", sdk_maturity="alpha").warn()

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
        partitions: int | None = None,
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
        partitions: int | None = None,
    ) -> Iterator[FileMetadata] | Iterator[FileMetadataList]:
        """Iterate over files

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
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).

        Returns:
            Iterator[FileMetadata] | Iterator[FileMetadataList]: yields FileMetadata one by one if chunk_size is not specified, else FileMetadataList objects.
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
        return self._list_generator(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[FileMetadata]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Returns:
            Iterator[FileMetadata]: yields Files one by one.
        """
        return self()

    def create(
        self, file_metadata: FileMetadata | FileMetadataWrite, overwrite: bool = False
    ) -> tuple[FileMetadata, str]:
        """Create file without uploading content.

        Args:
            file_metadata (FileMetadata | FileMetadataWrite): File metadata for the file to create.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field, fields for the file found for externalId can be overwritten. The default setting is false. If metadata is included in the request body, all of the original metadata will be overwritten. File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body. Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            tuple[FileMetadata, str]: Tuple containing the file metadata and upload url of the created file.

        Examples:

            Create a file::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataWrite
                >>> client = CogniteClient()
                >>> file_metadata = FileMetadataWrite(name="MyFile")
                >>> res = client.files.create(file_metadata)

        """
        if isinstance(file_metadata, FileMetadata):
            file_metadata = file_metadata.as_write()
        res = self._post(
            url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        file_metadata = FileMetadata._load(returned_file_metadata)
        return file_metadata, upload_url

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata | None:
        """`Retrieve a single file metadata by id. <https://developer.cognite.com/api#tag/Files/operation/getFileByInternalId>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            FileMetadata | None: Requested file metadata or None if it does not exist.

        Examples:

            Get file metadata by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve(id=1)

            Get file metadata by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve(external_id="1")
        """
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=FileMetadataList, resource_cls=FileMetadata, identifiers=identifiers, headers=headers
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        """`Retrieve multiple file metadatas by id. <https://developer.cognite.com/api#tag/Files/operation/byIdsFiles>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FileMetadataList: The requested file metadatas.

        Examples:

            Get file metadatas by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve_multiple(ids=[1, 2, 3])

            Get file_metadatas by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        header: dict | None = None
        if instance_ids is not None:
            self._warn_alpha()
            header = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            headers=header,
        )

    def aggregate(self, filter: FileMetadataFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate files <https://developer.cognite.com/api#tag/Files/operation/aggregateFiles>`_

        Args:
            filter (FileMetadataFilter | dict[str, Any] | None): Filter on file metadata filter with exact match

        Returns:
            list[CountAggregate]: List of count aggregates

        Examples:

            List files metadata and filter on external id prefix::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> aggregate_uploaded = client.files.aggregate(filter={"uploaded": True})
        """

        return self._aggregate(filter=filter, cls=CountAggregate)

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete files <https://developer.cognite.com/api#tag/Files/operation/deleteFiles>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): str or list of str
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete files by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.files.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
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
        """`Update files <https://developer.cognite.com/api#tag/Files/operation/updateFiles>`_
        Currently, a full replacement of labels on a file is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (FileMetadata | FileMetadataWrite | FileMetadataUpdate | Sequence[FileMetadata | FileMetadataWrite | FileMetadataUpdate]): file(s) to update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (FilesMetadata or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            FileMetadata | FileMetadataList: The updated files.

        Examples:

            Update file metadata that you have fetched. This will perform a full update of the file metadata::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> file_metadata = client.files.retrieve(id=1)
                >>> file_metadata.description = "New description"
                >>> res = client.files.update(file_metadata)

            Perform a partial update on file metadata, updating the source and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> client = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).source.set("new source").metadata.add({"key": "value"})
                >>> res = client.files.update(my_update)

            Attach labels to a files::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> client = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).labels.add(["PUMP", "VERIFIED"])
                >>> res = client.files.update(my_update)

            Detach a single label from a file::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> client = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).labels.remove("PUMP")
                >>> res = client.files.update(my_update)
        """
        headers: dict | None = None
        if (isinstance(item, Sequence) and any(file.instance_id for file in item)) or (
            not isinstance(item, Sequence) and item.instance_id
        ):
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}

        return self._update_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            update_cls=FileMetadataUpdate,
            resource_path=self._RESOURCE_PATH,
            items=item,
            headers=headers,
            mode=mode,
        )

    def search(
        self,
        name: str | None = None,
        filter: FileMetadataFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> FileMetadataList:
        """`Search for files. <https://developer.cognite.com/api#tag/Files/operation/searchFiles>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            filter (FileMetadataFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Max number of results to return.

        Returns:
            FileMetadataList: List of requested files metadata.

        Examples:

            Search for a file::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.search(name="some name")

            Search for an asset with an attached label:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG"])
                >>> res = client.assets.search(name="xyz",filter=FileMetadataFilter(labels=my_label_filter))
        """
        return self._search(list_cls=FileMetadataList, search={"name": name}, filter=filter or {}, limit=limit)

    def upload_content(
        self,
        path: str,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMetadata:
        """`Upload a file content <https://developer.cognite.com/api#tag/Files/operation/getUploadLink>`_

        Args:
            path (str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.
        Returns:
            FileMetadata: No description.
        """
        fh: bytes | BufferedReader
        if os.path.isfile(path):
            with open(path, "rb") as fh:
                if _RUNNING_IN_BROWSER:
                    # Pyodide doesn't handle file handles correctly, so we need to read everything into memory:
                    fh = fh.read()
                file_metadata = self.upload_content_bytes(fh, external_id=external_id, instance_id=instance_id)
            return file_metadata

        raise ValueError(f"The path '{path}' must exist and be a file")

    def upload(
        self,
        path: str,
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
            path (str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
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

            Upload a file in a given path::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.upload("/path/to/file", name="my_file")

            If name is omitted, this method will use the name of the file

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.upload("/path/to/file")

            You can also upload all files in a directory by setting path to the path of a directory::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.upload("/path/to/my/directory")

            Upload a file with a label::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Label
                >>> client = CogniteClient()
                >>> res = client.files.upload("/path/to/file", name="my_file", labels=[Label(external_id="WELL LOG")])

            Upload a file with a geo_location::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GeoLocation, Geometry
                >>> client = CogniteClient()
                >>> geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
                >>> res = client.files.upload("/path/to/file", geo_location=GeoLocation(type="Feature", geometry=geometry))

        """
        file_metadata = FileMetadata(
            name=name,
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
        if os.path.isfile(path):
            if not name:
                file_metadata.name = os.path.basename(path)
            return self._upload_file_from_path(file_metadata, path, overwrite)
        elif os.path.isdir(path):
            tasks = []
            if recursive:
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        basename = os.path.basename(file_path)
                        file_metadata = copy.copy(file_metadata)
                        file_metadata.name = basename
                        tasks.append((file_metadata, file_path, overwrite))
            else:
                for file_name in os.listdir(path):
                    file_path = os.path.join(path, file_name)
                    if os.path.isfile(file_path):
                        file_metadata = copy.copy(file_metadata)
                        file_metadata.name = file_name
                        tasks.append((file_metadata, file_path, overwrite))
            tasks_summary = execute_tasks(self._upload_file_from_path, tasks, self._config.max_workers)
            tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=lambda x: x[0].name)
            return FileMetadataList(tasks_summary.results)
        raise ValueError(f"The path '{path}' does not exist")

    def _upload_file_from_path(self, file: FileMetadata, file_path: str, overwrite: bool) -> FileMetadata:
        fh: bytes | BufferedReader
        with open(file_path, "rb") as fh:
            if _RUNNING_IN_BROWSER:
                # Pyodide doesn't handle file handles correctly, so we need to read everything into memory:
                fh = fh.read()
            file_metadata = self.upload_bytes(fh, overwrite=overwrite, **file.dump(camel_case=False))
        return file_metadata

    def upload_content_bytes(
        self,
        content: str | bytes | TextIO | BinaryIO,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMetadata:
        """Upload bytes or string.

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_content_session`.

        Args:
            content (str | bytes | TextIO | BinaryIO): The content to upload.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.

        Returns:
            FileMetadata: No description.

        Examples:

            Upload a file from memory::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.upload_bytes(b"some content", name="my_file", asset_ids=[1,2,3])
        """
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(external_ids=external_id, instance_ids=instance_id).as_singleton()

        if isinstance(content, str):
            content = content.encode("utf-8")

        try:
            res = self._post(
                url_path=f"{self._RESOURCE_PATH}/uploadlink",
                json={"items": identifiers.as_dicts()},
                headers=headers,
            )
        except CogniteAPIError as e:
            if e.code == 403:
                raise CogniteAuthorizationError(message=e.message, code=e.code, x_request_id=e.x_request_id) from e
            raise

        file_metadata = self._upload_bytes(content, res.json()["items"][0])

        return file_metadata

    def _upload_bytes(self, content: bytes | TextIO | BinaryIO, returned_file_metadata: dict) -> FileMetadata:
        upload_url = returned_file_metadata["uploadUrl"]
        if urlparse(upload_url).netloc:
            full_upload_url = upload_url
        else:
            full_upload_url = urljoin(self._config.base_url, upload_url)
        file_metadata = FileMetadata.load(returned_file_metadata)
        headers = {"Content-Type": file_metadata.mime_type}
        upload_response = self._http_client_with_retry.request(
            "PUT",
            full_upload_url,
            accept="*/*",
            data=content,
            timeout=self._config.file_transfer_timeout,
            headers=headers,
        )
        if not upload_response.ok:
            raise CogniteFileUploadError(
                message=upload_response.text,
                code=upload_response.status_code,
            )
        return file_metadata

    def upload_bytes(
        self,
        content: str | bytes | TextIO | BinaryIO,
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

        You can also pass a file handle to content.

        Note that the maximum file size is 5GiB. In order to upload larger files use `multipart_upload_session`.

        Args:
            content (str | bytes | TextIO | BinaryIO): The content to upload.
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
            FileMetadata: No description.

        Examples:

            Upload a file from memory::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.upload_bytes(b"some content", name="my_file", asset_ids=[1,2,3])
        """
        if isinstance(content, str):
            content = content.encode("utf-8")

        file_metadata = FileMetadata(
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
            res = self._post(
                url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
            )
        except CogniteAPIError as e:
            if e.code == 403 and "insufficient access rights" in e.message:
                dsid_notice = " Try to provide a data_set_id." if data_set_id is None else ""
                msg = f"Could not create a file due to insufficient access rights.{dsid_notice}"
                raise CogniteAuthorizationError(message=msg, code=e.code, x_request_id=e.x_request_id) from e
            raise

        return self._upload_bytes(content, res.json())

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
        """Begin uploading a file in multiple parts. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context object you must enter (using the `with` keyword), then call `upload_part` on
        for each part before exiting.

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
            FileMultipartUploadSession: Object containing metadata about the created file,
            and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once
            all parts are uploaded.

        Examples:

            Upload binary data in two chunks

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> with client.files.multipart_upload_session("my_file.txt", parts=2) as session:
                ...     # Note that the minimum chunk size is 5 MiB.
                ...     session.upload_part(0, "hello" * 1_200_000)
                ...     session.upload_part(1, " world")
        """
        file_metadata = FileMetadata(
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
            res = self._post(
                url_path=self._RESOURCE_PATH + "/initmultipartupload",
                json=file_metadata.dump(camel_case=True),
                params={"overwrite": overwrite, "parts": parts},
            )
        except CogniteAPIError as e:
            if e.code == 403 and "insufficient access rights" in e.message:
                dsid_notice = " Try to provide a data_set_id." if data_set_id is None else ""
                msg = f"Could not create a file due to insufficient access rights.{dsid_notice}"
                raise CogniteAuthorizationError(message=msg, code=e.code, x_request_id=e.x_request_id) from e
            raise

        returned_file_metadata = res.json()
        upload_urls = returned_file_metadata["uploadUrls"]
        upload_id = returned_file_metadata["uploadId"]

        return FileMultipartUploadSession(
            FileMetadata._load(returned_file_metadata), upload_urls, upload_id, self._cognite_client
        )

    def multipart_upload_content_session(
        self,
        parts: int,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> FileMultipartUploadSession:
        """Begin uploading a file in multiple parts. This allows uploading files larger than 5GiB.
        Note that the size of each part may not exceed 4000MiB, and the size of each part except the last
        must be greater than 5MiB.

        The file chunks may be uploaded in any order, and in parallel, but the client must ensure that
        the parts are stored in the correct order by uploading each chunk to the correct upload URL.

        This returns a context object you must enter (using the `with` keyword), then call `upload_part` on
        for each part before exiting.

        Args:
            parts (int): The number of parts to upload, must be between 1 and 250.
            external_id (str | None): The external ID provided by the client. Must be unique within the project.
            instance_id (NodeId | None): Instance ID of the file.

        Returns:
            FileMultipartUploadSession: Object containing metadata about the created file,
            and information needed to upload the file content. Use this object to manage the file upload, and `exit` it once
            all parts are uploaded.

        Examples:

            Upload binary data in two chunks

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> with client.files.multipart_upload_content_session(external_id="external-id", parts=2) as session:
                ...     # Note that the minimum chunk size is 5 MiB.
                ...     session.upload_part(0, "hello" * 1_200_000)
                ...     session.upload_part(1, " world")
        """
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(external_ids=external_id, instance_ids=instance_id).as_singleton()

        try:
            res = self._post(
                url_path=f"{self._RESOURCE_PATH}/multiuploadlink",
                json={"items": identifiers.as_dicts()},
                params={"parts": parts},
                headers=headers,
            )
        except CogniteAPIError as e:
            if e.code == 403:
                raise CogniteAuthorizationError(message=e.message, code=e.code, x_request_id=e.x_request_id) from e
            raise

        returned_file_metadata = res.json()["items"][0]
        upload_urls = returned_file_metadata["uploadUrls"]
        upload_id = returned_file_metadata["uploadId"]

        return FileMultipartUploadSession(
            FileMetadata.load(returned_file_metadata), upload_urls, upload_id, self._cognite_client
        )

    def _upload_multipart_part(self, upload_url: str, content: str | bytes | TextIO | BinaryIO) -> None:
        """Upload part of a file to an upload URL returned from `multipart_upload_session`.
        Note that if `content` does not somehow expose its length, this method may not work
        on Azure. See `requests.utils.super_len`.

        Args:
            upload_url (str): URL to upload file chunk to.
            content (str | bytes | TextIO | BinaryIO): The content to upload.
        """
        if isinstance(content, str):
            content = content.encode("utf-8")

        upload_response = self._http_client_with_retry.request(
            "PUT",
            upload_url,
            accept="*/*",
            data=content,
            timeout=self._config.file_transfer_timeout,
            headers=None,
        )
        if not upload_response.ok:
            raise CogniteFileUploadError(
                message=upload_response.text,
                code=upload_response.status_code,
            )

    def _complete_multipart_upload(self, session: FileMultipartUploadSession) -> None:
        """Complete a multipart upload. Once this returns the file can be downloaded.

        Args:
            session (FileMultipartUploadSession): Multipart upload session returned from
        """
        headers: dict | None = None
        if session.file_metadata.instance_id is not None:
            headers = {"cdf-version": "alpha"}
        self._post(
            self._RESOURCE_PATH + "/completemultipartupload",
            json={"id": session.file_metadata.id, "uploadId": session._upload_id},
            headers=headers,
        )

    def retrieve_download_urls(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        extended_expiration: bool = False,
    ) -> dict[int | str | NodeId, str]:
        """Get download links by id or external id

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External id or list of external ids.
            instance_id (NodeId | Sequence[NodeId] | None): Instance id or list of instance ids.
            extended_expiration (bool): Extend expiration time of download url to 1 hour. Defaults to false.

        Returns:
            dict[int | str | NodeId, str]: Dictionary containing download urls.
        """

        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id)

        batch_size = 100
        id_batches = [seq.as_dicts() for seq in identifiers.chunked(batch_size)]
        query_params = {}
        if extended_expiration:
            query_params["extendedExpiration"] = True
        tasks = [
            dict(url_path="/files/downloadlink", json={"items": id_batch}, params=query_params, headers=headers)
            for id_batch in id_batches
        ]
        tasks_summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        results = tasks_summary.joined_results(unwrap_fn=lambda res: res.json()["items"])
        return {
            result.get("id") or result.get("externalId") or NodeId.load(result["instanceId"]): result["downloadUrl"]
            for result in results
        }

    @staticmethod
    def _create_unique_file_names(file_names_in: list[str] | list[Path]) -> list[str]:
        """
        Create unique file names by appending a number to the base file name.
        """
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
            file_base = file_name[: file_name.index(file_postfix) or None]  # Awaiting .removesuffix in 3.9:

            new_name = file_name
            while (new_name in unique_created) or (new_name in unique_original):
                original_count[file_name] += 1
                new_name = f"{file_base}({original_count[file_name]}){file_postfix}"

            unique_created.append(new_name)

        return unique_created

    def download(
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
            directory (str | Path): Directory to download the file(s) to.
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.
            instance_id (NodeId | Sequence[NodeId] | None): Instance ID or list of instance ids.
            keep_directory_structure (bool): Whether or not to keep the directory hierarchy in CDF,
                creating subdirectories as needed below the given directory.
            resolve_duplicate_file_names (bool): Whether or not to resolve duplicate file names by appending a number on duplicate file names

        Examples:

            Download files by id and external id into directory 'my_directory'::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])

            Download files by id to the current directory::

                >>> client.files.download(directory=".", id=[1,2,3])
        """
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id)

        directory = Path(directory)
        if not directory.is_dir():
            raise NotADirectoryError(str(directory))

        all_identifiers = identifiers.as_dicts()
        id_to_metadata = self._get_id_to_metadata_map(all_identifiers)

        all_ids, filepaths, directories = self._get_ids_filepaths_directories(
            directory, id_to_metadata, keep_directory_structure
        )

        if keep_directory_structure:
            for file_folder in set(directories):
                file_folder.mkdir(parents=True, exist_ok=True)

        if resolve_duplicate_file_names:
            filepaths_str = self._create_unique_file_names(filepaths)
            filepaths = [Path(file_path) for file_path in filepaths_str]

        self._download_files_to_directory(
            directory=directory, all_ids=all_ids, id_to_metadata=id_to_metadata, filepaths=filepaths, headers=headers
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
            filepaths.append(file_directory / cast(str, metadata.name))

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

    def _get_id_to_metadata_map(self, all_ids: Sequence[dict]) -> dict[int, FileMetadata]:
        ids = [id["id"] for id in all_ids if "id" in id]
        external_ids = [id["externalId"] for id in all_ids if "externalId" in id]
        instance_ids = [id["instanceId"] for id in all_ids if "instanceId" in id]

        files_metadata = self.retrieve_multiple(ids=ids, external_ids=external_ids, instance_ids=instance_ids)

        id_to_metadata: dict[int, FileMetadata] = {}
        for f in files_metadata:
            id_to_metadata[f.id] = f

        return id_to_metadata

    def _download_files_to_directory(
        self,
        directory: Path,
        all_ids: Sequence[int],
        id_to_metadata: dict[int, FileMetadata],
        filepaths: list[Path],
        headers: dict | None = None,
    ) -> None:
        self._warn_on_duplicate_filenames(filepaths)
        tasks = [(directory, {"id": id}, filepath, headers) for id, filepath in zip(all_ids, filepaths)]
        tasks_summary = execute_tasks(self._process_file_download, tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: id_to_metadata[task[1]["id"]],
            str_format_element_fn=lambda metadata: metadata.id,
        )

    def _get_download_link(self, identifier: dict[str, int | str], headers: dict | None = None) -> str:
        return self._post(url_path="/files/downloadlink", json={"items": [identifier]}, headers=headers).json()[
            "items"
        ][0]["downloadUrl"]

    def _process_file_download(
        self,
        directory: Path,
        identifier: dict[str, int | str],
        file_path: Path,
        headers: dict | None = None,
    ) -> None:
        file_path_absolute = file_path.resolve()
        file_is_in_download_directory = directory.resolve() in file_path_absolute.parents
        if not file_is_in_download_directory:
            raise RuntimeError(f"Resolved file path '{file_path_absolute}' is not inside download directory")
        download_link = self._get_download_link(identifier, headers)
        self._download_file_to_path(download_link, file_path_absolute)

    def _download_file_to_path(self, download_link: str, path: Path, chunk_size: int = 2**21) -> None:
        with self._http_client_with_retry.request(
            "GET", download_link, accept="*/*", stream=True, timeout=self._config.file_transfer_timeout
        ) as r:
            with path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def download_to_path(
        self, path: Path | str, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> None:
        """Download a file to a specific target.

        Args:
            path (Path | str): The path in which to place the file.
            id (int | None): Id of of the file to download.
            external_id (str | None): External id of the file to download.
            instance_id (NodeId | None): Instance id of the file to download.

        Examples:

            Download a file by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.files.download_to_path("~/mydir/my_downloaded_file.txt", id=123)
        """
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}

        if isinstance(path, str):
            path = Path(path)
        if not path.parent.is_dir():
            raise NotADirectoryError(f"{path.parent} is not a directory")
        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        download_link = self._get_download_link(identifier, headers)
        self._download_file_to_path(download_link, path)

    def download_bytes(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> bytes:
        """Download a file as bytes.

        Args:
            id (int | None): Id of the file
            external_id (str | None): External id of the file
            instance_id (NodeId | None): Instance id of the file

        Examples:

            Download a file's content into memory::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> file_content = client.files.download_bytes(id=1)

        Returns:
            bytes: No description."""
        headers: dict | None = None
        if instance_id is not None:
            self._warn_alpha()
            headers = {"cdf-version": "alpha"}

        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        download_link = self._get_download_link(identifier, headers)
        return self._download_file(download_link)

    def _download_file(self, download_link: str) -> bytes:
        res = self._http_client_with_retry.request(
            "GET", download_link, accept="*/*", timeout=self._config.file_transfer_timeout
        )
        return res.content

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
        """`List files <https://developer.cognite.com/api#tag/Files/operation/advancedListFiles>`_

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

            List files metadata and filter on external id prefix::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> file_list = client.files.list(limit=5, external_id_prefix="prefix")

            Iterate over files metadata::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for file_metadata in client.files:
                ...     file_metadata # do something with the file metadata

            Iterate over chunks of files metadata to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for file_list in client.files(chunk_size=2500):
                ...     file_list # do something with the files

            Filter files based on labels::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelFilter
                >>> client = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
                >>> file_list = client.files.list(labels=my_label_filter)

            Filter files based on geoLocation::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GeoLocationFilter, GeometryFilter
                >>> client = CogniteClient()
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

        return self._list(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
        )
