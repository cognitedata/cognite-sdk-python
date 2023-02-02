from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Sequence, TextIO, Tuple, Union, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    FileAggregate,
    FileMetadata,
    FileMetadataFilter,
    FileMetadataList,
    FileMetadataUpdate,
    GeoLocation,
    GeoLocationFilter,
    Label,
    LabelFilter,
    TimestampRange,
)
from cognite.client.utils._identifier import Identifier, IdentifierSequence


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        source_created_time: Union[Dict[str, Any], TimestampRange] = None,
        source_modified_time: Union[Dict[str, Any], TimestampRange] = None,
        uploaded_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        directory_prefix: str = None,
        uploaded: bool = None,
        limit: int = None,
    ) -> Union[Iterator[FileMetadata], Iterator[FileMetadataList]]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size (int, optional): Number of files to return in each chunk. Defaults to yielding one event a time.
            name (str): Name of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ..
            metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
            asset_ids (Sequence[int]): Only include files that reference these specific asset IDs.
            asset_subtree_external_ids (Sequence[str]): Only include files that reference these specific asset external IDs.
            root_asset_external_ids (Sequence[str]): The external IDs of the root assets that the related assets should be children of.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            data_set_ids (Sequence[int]): Return only files in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only files in the specified data sets with these external ids.
            labels (LabelFilter): Return only the files matching the specified label(s).
            geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
            source (str): The source of this event.
            source_created_time (Union[Dict[str, Any], TimestampRange]): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time (Union[Dict[str, Any], TimestampRange]): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            uploaded_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
            directory_prefix (str): Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit (int, optional): Maximum number of files to return. Defaults to return all items.

        Yields:
            Union[FileMetadata, FileMetadataList]: yields FileMetadata one by one if chunk is not specified, else FileMetadataList objects.
        """
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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
        )

    def __iter__(self) -> Iterator[FileMetadata]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            FileMetadata: yields Files one by one.
        """
        return cast(Iterator[FileMetadata], self())

    def create(self, file_metadata: FileMetadata, overwrite: bool = False) -> Tuple[FileMetadata, str]:
        """Create file without uploading content.

        Args:
            file_metadata (FileMetaData): File metadata for the file to create.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field,
                fields for the file found for externalId can be overwritten. The default setting is false.
                If metadata is included in the request body, all of the original metadata will be overwritten.
                File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body.
                Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            Tuple[FileMetaData, str]: Tuple containing the file metadata and upload url of the created file.

        Examples:

            Create a file::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadata
                >>> c = CogniteClient()
                >>> file_metadata = FileMetadata(name="MyFile")
                >>> res = c.files.create(file_metadata)

        """

        res = self._post(
            url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        file_metadata = FileMetadata._load(returned_file_metadata)
        return (file_metadata, upload_url)

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[FileMetadata]:
        """`Retrieve a single file metadata by id. <https://docs.cognite.com/api/v1/#operation/getFileByInternalId>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[FileMetadata]: Requested file metadata or None if it does not exist.

        Examples:

            Get file metadata by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve(id=1)

            Get file metadata by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=FileMetadataList, resource_cls=FileMetadata, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        """`Retrieve multiple file metadatas by id. <https://docs.cognite.com/api/v1/#operation/byIdsFiles>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FileMetadataList: The requested file metadatas.

        Examples:

            Get file metadatas by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve_multiple(ids=[1, 2, 3])

            Get file_metadatas by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        source_created_time: Union[Dict[str, Any], TimestampRange] = None,
        source_modified_time: Union[Dict[str, Any], TimestampRange] = None,
        uploaded_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        directory_prefix: str = None,
        uploaded: bool = None,
        limit: int = 25,
    ) -> FileMetadataList:
        """`List files <https://docs.cognite.com/api/v1/#operation/advancedListFiles>`_

        Args:
            name (str): Name of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ..
            metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
            asset_ids (Sequence[int]): Only include files that reference these specific asset IDs.
            asset_subtree_external_ids (Sequence[str]): Only include files that reference these specific asset external IDs.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            data_set_ids (Sequence[int]): Return only files in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only files in the specified data sets with these external ids.
            labels (LabelFilter): Return only the files matching the specified label filter(s).
            geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
            source (str): The source of this event.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            uploaded_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            source_created_time (Union[Dict[str, Any], TimestampRange]): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
            source_modified_time (Union[Dict[str, Any], TimestampRange]): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
            directory_prefix (str): Filter by this (case-sensitive) prefix for the directory provided by the client.
            uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
            limit (int, optional): Max number of files to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FileMetadataList: The requested files.

        Examples:

            List files metadata and filter on external id prefix::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> file_list = c.files.list(limit=5, external_id_prefix="prefix")

            Iterate over files metadata::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for file_metadata in c.files:
                ...     file_metadata # do something with the file metadata

            Iterate over chunks of files metadata to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for file_list in c.files(chunk_size=2500):
                ...     file_list # do something with the files

            Filter files based on labels::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelFilter
                >>> c = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
                >>> file_list = c.files.list(labels=my_label_filter)

            Filter files based on geoLocation::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GeoLocationFilter, GeometryFilter
                >>> c = CogniteClient()
                >>> my_geo_location_filter = GeoLocationFilter(relation="intersects", shape=GeometryFilter(type="Point", coordinates=[35,10]))
                >>> file_list = c.files.list(geo_location=my_geo_location_filter)
        """
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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
            list_cls=FileMetadataList, resource_cls=FileMetadata, method="POST", limit=limit, filter=filter
        )

    def aggregate(self, filter: Union[FileMetadataFilter, Dict] = None) -> List[FileAggregate]:
        """`Aggregate files <https://docs.cognite.com/api/v1/#operation/aggregateFiles>`_

        Args:
            filter (Union[FileMetadataFilter, Dict]): Filter on file metadata filter with exact match

        Returns:
            List[FileAggregate]: List of file aggregates

        Examples:

            List files metadata and filter on external id prefix::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_uploaded = c.files.aggregate(filter={"uploaded": True})
        """

        return self._aggregate(filter=filter, cls=FileAggregate)

    def delete(self, id: Union[int, Sequence[int]] = None, external_id: Union[str, Sequence[str]] = None) -> None:
        """`Delete files <https://docs.cognite.com/api/v1/#operation/deleteFiles>`_

        Args:
            id (Union[int, Sequence[int]]): Id or list of ids
            external_id (Union[str, Sequence[str]]): str or list of str

        Returns:
            None

        Examples:

            Delete files by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.files.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    @overload
    def update(self, item: Union[FileMetadata, FileMetadataUpdate]) -> FileMetadata:
        ...

    @overload
    def update(self, item: Sequence[Union[FileMetadata, FileMetadataUpdate]]) -> FileMetadataList:
        ...

    def update(
        self, item: Union[FileMetadata, FileMetadataUpdate, Sequence[Union[FileMetadata, FileMetadataUpdate]]]
    ) -> Union[FileMetadata, FileMetadataList]:
        """`Update files <https://docs.cognite.com/api/v1/#operation/updateFiles>`_
        Currently, a full replacement of labels on a file is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (Union[FileMetadata, FileMetadataUpdate, Sequence[Union[FileMetadata, FileMetadataUpdate]]]): file(s) to update.

        Returns:
            Union[FileMetadata, FileMetadataList]: The updated files.

        Examples:

            Update file metadata that you have fetched. This will perform a full update of the file metadata::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> file_metadata = c.files.retrieve(id=1)
                >>> file_metadata.description = "New description"
                >>> res = c.files.update(file_metadata)

            Perform a partial update on file metadata, updating the source and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> c = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).source.set("new source").metadata.add({"key": "value"})
                >>> res = c.files.update(my_update)

            Attach labels to a files::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> c = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).labels.add(["PUMP", "VERIFIED"])
                >>> res = c.files.update(my_update)

            Detach a single label from a file::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import FileMetadataUpdate
                >>> c = CogniteClient()
                >>> my_update = FileMetadataUpdate(id=1).labels.remove("PUMP")
                >>> res = c.files.update(my_update)
        """
        return self._update_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            update_cls=FileMetadataUpdate,
            resource_path=self._RESOURCE_PATH,
            items=item,
        )

    def search(
        self, name: str = None, filter: Optional[Union[FileMetadataFilter, dict]] = None, limit: int = 100
    ) -> FileMetadataList:
        """`Search for files. <https://docs.cognite.com/api/v1/#operation/searchFiles>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str, optional): Prefix and fuzzy search on name.
            filter (Union[FileMetadataFilter, dict], optional): Filter to apply. Performs exact match on these fields.
            limit (int, optional): Max number of results to return.

        Returns:
            FileMetadataList: List of requested files metadata.

        Examples:

            Search for a file::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.search(name="some name")

            Search for an asset with an attached label:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["WELL LOG"])
                >>> res = c.assets.search(name="xyz",filter=FileMetadataFilter(labels=my_label_filter))
        """
        return self._search(list_cls=FileMetadataList, search={"name": name}, filter=filter or {}, limit=limit)

    def upload(
        self,
        path: str,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        directory: str = None,
        asset_ids: Sequence[int] = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        data_set_id: int = None,
        labels: Sequence[Label] = None,
        geo_location: GeoLocation = None,
        security_categories: Sequence[int] = None,
        recursive: bool = False,
        overwrite: bool = False,
    ) -> Union[FileMetadata, FileMetadataList]:
        """`Upload a file <https://docs.cognite.com/api/v1/#operation/initFileUpload>`_

        Args:
            path (str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            external_id (str): The external ID provided by the client. Must be unique within the project.
            name (str): Name of the file.
            source (str): The source of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ...
            metadata (Dict[str, str]): Customizable extra data about the file. String key -> String value.
            directory (str): The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids (Sequence[int]): No description.
            data_set_id (int): ID of the data set.
            labels (Sequence[Label]): A list of the labels associated with this resource item.
            geo_location (GeoLocation): The geographic metadata of the file.
            security_categories (Sequence[int]): Security categories to attach to this file.
            source_created_time (int): The timestamp for when the file was originally created in the source system.
            source_modified_time (int): The timestamp for when the file was last modified in the source system.
            recursive (bool): If path is a directory, upload all contained files recursively.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field,
                fields for the file found for externalId can be overwritten. The default setting is false.
                If metadata is included in the request body, all of the original metadata will be overwritten.
                The actual file will be overwritten after successful upload. If there is no successful upload, the
                current file contents will be kept.
                File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body.
                Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Returns:
            Union[FileMetadata, FileMetadataList]: The file metadata of the uploaded file(s).

        Examples:

            Upload a file in a given path::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.upload("/path/to/file", name="my_file")

            If name is omitted, this method will use the name of the file

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.upload("/path/to/file")

            You can also upload all files in a directory by setting path to the path of a directory::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.upload("/path/to/my/directory")

            Upload a file with a label::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Label
                >>> c = CogniteClient()
                >>> res = c.files.upload("/path/to/file", name="my_file", labels=[Label(external_id="WELL LOG")])

            Upload a file with a geo_location::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GeoLocation, Geometry
                >>> c = CogniteClient()
                >>> geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
                >>> res = c.files.upload("/path/to/file", geo_location=GeoLocation(type="Feature", geometry=geometry))

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
            tasks_summary = utils._concurrency.execute_tasks(
                self._upload_file_from_path, tasks, self._config.max_workers
            )
            tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=lambda x: x[0].name)
            return FileMetadataList(tasks_summary.results)
        raise ValueError(f"The path '{path}' does not exist")

    def _upload_file_from_path(self, file: FileMetadata, file_path: str, overwrite: bool) -> FileMetadata:
        with open(file_path, "rb") as fh:
            file_metadata = self.upload_bytes(fh, overwrite=overwrite, **file.dump())
        return file_metadata

    def upload_bytes(
        self,
        content: Union[str, bytes, TextIO, BinaryIO],
        name: str,
        external_id: str = None,
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
        overwrite: bool = False,
    ) -> FileMetadata:
        """Upload bytes or string.

        You can also pass a file handle to content.

        Args:
            content (Union[str, bytes, TextIO, BinaryIO]): The content to upload.
            name (str): Name of the file.
            external_id (str): The external ID provided by the client. Must be unique within the project.
            source (str): The source of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf,...
            metadata (Dict[str, str]): Customizable extra data about the file. String key -> String value.
            directory (str): The directory to be associated with this file. Must be an absolute, unix-style path.
            asset_ids (Sequence[int]): No description.
            data_set_id (int): Id of the data set.
            labels (Sequence[Label]): A list of the labels associated with this resource item.
            geo_location (GeoLocation): The geographic metadata of the file.
            source_created_time (int): The timestamp for when the file was originally created in the source system.
            source_modified_time (int): The timestamp for when the file was last modified in the source system.
            security_categories (Sequence[int]): Security categories to attach to this file.
            overwrite (bool): If 'overwrite' is set to true, and the POST body content specifies a 'externalId' field,
                fields for the file found for externalId can be overwritten. The default setting is false.
                If metadata is included in the request body, all of the original metadata will be overwritten.
                The actual file will be overwritten after successful upload. If there is no successful upload, the
                current file contents will be kept.
                File-Asset mappings only change if explicitly stated in the assetIds field of the POST json body.
                Do not set assetIds in request body if you want to keep the current file-asset mappings.

        Examples:

            Upload a file from memory::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.upload_bytes(b"some content", name="my_file", asset_ids=[1,2,3])

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

        res = self._post(
            url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        headers = {"X-Upload-Content-Type": file_metadata.mime_type}
        self._http_client_with_retry.request(
            "PUT", upload_url, data=content, timeout=self._config.file_transfer_timeout, headers=headers
        )
        return FileMetadata._load(returned_file_metadata)

    def retrieve_download_urls(
        self,
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
        extended_expiration: bool = False,
    ) -> Dict[Union[int, str], str]:
        """Get download links by id or external id

        Args:
            id (Union[int, Sequence[int]]): Id or list of ids.
            external_id (Union[str, Sequence[str]]): External id or list of external ids.
            extended_expiration (bool): Extend expiration time of download url to 1 hour. Defaults to false.

        Returns:
            Dict[Union[str, int], str]: Dictionary containing download urls.
        """
        batch_size = 100
        id_batches = [seq.as_dicts() for seq in IdentifierSequence.load(id, external_id).chunked(batch_size)]
        query_params = {}
        if extended_expiration:
            query_params["extendedExpiration"] = True
        tasks = [
            dict(url_path="/files/downloadlink", json={"items": id_batch}, params=query_params)
            for id_batch in id_batches
        ]
        tasks_summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        results = tasks_summary.joined_results(unwrap_fn=lambda res: res.json()["items"])
        return {result.get("id") or result["externalId"]: result["downloadUrl"] for result in results}

    def download(
        self,
        directory: Union[str, Path],
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
    ) -> None:
        """`Download files by id or external id. <https://docs.cognite.com/api/v1/#operation/downloadLinks>`_

        This method will stream all files to disk, never keeping more than 2MB in memory per worker.
        The files will be stored in the provided directory using the name retrieved from the file metadata in CDF.


        Args:
            directory (str): Directory to download the file(s) to.
            id (Union[int, Sequence[int]], optional): Id or list of ids
            external_id (Union[str, Sequence[str]), optional): External ID or list of external ids.

        Returns:
            None

        Examples:

            Download files by id and external id into directory 'my_directory'::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])
        """
        if isinstance(directory, str):
            directory = Path(directory)
        all_ids = IdentifierSequence.load(id, external_id).as_dicts()
        id_to_metadata = self._get_id_to_metadata_map(all_ids)
        assert directory.is_dir(), f"{directory} is not a directory"
        self._download_files_to_directory(directory, all_ids, id_to_metadata)

    def _get_id_to_metadata_map(self, all_ids: Sequence[Dict]) -> Dict[Union[str, int], FileMetadata]:
        ids = [id["id"] for id in all_ids if "id" in id]
        external_ids = [id["externalId"] for id in all_ids if "externalId" in id]

        files_metadata = self.retrieve_multiple(ids=ids, external_ids=external_ids)

        id_to_metadata = {}
        for f in files_metadata:
            id_to_metadata[f.id] = f
            id_to_metadata[f.external_id] = f

        return id_to_metadata

    def _download_files_to_directory(
        self,
        directory: Path,
        all_ids: Sequence[Dict[str, Union[int, str]]],
        id_to_metadata: Dict[Union[str, int], FileMetadata],
    ) -> None:
        tasks = [(directory, id, id_to_metadata) for id in all_ids]
        tasks_summary = utils._concurrency.execute_tasks(
            self._process_file_download, tasks, max_workers=self._config.max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: id_to_metadata[utils._auxiliary.unwrap_identifer(task[1])],
            str_format_element_fn=lambda metadata: metadata.id,
        )

    def _get_download_link(self, identifier: Dict[str, Union[int, str]]) -> str:
        return self._post(url_path="/files/downloadlink", json={"items": [identifier]}).json()["items"][0][
            "downloadUrl"
        ]

    def _process_file_download(
        self,
        directory: Path,
        identifier: Dict[str, Union[int, str]],
        id_to_metadata: Dict[Union[str, int], FileMetadata],
    ) -> None:
        id = utils._auxiliary.unwrap_identifer(identifier)
        file_metadata = id_to_metadata[id]
        file_path = (directory / cast(str, file_metadata.name)).resolve()
        file_is_in_download_directory = directory.resolve() in file_path.parents
        if not file_is_in_download_directory:
            raise RuntimeError(f"Resolved file path '{file_path}' is not inside download directory")
        download_link = self._get_download_link(identifier)
        self._download_file_to_path(download_link, file_path)

    def _download_file_to_path(self, download_link: str, path: Path, chunk_size: int = 2**21) -> None:
        with self._http_client_with_retry.request(
            "GET", download_link, stream=True, timeout=self._config.file_transfer_timeout
        ) as r:
            with path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def download_to_path(self, path: Union[Path, str], id: int = None, external_id: str = None) -> None:
        """Download a file to a specific target.

        Args:
            path (str): The path in which to place the file.
            id (int): Id of of the file to download.
            external_id (str): External id of the file to download.

        Returns:
            None

        Examples:

            Download a file by id:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.files.download_to_path("~/mydir/my_downloaded_file.txt", id=123)
        """
        if isinstance(path, str):
            path = Path(path)
        assert path.parent.is_dir(), f"{path.parent} is not a directory"
        identifier = Identifier.of_either(id, external_id).as_dict()
        download_link = self._get_download_link(identifier)
        self._download_file_to_path(download_link, path)

    def download_bytes(self, id: int = None, external_id: str = None) -> bytes:
        """Download a file as bytes.

        Args:
            id (int, optional): Id of the file
            external_id (str, optional): External id of the file

        Examples:

            Download a file's content into memory::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> file_content = c.files.download_bytes(id=1)
        """
        identifier = Identifier.of_either(id, external_id).as_dict()
        download_link = self._get_download_link(identifier)
        return self._download_file(download_link)

    def _download_file(self, download_link: str) -> bytes:
        res = self._http_client_with_retry.request("GET", download_link, timeout=self._config.file_transfer_timeout)
        return res.content
