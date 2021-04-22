import copy
import os
from typing import *
from typing.io import BinaryIO, TextIO

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


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"
    _LIST_CLASS = FileMetadataList

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: List[int] = None,
        asset_external_ids: List[str] = None,
        root_asset_ids: List[int] = None,
        root_asset_external_ids: List[str] = None,
        asset_subtree_ids: List[int] = None,
        asset_subtree_external_ids: List[str] = None,
        data_set_ids: List[int] = None,
        data_set_external_ids: List[str] = None,
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
    ) -> Generator[Union[FileMetadata, FileMetadataList], None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size (int, optional): Number of files to return in each chunk. Defaults to yielding one event a time.
            name (str): Name of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ..
            metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
            asset_ids (List[int]): Only include files that reference these specific asset IDs.
            asset_subtree_external_ids (List[str]): Only include files that reference these specific asset external IDs.
            root_asset_ids (List[int]): The IDs of the root assets that the related assets should be children of.
            root_asset_external_ids (List[str]): The external IDs of the root assets that the related assets should be children of.
            asset_subtree_ids (List[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (List[str]): List of asset subtrees external ids to filter on.
            data_set_ids (List[int]): Return only files in the specified data sets with these ids.
            data_set_external_ids (List[str]): Return only files in the specified data sets with these external ids.
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
        if root_asset_ids or root_asset_external_ids:
            root_asset_ids = self._process_ids(root_asset_ids, root_asset_external_ids, wrap_ids=True)
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids = self._process_ids(asset_subtree_ids, asset_subtree_external_ids, wrap_ids=True)
        if data_set_ids or data_set_external_ids:
            data_set_ids = self._process_ids(data_set_ids, data_set_external_ids, wrap_ids=True)

        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            root_asset_ids=root_asset_ids,
            asset_subtree_ids=asset_subtree_ids,
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
            data_set_ids=data_set_ids,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[FileMetadata, None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            FileMetadata: yields Files one by one.
        """
        return self.__call__()

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
        upload_url = returned_file_metadata.pop("uploadUrl")
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
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None
    ) -> FileMetadataList:
        """`Retrieve multiple file metadatas by id. <https://docs.cognite.com/api/v1/#operation/byIdsFiles>`_

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

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
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: List[int] = None,
        asset_external_ids: List[str] = None,
        root_asset_ids: List[int] = None,
        root_asset_external_ids: List[str] = None,
        asset_subtree_ids: List[int] = None,
        asset_subtree_external_ids: List[str] = None,
        data_set_ids: List[int] = None,
        data_set_external_ids: List[str] = None,
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
            asset_ids (List[int]): Only include files that reference these specific asset IDs.
            asset_subtree_external_ids (List[str]): Only include files that reference these specific asset external IDs.
            root_asset_ids (List[int]): The IDs of the root assets that the related assets should be children of.
            root_asset_external_ids (List[str]): The external IDs of the root assets that the related assets should be children of.
            asset_subtree_ids (List[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (List[str]): List of asset subtrees external ids to filter on.
            data_set_ids (List[int]): Return only files in the specified data sets with these ids.
            data_set_external_ids (List[str]): Return only files in the specified data sets with these external ids.
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
        if root_asset_ids or root_asset_external_ids:
            root_asset_ids = self._process_ids(root_asset_ids, root_asset_external_ids, wrap_ids=True)
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids = self._process_ids(asset_subtree_ids, asset_subtree_external_ids, wrap_ids=True)
        if data_set_ids or data_set_external_ids:
            data_set_ids = self._process_ids(data_set_ids, data_set_external_ids, wrap_ids=True)

        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            root_asset_ids=root_asset_ids,
            asset_subtree_ids=asset_subtree_ids,
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
            data_set_ids=data_set_ids,
        ).dump(camel_case=True)

        return self._list(method="POST", limit=limit, filter=filter)

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

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """`Delete files <https://docs.cognite.com/api/v1/#operation/deleteFiles>`_

        Args:
            id (Union[int, List[int]]): Id or list of ids
            external_id (Union[str, List[str]]): str or list of str

        Returns:
            None

        Examples:

            Delete files by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.files.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[FileMetadata, FileMetadataUpdate, List[Union[FileMetadata, FileMetadataUpdate]]]
    ) -> Union[FileMetadata, FileMetadataList]:
        """`Update files <https://docs.cognite.com/api/v1/#operation/updateFiles>`_
        Currently, a full replacement of labels on a file is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (Union[FileMetadata, FileMetadataUpdate, List[Union[FileMetadata, FileMetadataUpdate]]]): file(s) to update.

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
        return self._update_multiple(cls=FileMetadataList, resource_path=self._RESOURCE_PATH, items=item)

    def search(
        self, name: str = None, filter: Union[FileMetadataFilter, dict] = None, limit: int = 100
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
        return self._search(search={"name": name}, filter=filter, limit=limit)

    def upload(
        self,
        path: str,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        directory: str = None,
        asset_ids: List[int] = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        data_set_id: int = None,
        labels: List[Label] = None,
        geo_location: GeoLocation = None,
        security_categories: List[int] = None,
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
            asset_ids (List[int]): No description.
            data_set_id (int): ID of the data set.
            labels (List[Label]): A list of the labels associated with this resource item.
            geo_location (GeoLocation): The geographic metadata of the file.
            security_categories (List[int]): Security categories to attach to this file.
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
            tasks_summary = utils._concurrency.execute_tasks_concurrently(
                self._upload_file_from_path, tasks, self._config.max_workers
            )
            tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=lambda x: x[0].name)
            return FileMetadataList(tasks_summary.results)
        raise ValueError("path '{}' does not exist".format(path))

    def _upload_file_from_path(self, file: FileMetadata, file_path: str, overwrite: bool):
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
        asset_ids: List[int] = None,
        data_set_id: int = None,
        labels: List[Label] = None,
        geo_location: GeoLocation = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        security_categories: List[int] = None,
        overwrite: bool = False,
    ):
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
            asset_ids (List[int]): No description.
            data_set_id (int): Id of the data set.
            labels (List[Label]): A list of the labels associated with this resource item.
            geo_location (GeoLocation): The geographic metadata of the file.
            source_created_time (int): The timestamp for when the file was originally created in the source system.
            source_modified_time (int): The timestamp for when the file was last modified in the source system.
            security_categories (List[int]): Security categories to attach to this file.
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
        upload_url = returned_file_metadata.pop("uploadUrl")
        headers = {"X-Upload-Content-Type": file_metadata.mime_type}
        self._http_client_with_retry.request(
            "PUT", upload_url, data=content, timeout=self._config.file_transfer_timeout, headers=headers
        )
        return FileMetadata._load(returned_file_metadata)

    def download(
        self, directory: str, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> None:
        """`Download files by id or external id. <https://docs.cognite.com/api/v1/#operation/downloadLinks>`_

        This method will stream all files to disk, never keeping more than 2MB of a given file in memory.


        Args:
            directory (str): Directory to download the file(s) to.
            id (Union[int, List[int]], optional): Id or list of ids
            external_id (Union[str, List[str]), optional): External ID or list of external ids.

        Returns:
            None

        Examples:

            Download files by id and external id into directory 'my_directory'::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])
        """
        all_ids = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)
        id_to_metadata = self._get_id_to_metadata_map(all_ids)
        assert os.path.isdir(directory), "{} is not a directory".format(directory)
        self._download_files_to_directory(directory, all_ids, id_to_metadata)

    def _get_id_to_metadata_map(self, all_ids):
        ids = [id["id"] for id in all_ids if "id" in id]
        external_ids = [id["externalId"] for id in all_ids if "externalId" in id]

        files_metadata = self.retrieve_multiple(ids=ids, external_ids=external_ids)

        id_to_metadata = {}
        for f in files_metadata:
            id_to_metadata[f.id] = f
            id_to_metadata[f.external_id] = f

        return id_to_metadata

    def _download_files_to_directory(self, directory, all_ids, id_to_metadata):
        tasks = [(directory, id, id_to_metadata) for id in all_ids]
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
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

    def _process_file_download(self, directory, identifier, id_to_metadata):
        download_link = self._get_download_link(identifier)
        id = utils._auxiliary.unwrap_identifer(identifier)
        file_metadata = id_to_metadata[id]
        file_path = os.path.join(directory, file_metadata.name)
        self._download_file_to_path(download_link, file_path)

    def _download_file_to_path(self, download_link: str, path: str, chunk_size: int = 2 ** 21):
        with self._http_client_with_retry.request(
            "GET", download_link, stream=True, timeout=self._config.file_transfer_timeout
        ) as r:
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def download_to_path(self, path: str, id: int = None, external_id: str = None):
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
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        dirname = os.path.dirname(path)
        assert os.path.isdir(dirname), "{} is not a directory".format(dirname)
        identifier = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)[0]
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
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        identifier = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)[0]
        download_link = self._get_download_link(identifier)
        return self._download_file(download_link)

    def _download_file(self, download_link: str) -> bytes:
        res = self._http_client_with_retry.request("GET", download_link, timeout=self._config.file_transfer_timeout)
        return res.content
