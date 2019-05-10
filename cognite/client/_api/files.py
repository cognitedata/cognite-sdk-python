import os
import queue
import threading
from typing import *
from typing import Dict, List

from cognite.client._api_client import APIClient
from cognite.client.data_classes import FileMetadata, FileMetadataFilter, FileMetadataList, FileMetadataUpdate
from cognite.client.utils import _utils as utils


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"
    _LIST_CLASS = FileMetadataList

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        asset_subtrees: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        uploaded_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        uploaded: bool = None,
    ) -> Generator[Union[FileMetadata, FileMetadataList], None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size (int, optional): Number of files to return in each chunk. Defaults to yielding one event a time.
            name (str): Name of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ..
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            asset_ids (List[int]): Only include files that reference these specific asset IDs.
            asset_subtrees (List[int]): Only include files that reference these asset Ids or any  sub-nodes of the specified asset Ids.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            uploaded_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
            uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.

        Yields:
            Union[FileMetadata, FileMetadataList]: yields FileMetadata one by one if chunk is not specified, else FileMetadataList objects.
        """
        filter = FileMetadataFilter(
            name,
            mime_type,
            metadata,
            asset_ids,
            asset_subtrees,
            source,
            created_time,
            last_updated_time,
            uploaded_time,
            external_id_prefix,
            uploaded,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter)

    def __iter__(self) -> Generator[FileMetadata, None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            FileMetadata: yields Files one by one.
        """
        return self.__call__()

    def retrieve(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[FileMetadata, FileMetadataList]:
        """Get files by id

        Args:
            id (Union[int, List[int]], optional): Id or list of ids
            external_id(Union[str, List[str]], optional): str or list of str

        Returns:
            Union[FileMetadata, FileMetadataList]: The requested files

        Examples:

            Get file metadata by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve(id=1)

            Get file meta data by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.retrieve(external_id=["1", "abc"])
        """
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def list(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        asset_subtrees: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        uploaded_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        uploaded: bool = None,
        limit: int = 25,
    ) -> FileMetadataList:
        """List files

        Args:
            name (str): Name of the file.
            mime_type (str): File type. E.g. text/plain, application/pdf, ..
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            asset_ids (List[int]): Only include files that reference these specific asset IDs.
            asset_subtrees (List[int]): Only include files that reference these asset Ids or any  sub-nodes of the specified asset Ids.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            uploaded_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
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
        """
        filter = FileMetadataFilter(
            name,
            mime_type,
            metadata,
            asset_ids,
            asset_subtrees,
            source,
            created_time,
            last_updated_time,
            uploaded_time,
            external_id_prefix,
            uploaded,
        ).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete files

        Args:
            id (Union[int, List[int]]): Id or list of ids
            external_id (Union[str, List[str]]): str or list of str

        Returns:
            None

        Examples:

            Delete files by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.files.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[FileMetadata, FileMetadataUpdate, List[Union[FileMetadata, FileMetadataUpdate]]]
    ) -> Union[FileMetadata, FileMetadataList]:
        """Update files

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
        """
        return self._update_multiple(cls=FileMetadataList, resource_path=self._RESOURCE_PATH, items=item)

    def search(
        self, name: str = None, filter: Union[FileMetadataFilter, dict] = None, limit: int = None
    ) -> FileMetadataList:
        """Search for files.

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
        """
        return self._search(search={"name": name}, filter=filter, limit=limit)

    def upload(
        self,
        path: str,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        recursive: bool = False,
    ) -> Union[FileMetadata, FileMetadataList]:
        """Upload a file

        Args:
            path (str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            external_id (str): External Id provided by client. Should be unique within the project.
            name (str): No description.
            source (str): The source of the file.
            mime_type (str): File type. E.g. pdf, css, spreadsheet, ..
            metadata (Dict[str, Any]): Customizable extra data about the file. String key -> String value.
            asset_ids (List[int]): No description.
            recursive (bool): If path is a directory, upload all contained files recursively.

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

        """
        file_metadata = FileMetadata(
            name=name,
            external_id=external_id,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
        )
        if os.path.isfile(path):
            if not name:
                file_metadata.name = os.path.basename(path)
            return self._upload_file_from_path(file_metadata, path)
        elif os.path.isdir(path):
            tasks = []
            if recursive:
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        basename = os.path.basename(file_path)
                        tasks.append((FileMetadata(name=basename), file_path))
            else:
                for file_name in os.listdir(path):
                    file_path = os.path.join(path, file_name)
                    if os.path.isfile(file_path):
                        tasks.append((FileMetadata(name=file_name), file_path))
            tasks_summary = utils.execute_tasks_concurrently(self._upload_file_from_path, tasks, self._max_workers)
            tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=lambda x: x[0].name)
            return FileMetadataList(tasks_summary.results)
        raise ValueError("path '{}' does not exist".format(path))

    def _upload_file_from_path(self, file: FileMetadata, file_path: str):
        with open(file_path, "rb") as f:
            file_metadata = self.upload_bytes(f.read(), **file.dump(camel_case=True))
        return file_metadata

    def upload_bytes(
        self,
        content: Union[str, bytes],
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
    ):
        """Upload bytes or string.

        Args:
            content (Union[str, bytes]): The content to upload.
            external_id (str): External Id provided by client. Should be unique within the project.
            name (str): No description.
            source (str): The source of the file.
            mime_type (str): File type. E.g. pdf, css, spreadsheet, ..
            metadata (Dict[str, Any]): Customizable extra data about the file. String key -> String value.
            asset_ids (List[int]): No description.

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
            asset_ids=asset_ids,
        )
        url_path = self._RESOURCE_PATH + "/initupload"

        res = self._post(url_path=url_path, json=file_metadata.dump(camel_case=True))
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata.pop("uploadUrl")
        headers = {"X-Upload-Content-Type": file_metadata.mime_type, "content-length": str(len(content))}
        self._request_session.put(upload_url, data=content, headers=headers)
        return FileMetadata._load(returned_file_metadata)

    def download(
        self, directory: str, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> None:
        """Download files by id or external id.

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
                >>> res = c.files.download(directory="my_directory", id=[1,2,3], external_id=["abc", "def"])
        """
        all_ids = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)
        id_to_metadata = self._get_id_to_metadata_map(all_ids)
        self._download_files_to_directory(directory, all_ids, id_to_metadata)

    def _get_id_to_metadata_map(self, all_ids):
        ids = [id["id"] for id in all_ids if "id" in id]
        external_ids = [id["externalId"] for id in all_ids if "externalId" in id]

        files_metadata = self.retrieve(id=ids, external_id=external_ids)

        id_to_metadata = {}
        for f in files_metadata:
            id_to_metadata[f.id] = f
            id_to_metadata[f.external_id] = f

        return id_to_metadata

    def _download_files_to_directory(self, directory, all_ids, id_to_metadata):
        tasks = [(directory, id, id_to_metadata) for id in all_ids]
        tasks_summary = utils.execute_tasks_concurrently(
            self._process_file_download, tasks, max_workers=self._max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: id_to_metadata[utils.unwrap_identifer(task[1])],
            str_format_element_fn=lambda metadata: metadata.id,
        )

    def _process_file_download(self, directory, identifier, id_to_metadata):
        download_link = self._post(url_path="/files/downloadlink", json={"items": [identifier]}).json()["items"][0][
            "downloadUrl"
        ]
        id = utils.unwrap_identifer(identifier)
        file_metadata = id_to_metadata[id]
        file_path = os.path.join(directory, file_metadata.name)
        self._download_file_to_path(download_link, file_path)

    def _download_file_to_path(self, download_link: str, path: str, chunk_size: int = 2 ** 21):
        with self._request_session.get(download_link, stream=True) as r:
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

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
        utils.assert_exactly_one_of_id_or_external_id(id, external_id)
        all_ids = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)
        res = self._post(url_path="/files/downloadlink", json={"items": all_ids})
        dl_link = res.json()["items"][0]["downloadUrl"]
        return self._download_file(dl_link)

    def _download_file(self, download_link: str) -> bytes:
        res = self._request_session.get(download_link)
        return res.content
