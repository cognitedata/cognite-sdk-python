import os
from typing import *
from typing import Dict, List, Union

from cognite.client._utils import utils
from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteUpdate


# GenClass: FilesMetadata
class FileMetadata(CogniteResource):
    """No description.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project.
        name (str): No description.
        source (str): The source of the file.
        mime_type (str): File type. E.g. pdf, css, spreadsheet, ..
        metadata (Dict[str, Any]): Customizable extra data about the file. String key -> String value.
        asset_ids (List[int]): No description.
        id (int): Javascript friendly internal ID given to the object.
        uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        uploaded_at (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        id: int = None,
        uploaded: bool = None,
        uploaded_at: int = None,
        created_time: int = None,
        last_updated_time: int = None,
    ):
        self.external_id = external_id
        self.name = name
        self.source = source
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.id = id
        self.uploaded = uploaded
        self.uploaded_at = uploaded_at
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    # GenStop


# GenClass: FilesSearchFilter.filter
class FileMetadataFilter(CogniteFilter):
    """No description.

    Args:
        metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
        asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
        source (str): The source of this event.
        created_time (Dict[str, Any]): Range between two timestamps
        last_updated_time (Dict[str, Any]): Range between two timestamps
        external_id_prefix (str): External Id provided by client. Should be unique within the project.
    """

    def __init__(
        self,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
    ):
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix

    # GenStop


# GenUpdateClass: FileChange
class FileMetadataUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
        id (int): ID given by the API to the file.
        external_id (str): No description.
    """

    _UPDATE_ATTRIBUTES = ["source", "assetIds", "metadata", "mimeType", "name", "externalId"]

    def __init__(self, id: int = None, external_id: str = None):
        super().__init__(id=id, external_id=external_id)

    def name_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["name"] = {"setNull": True}
            return self
        self._update_object["name"] = {"set": value}
        return self

    def mime_type_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["mimeType"] = {"setNull": True}
            return self
        self._update_object["mimeType"] = {"set": value}
        return self

    def source_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["source"] = {"setNull": True}
            return self
        self._update_object["source"] = {"set": value}
        return self

    def metadata_add(self, value: Dict[str, Any]):
        self._update_object["metadata"] = {"add": value}
        return self

    def metadata_remove(self, value: List):
        self._update_object["metadata"] = {"remove": value}
        return self

    def metadata_set(self, value: Union[Dict[str, Any], None]):
        if value is None:
            self._update_object["metadata"] = {"setNull": True}
            return self
        self._update_object["metadata"] = {"set": value}
        return self

    def asset_ids_add(self, value: List):
        self._update_object["assetIds"] = {"add": value}
        return self

    def asset_ids_remove(self, value: List):
        self._update_object["assetIds"] = {"remove": value}
        return self

    def asset_ids_set(self, value: Union[List, None]):
        if value is None:
            self._update_object["assetIds"] = {"setNull": True}
            return self
        self._update_object["assetIds"] = {"set": value}
        return self

    def external_id_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["externalId"] = {"setNull": True}
            return self
        self._update_object["externalId"] = {"set": value}
        return self

    # GenStop


class FileMetadaList(CogniteResourceList):
    _RESOURCE = FileMetadata
    _UPDATE = FileMetadataUpdate


class FilesAPI(APIClient):
    RESOURCE_PATH = "/files"

    def __call__(
        self,
        chunk_size: int = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
    ) -> Generator[Union[FileMetadata, FileMetadaList], None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            chunk_size (int, optional): Number of files to return in each chunk. Defaults to yielding one event a time.
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project


        Yields:
            Union[FileMetadata, FileMetadataList]: yields FileMetadata one by one if chunk is not specified, else FileMetadataList objects.
        """
        filter = FileMetadataFilter(
            metadata, asset_ids, source, created_time, last_updated_time, external_id_prefix
        ).dump(camel_case=True)
        return self._list_generator(
            FileMetadaList, resource_path=self.RESOURCE_PATH, method="POST", chunk_size=chunk_size, filter=filter
        )

    def __iter__(self) -> Generator[FileMetadata, None, None]:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            FileMetadata: yields Files one by one.
        """
        return self.__call__()

    def get(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[FileMetadata, FileMetadaList]:
        """Get files by id

        Args:
            id (Union[int, List[int]], optional): Id or list of ids
            external_id(Union[str, List[str]], optional): str or list of str
        Returns:
            Union[FileMetadata, FileMetadataList]: The requested files
        """
        return self._retrieve_multiple(
            cls=FileMetadaList, resource_path=self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True
        )

    def list(
        self,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        limit: int = None,
    ) -> FileMetadaList:
        """List files

        Args:
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            limit (int, optional): Max number of files to return.

        Returns:
            FileMetadataList: The requested files.
        """
        filter = FileMetadataFilter(
            metadata, asset_ids, source, created_time, last_updated_time, external_id_prefix
        ).dump(camel_case=True)
        return self._list(
            cls=FileMetadaList, resource_path=self.RESOURCE_PATH, method="POST", limit=limit, filter=filter
        )

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete files

        Args:
            id (Union[int, List[int]]): Id or list of ids
            external_id (Union[str, List[str]]): str or list of str

        Returns:
            None
        """
        self._delete_multiple(resource_path=self.RESOURCE_PATH, wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[FileMetadata, FileMetadataUpdate, List[Union[FileMetadata, FileMetadataUpdate]]]
    ) -> Union[FileMetadata, FileMetadaList]:
        """Update files

        Args:
            item (Union[FileMetadata, FileMetadataUpdate, List[Union[FileMetadata, FileMetadataUpdate]]]): file(s) to update.

        Returns:
            Union[FileMetadata, FileMetadataList]: The updated files.
        """
        return self._update_multiple(cls=FileMetadaList, resource_path=self.RESOURCE_PATH, items=item)

    def upload(self, path: str, file_metadata: FileMetadata = None) -> Union[FileMetadata, FileMetadaList]:
        """Upload a file

        Args:
            path (str): Path to the file you wish to upload. If path is a directory, this method will upload all files in that directory.
            file_metadata (FileMetadata, optional): The file metadata

        Returns:
            Union[FileMetadata, FileMetadataList]: The file metadata of the uploaded file(s).
        """
        if os.path.isfile(path):
            if not file_metadata:
                file_metadata = FileMetadata(name=os.path.basename(path))
            return self._upload_file_from_path(file_metadata, path)
        elif os.path.isdir(path):
            assert file_metadata is None, "file_metadata must not be specified when 'path' is a directory"
            tasks = []
            for file_name in os.listdir(path):
                file_path = os.path.join(path, file_name)
                if os.path.isfile(file_path):
                    tasks.append((FileMetadata(name=file_name), file_path))
            file_metadata_list = utils.execute_tasks_concurrently(self._upload_file_from_path, tasks, self._max_workers)
            return FileMetadaList(file_metadata_list)
        raise ValueError("path '{}' does not exist".format(path))

    def _upload_file_from_path(self, file: FileMetadata, file_path: str):
        with open(file_path, "rb") as f:
            file_metadata = self.upload_from_memory(file, f.read())
        return file_metadata

    def upload_from_memory(self, file_metadata: FileMetadata, content: Union[str, bytes]):
        """Upload a file from memory

        Args:
            file_metadata (FileMetadata): The file metadata.
            content (Union[str, bytes]): The content to upload.
        """
        url_path = self.RESOURCE_PATH + "/initupload"

        res = self._post(url_path=url_path, json=file_metadata.dump(camel_case=True))
        returned_file_metadata = res.json()["data"]
        upload_url = returned_file_metadata.pop("uploadUrl")
        headers = {"X-Upload-Content-Type": file_metadata.mime_type, "content-length": str(len(content))}
        self._request_session.put(upload_url, data=content, headers=headers)
        return FileMetadata._load(returned_file_metadata)

    def download(
        self, directory: str, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> None:
        """Download files by id or external id.

        Args:
            directory (str): Directory to download the file(s) to.
            id (Union[int, List[int]], optional): Id or list of ids
            external_id (Union[str, List[str]), optional): External ID or list of external ids.

        Returns:
            None
        """
        all_ids = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)
        res = self._post(url_path="/files/download", json={"items": all_ids})

        download_tasks = []
        for item in res.json()["data"]["items"]:
            dl_link = item["link"]
            if "id" in item:
                path = os.path.join(directory, str(item["id"]))
                task = (dl_link, path)
            elif "externalId" in item:
                path = os.path.join(directory, item["externalId"])
                task = (dl_link, path)
            else:
                raise AssertionError("FileMetadata download does not contain 'id' or 'externalId'")
            download_tasks.append(task)

        utils.execute_tasks_concurrently(self._download_file_to_path, download_tasks, self._max_workers)

    def download_to_memory(self, id: int = None, external_id: str = None) -> bytes:
        """Download a file to memory.

        Args:
            id (int, optional): Id of the file
            external_id (str, optional): External id of the file
        """
        utils.assert_exactly_one_of_id_or_external_id(id, external_id)
        all_ids = self._process_ids(ids=id, external_ids=external_id, wrap_ids=True)
        res = self._post(url_path="/files/download", json={"items": all_ids})
        dl_link = res.json()["data"]["items"][0]["link"]
        return self._download_file(dl_link)

    def _download_file_to_path(self, download_link: str, path: str, chunk_size: int = 2 ** 21):
        with self._request_session.get(download_link, stream=True) as r:
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def _download_file(self, download_link: str) -> bytes:
        res = self._request_session.get(download_link)
        return res.content
