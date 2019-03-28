# -*- coding: utf-8 -*-
import os
import warnings
from typing import *
from typing import Dict, List, Union

from cognite.client._utils import utils
from cognite.client._utils.api_client import APIClient
from cognite.client._utils.resource_base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteUpdate


# GenClass: FilesMetadata
class File(CogniteResource):
    """No description.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project
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


class FileList(CogniteResourceList):
    _RESOURCE = File


# GenClass: FileFilter.filter
class FileFilter(CogniteFilter):
    """No description.

    Args:
        metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
        asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
        source (str): The source of this event.
        created_time (Dict[str, Any]): Range between two timestamps
        last_updated_time (Dict[str, Any]): Range between two timestamps
        external_id_prefix (str): External Id provided by client. Should be unique within the project
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
class FileUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
        id (int): ID given by the API to the file.
        external_id (str): No description.
    """

    def __init__(self, id: int = None, external_id: str = None):
        self.id = id
        self.external_id = external_id
        self._update_object = {}

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


class FilesAPI(APIClient):
    RESOURCE_PATH = "/files"

    def __call__(self, filter: FileFilter = None, chunk_size: int = None) -> Generator:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Args:
            filter (FileFilter, optional): Filter to apply.
            chunk_size (int, optional): Number of files to return in each chunk. Defaults to yielding one event a time.

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._list_generator(
            FileList, resource_path=self.RESOURCE_PATH, method="POST", chunk=chunk_size, filter=filter
        )

    def __iter__(self) -> Generator:
        """Iterate over files

        Fetches file metadata objects as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            File: yields Files one by one.
        """
        return self.__call__()

    def get(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> Union[File, FileList]:
        """Get files by id

        Args:
            id (Union[int, List[int]], optional): Id or list of ids
            external_id(Union[str, List[str]], optional): str or list of str
        Returns:
            Union[File, FileList]: The requested files
        """
        return self._retrieve_multiple(
            cls=FileList, resource_path=self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True
        )

    def list(self, filter: FileFilter = None, limit: int = None) -> FileList:
        """List files

        Args:
            filter (FileFilter, optional): Filter to apply.
            limit (int, optional): Max number of files to return.

        Returns:
            FileList: The requested files.
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._list(cls=FileList, resource_path=self.RESOURCE_PATH, method="POST", limit=limit, filter=filter)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete files

        Args:
            id (Union[int, List[int]]): Id or list of ids
            external_id (Union[str, List[str]]): str or list of str

        Returns:
            None
        """
        self._delete_multiple(resource_path=self.RESOURCE_PATH, wrap_ids=True, ids=id, external_ids=external_id)

    def update(self, item: Union[File, FileUpdate, List[Union[File, FileUpdate]]]) -> Union[File, FileList]:
        """Update files

        Args:
            item (Union[File, FileUpdate, List[Union[File, FileUpdate]]]): File(s) to update.

        Returns:
            Union[File, FileList]: The updated files.
        """
        return self._update_multiple(cls=FileList, resource_path=self.RESOURCE_PATH, items=item)

    def upload(self, file: File, path: str) -> File:
        """Upload a file

        Args:
            file (File): The file metadata
            path (str): Path to the file you wish to upload

        Returns:
            File (File): The file metadata of the uploaded file.
        """
        with open(path, "rb") as f:
            file_metadata = self.upload_from_memory(file, f.read())
        return file_metadata

    def upload_from_memory(self, file: File, content: Union[str, bytes]):
        """Upload a file from memory

        Args:
            file (File): The file metadata.
            content (Union[str, bytes]): The content to upload.
        """
        url_path = self.RESOURCE_PATH + "/initupload"

        res = self._post(url_path=url_path, json=file.dump())
        file_metadata = res.json()["data"]
        upload_url = file_metadata.pop("uploadUrl")

        if not file.mime_type:
            warning = "File.mime_type should be specified when uploading the file."
            warnings.warn(warning)
        headers = {"X-Upload-Content-Type": file.mime_type, "content-length": str(len(content))}
        self._request_session.put(upload_url, data=content, headers=headers)
        return File._load(file_metadata)

    def download(
        self, directory: str, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> None:
        """Download files by id or external id.

        Args:
            directory (str): Directory to download the file(s) to.
            id (Union[int, List[int]], optional), optional): Id of the file
            external_id (str, optional): External ID of the file.

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
                raise AssertionError("File download does not contain 'id' or 'externalId'")
            download_tasks.append(task)

        utils.execute_tasks_concurrently(self._download_file_to_path, download_tasks, self._num_of_workers)

    def download_to_memory(self, id: int = None, external_id: str = None) -> bytes:
        """Download a file to memory.

        Args:
            id (int, optional): Id of the file
            external_id (str, optional): External id of the file
        """
        assert (id or external_id) and not (id and external_id), "Exactly one of id and external_id must be specified"
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
