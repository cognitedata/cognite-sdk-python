# -*- coding: utf-8 -*-
import os
import warnings
from copy import copy
from typing import Dict, List, Union

import pandas as pd

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResponse


class FileInfoResponse(CogniteResponse):
    """File Info Response Object.

    Args:
        id (int):               ID given by the API to the file.
        file_name (str):        File name. Max length is 256.
        directory (str):        Directory containing the file. Max length is 512.
        source (dict):          Source that this file comes from. Max length is 256.
        file_type (str):        File type. E.g. pdf, css, spreadsheet, .. Max length is 64.
        metadata (dict):        Customized data about the file.
        asset_ids (list[str]):  Names of assets related to this file.
        uploaded (bool):        Whether or not the file is uploaded.
        uploaded_at (int):      Epoc thime (ms) when the file was uploaded succesfully.
    """

    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.internal_representation["data"]["items"][0]
        self.id = item.get("id")
        self.file_name = item.get("fileName")
        self.directory = item.get("directory")
        self.source = item.get("source")
        self.file_type = item.get("fileType")
        self.metadata = item.get("metadata")
        self.asset_ids = item.get("assetIds")
        self.uploaded = item.get("uploaded")
        self.uploaded_at = item.get("uploadedAt")

    def to_pandas(self):
        file_info = copy(self.to_json())
        if file_info.get("metadata"):
            file_info.update(file_info.pop("metadata"))
        return pd.DataFrame.from_dict(file_info, orient="index")


class FileListResponse(CogniteCollectionResponse):
    """File List Response Object"""

    _RESPONSE_CLASS = FileInfoResponse

    def to_pandas(self):
        return pd.DataFrame(self.to_json())


class FilesClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.5", **kwargs)

    def upload_file(
        self, file_name, file_path=None, directory=None, source=None, file_type=None, content_type=None, **kwargs
    ) -> Dict:
        """Upload metadata about a file and get an upload link.

        The link will expire after 30 seconds if not resumable. A resumable upload link is default. Such a link is one-time
        use and expires after one week. For more information, check this link:
        https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload. Use PUT request to upload file with the
        link returned.

        If file_path is specified, the file will be uploaded directly by the SDK.

        Args:
            file_name (str):      File name. Max length is 256.

            file_path (str, optional):     Path of file to upload, if omitted a upload link will be returned.

            content_type (str, optional):   MIME type of your file. Required if file_path is specified.

            directory (str, optional):      Directory containing the file. Max length is 512.

            source (str, optional):         Source that this file comes from. Max length is 256.

            file_type (str, optional):      File type. E.g. pdf, css, spreadsheet, .. Max length is 64.

        Keyword Args:
            metadata (dict):      Customized data about the file.

            asset_ids (list):       IDs of assets related to this file.

            resumable (bool):     Whether to generate a resumable URL or not. Default is true.

            overwrite (bool):     Whether to overwrite existing data if duplicate or not. Default is false.

        Returns:
            Dict: A dictionary containing the field fileId and optionally also uploadURL if file_path is omitted.

        Examples:
            Upload a file and link it to an asset::

                client = CogniteClient()
                res = client.files.upload_file(file_name="myfile", file_path="/path/to/my/file.txt",
                        content_type="text/plain", asset_ids=[123])
                file_id = res["fileId"]
        """
        url = "/files/initupload"

        headers = {"X-Upload-Content-Type": content_type}

        params = {"resumable": kwargs.get("resumable", True), "overwrite": kwargs.get("overwrite", False)}

        body = {
            "fileName": file_name,
            "directory": directory,
            "source": source,
            "fileType": file_type,
            "metadata": kwargs.get("metadata", None),
            "assetIds": kwargs.get("asset_ids", None),
        }
        res_storage = self._post(url=url, body=body, headers=headers, params=params)
        result = res_storage.json()["data"]
        if file_path:
            if not content_type:
                warning = "content_type should be specified when directly uploading the file."
                warnings.warn(warning)
            headers = {"content-length": str(os.path.getsize(file_path))}
            with open(file_path, "rb") as file:
                self._request_session.put(result["uploadURL"], data=file, headers=headers)
            result.pop("uploadURL")
        return result

    def download_file(self, id: int, get_contents: bool = False) -> Union[str, bytes]:
        """Get list of files matching query.

        Args:
            id (int):                           Path to file to upload, if omitted a upload link will be returned.

            get_contents (bool, optional):      Boolean to determince whether or not to return file contents as string.
                                                Default is False and download url is returned.

        Returns:
            Union[str, bytes]: Download link if get_contents is False else file contents.

        Examples:
            Get a download url for the file::

                client = CogniteClient()
                res = client.files.download_file(id=12345)
                download_url = res["downloadUrl"]

            Download a file::

                client = CogniteClient()
                file_bytes = client.files.download_file(id=12345, get_contents=True)

        """
        url = "/files/{}/downloadlink".format(id)
        res = self._get(url=url)
        if get_contents:
            dl_link = res.json()["data"]
            res = self._request_session.get(dl_link)
            return res.content
        return res.json()["data"]

    def delete_files(self, file_ids) -> List:
        """Delete

        Args:
            file_ids (list[int]):   List of IDs of files to delete.

        Returns:
            List of files deleted and files that failed to delete.

        Examples:
            Delete two files::

                client = CogniteClient()
                res = client.files.delete_files([123, 234])
        """
        url = "/files/delete"
        body = {"items": file_ids}
        res = self._post(url, body=body)
        return res.json()["data"]

    def list_files(self, name=None, directory=None, file_type=None, source=None, **kwargs) -> FileListResponse:
        """Get list of files matching query.

        Args:
            name (str, optional):      List all files with this name.

            directory (str, optional):      Directory to list files from.

            source (str, optional):         List files coming from this source.

            file_type (str, optional):      Type of files to list.

        Keyword Args:
            asset_id (list):                Returns all files associated with this asset id.

            sort (str):                     Sort descending or ascending. 'ASC' or 'DESC'.

            limit (int):                    Number of results to return.

            is_uploaded (bool):             List only uploaded files if true. If false, list only other files. If not set,
                                            list all files without considering whether they are uploaded or not.

            autopaging (bool):              Whether or not to automatically page through results. If set to true, limit will
                                            be disregarded. Defaults to False.

            cursor (str):                   Cursor to use for paging through results.

        Returns:
            stable.files.FileListResponse: A data object containing the requested files information.
        Examples:
            List all files in a given directory::

                client = CogniteClient()
                res = client.files.list_files(directory="allfiles/myspecialfiles", autopaging=True)
                print(res.to_pandas())
        """
        autopaging = kwargs.get("autopaging", False)
        url = "/files"
        params = {
            "assetId": kwargs.get("asset_id"),
            "dir": directory,
            "name": name,
            "type": file_type,
            "source": source,
            "isUploaded": kwargs.get("is_uploaded"),
            "sort": kwargs.get("sort"),
            "limit": kwargs.get("limit", self._LIMIT) if not autopaging else self._LIMIT,
            "cursor": kwargs.get("cursor"),
        }

        res = self._get(url=url, params=params, autopaging=autopaging)
        return FileListResponse(res.json())

    def get_file_info(self, id) -> FileInfoResponse:
        """Returns information about a file.

        Args:
            id (int):                   Id of the file.

        Returns:
            stable.files.FileInfoResponse: A data object containing the requested file information.

        Examples:
            Get info a bout a specific file::

                client = CogniteClient()
                res = client.files.get_file_info(12345)
                print(res)
        """
        url = "/files/{}".format(id)
        res = self._get(url)
        return FileInfoResponse(res.json())
