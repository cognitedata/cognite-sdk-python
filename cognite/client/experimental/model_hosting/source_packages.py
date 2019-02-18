import os
from typing import Dict, List

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResponse


class CreateSourcePackageResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.id = item["id"]
        self.upload_url = item.get("uploadUrl")

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]


class SourcePackageResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.is_deprecated = item["isDeprecated"]
        self.package_name = item["packageName"]
        self.is_uploaded = item["isUploaded"]
        self.name = item["name"]
        self.available_operations = item["availableOperations"]
        self.created_time = item["createdTime"]
        self.runtime_version = item["runtimeVersion"]
        self.id = item["id"]
        self.metadata = item["metadata"]
        self.description = item["description"]
        self.project = item["project"]


class SourcePackageCollectionResponse(CogniteCollectionResponse):
    _RESPONSE_CLASS = SourcePackageResponse


class SourcePackageClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)
        self._LIMIT = 1000

    def create_source_package(
        self,
        name: str,
        package_name: str,
        available_operations: List[str],
        runtime_version: str,
        description: str = None,
        metadata: Dict = None,
        file_path: str = None,
    ) -> CreateSourcePackageResponse:
        """Upload a source package to the model hosting environment.

        Args:
            name (str): Name of source package
            package_name (str): name of root package for model
            available_operations (List[str]): List of routines which this source package supports ["predict", "train"]
            runtime_version (str): Version of environment in which the source-package should run. Currently only 0.1.
            description (str): Description for source package
            metadata (Dict): User defined key value pair of additional information.
            file_path (str): File path of source package distribution. If not specified, a download url will be returned.

        Returns:
            experimental.model_hosting.source_packages.CreateSourcePackageResponse: An response object containing Source package ID
            if file_path was specified. Else, both source package id and upload url.

        """
        url = "/analytics/models/sourcepackages"
        body = {
            "name": name,
            "description": description or "",
            "packageName": package_name,
            "availableOperations": available_operations,
            "metadata": metadata or {},
            "runtimeVersion": runtime_version,
        }
        res = self._post(url, body=body)
        if file_path:
            self._upload_file(res.json()["data"]["uploadUrl"], file_path)
            del res.json()["data"]["uploadUrl"]
            return CreateSourcePackageResponse(res.json())
        return CreateSourcePackageResponse(res.json())

    def _upload_file(self, upload_url, file_path):
        with open(file_path, "rb") as fh:
            mydata = fh.read()
            response = self._request_session.put(upload_url, data=mydata)
        return response

    def list_source_packages(
        self, limit: int = None, cursor: str = None, autopaging: bool = False
    ) -> SourcePackageCollectionResponse:
        """List all model source packages.

        Args:
            limit (int): Maximum number of source_packages to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            experimental.model_hosting.source_packages.SourcePackageCollectionResponse: List of source packages.
        """
        url = "/analytics/models/sourcepackages"
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIMIT}
        res = self._get(url, params=params, autopaging=autopaging)
        return SourcePackageCollectionResponse(res.json())

    def get_source_package(self, id: int) -> SourcePackageResponse:
        """Get source package by id.

        Args:
            id (int): Id of soure package to get.

        Returns:
            experimental.model_hosting.source_packages.SourcePackageResponse: The requested source package.
        """
        url = "/analytics/models/sourcepackages/{}".format(id)
        res = self._get(url)
        return SourcePackageResponse(res.json())

    def delete_source_package(self, id: int) -> None:
        """Delete source package by id.

        Args:
            id (int): Id of soure package to delete.

        Returns:
            None
        """
        url = "/analytics/models/sourcepackages/{}".format(id)
        self._delete(url)

    def deprecate_source_package(self, id: int) -> SourcePackageResponse:
        """Deprecate a source package by id.

        Args:
            id (int): Id of soure package to get.

        Returns:
            experimental.model_hosting.source_packages.SourcePackageResponse: The requested source package.
        """
        url = "/analytics/models/sourcepackages/{}/deprecate".format(id)
        res = self._put(url)
        return SourcePackageResponse(res.json())

    def download_source_package_code(self, id: int, directory: str = None) -> None:
        """Download the tarball for a source package to a specified directory.


        Args:
            id (int): Id of source package.
            directory (str): Directory to put source package in. Defaults to current working directory.

        Returns:
            None
        """
        directory = directory or os.getcwd()
        file_path = os.path.join(directory, self.get_source_package(id).name + ".tar.gz")
        url = "/analytics/models/sourcepackages/{}/code".format(id)
        download_url = self._get(url).json()["data"]["downloadUrl"]
        with open(file_path, "wb") as fh:
            response = self._request_session.get(download_url).content
            fh.write(response)

    def delete_source_package_code(self, id: int) -> None:
        """Delete the code/tarball for the source package from the cloud storage location.
        This will only work if the source package has been deprecated.

        Warning: This cannot be undone.

        Args:
            id (int): Id of the source package.

        Returns:
            None

        """
        url = "/analytics/models/sourcepackages/{}/code".format(id)
        self._delete(url)
