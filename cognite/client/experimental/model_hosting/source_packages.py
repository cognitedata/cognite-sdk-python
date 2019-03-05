import os
import pkgutil
from collections import namedtuple
from subprocess import check_call
from typing import Dict, List, NamedTuple, Tuple

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

    def upload_source_package(
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
        res = self._post(url, body=body).json()
        if file_path:
            self._upload_file(res["data"]["uploadUrl"], file_path)
            del res["data"]["uploadUrl"]
            return CreateSourcePackageResponse(res)
        return CreateSourcePackageResponse(res)

    def _get_modules_named_model(self, path, package_name=None):
        modules = []
        for module_finder, name, ispkg in pkgutil.walk_packages([path]):
            if ispkg:
                modules.extend(self._get_modules_named_model(path + "/{}".format(name), package_name=name))
            else:
                if name == "model":
                    try:
                        module = module_finder.find_module(name).load_module()
                    except Exception as e:
                        raise AssertionError("Your model.py file could not be imported") from e
                    modules.append((package_name, module))
        return modules

    def _find_model_file_and_extract_details(self, package_directory: str) -> Tuple:
        for pkg_name, module in self._get_modules_named_model(package_directory):
            if hasattr(module, "Model"):
                package_name = pkg_name
                break
        else:
            raise AssertionError("Could not locate a file named model.py containing a Model class")

        available_operations = []
        if hasattr(module.Model, "train"):
            available_operations.append("TRAIN")
        if hasattr(module.Model, "predict"):
            if hasattr(module.Model, "load"):
                available_operations.append("PREDICT")
            else:
                raise AssertionError("Your Model class defines predict() but not load().")

        assert len(available_operations) > 0, "Your model does not define a train or a predict method"

        return package_name, available_operations

    def _build_distribution(self, package_path) -> str:
        check_call("cd {} && python setup.py sdist".format(package_path), shell=True)
        dist_directory = os.path.join(package_path, "dist")
        for file in os.listdir(dist_directory):
            if file.endswith(".tar.gz"):
                return os.path.join(dist_directory, file)

    def build_and_upload_source_package(
        self, name: str, runtime_version: str, package_directory: str, description: str = None, metadata: Dict = None
    ) -> CreateSourcePackageResponse:
        """Build a distribution for a source package and upload it to the model hosting environment.

        This method will recursively search through your package and infer available_operations as well as the package
        name.

        Args:
            name (str): Name of source package
            runtime_version (str): Version of environment in which the source-package should run. Currently only 0.1.
            description (str): Description for source package
            metadata (Dict): User defined key value pair of additional information.
            package_directory (str): Absolute path of directory containing your setup.py file.

        Returns:
            experimental.model_hosting.source_packages.CreateSourcePackageResponse: An response object containing Source package ID
            if file_path was specified. Else, both source package id and upload url.
        """
        package_name, available_operations = self._find_model_file_and_extract_details(package_directory)
        tar_gz_path = self._build_distribution(package_directory)

        return self.upload_source_package(
            name=name,
            package_name=package_name,
            available_operations=available_operations,
            runtime_version=runtime_version,
            description=description,
            metadata=metadata,
            file_path=tar_gz_path,
        )

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
