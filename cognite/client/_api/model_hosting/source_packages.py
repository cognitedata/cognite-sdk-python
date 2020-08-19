import os
import re
from subprocess import check_call
from typing import Dict, List, Tuple

from cognite.client._api_client import APIClient
from cognite.client.data_classes.model_hosting.source_packages import (
    CreateSourcePackageResponse,
    SourcePackage,
    SourcePackageList,
)


class SourcePackagesAPI(APIClient):
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
            CreateSourcePackageResponse: An response object containing Source package ID
            if file_path was specified. Else, both source package id and upload url.

        """
        body = {
            "name": name,
            "description": description or "",
            "packageName": package_name,
            "availableOperations": available_operations,
            "metadata": metadata or {},
            "runtimeVersion": runtime_version,
        }
        res = self._post("/modelhosting/models/sourcepackages", json=body).json()
        if file_path:
            self._upload_file(res["uploadUrl"], file_path)
            del res["uploadUrl"]
            return CreateSourcePackageResponse._load(res)
        return CreateSourcePackageResponse._load(res)

    def _get_model_py_files(self, path) -> List:
        files_containing_model_py = []
        for root, dirs, files in os.walk(path):
            if "model.py" in files:
                package_name = os.path.basename(root)
                file_path = os.path.join(root, "model.py")
                files_containing_model_py.append((package_name, file_path))
        return files_containing_model_py

    def _find_model_file_and_extract_details(self, package_directory: str) -> Tuple:
        num_of_eligible_model_py_files = 0
        for package_name, file_path in self._get_model_py_files(package_directory):
            with open(file_path, "r") as f:
                file_content = f.read()
                if re.search("class Model", file_content):
                    num_of_eligible_model_py_files += 1
                    model_package_name = package_name
                    model_file_content = file_content

        assert num_of_eligible_model_py_files != 0, "Could not locate a file named model.py containing a Model class"
        assert num_of_eligible_model_py_files == 1, "Multiple model.py files with a Model class in your source package"

        available_operations = []
        if re.search(r"def train\(", model_file_content):
            available_operations.append("TRAIN")
        if re.search(r"def predict\(", model_file_content):
            if re.search(r"def load\(", model_file_content):
                available_operations.append("PREDICT")
            else:
                raise AssertionError("Your Model class defines predict() but not load().")
        assert len(available_operations) > 0, "Your model does not define a train or a predict method"
        return model_package_name, available_operations

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
            CreateSourcePackageResponse: An response object containing Source package ID
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
            response = self._http_client.request("PUT", upload_url, data=mydata, timeout=180)
        return response

    def list_source_packages(
        self, limit: int = None, cursor: str = None, autopaging: bool = False
    ) -> SourcePackageList:
        """List all model source packages.

        Args:
            limit (int): Maximum number of source_packages to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            SourcePackageList: List of source packages.
        """
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIST_LIMIT}
        res = self._get("/modelhosting/models/sourcepackages", params=params)
        return SourcePackageList._load(res.json()["items"])

    def get_source_package(self, id: int) -> SourcePackage:
        """Get source package by id.

        Args:
            id (int): Id of soure package to get.

        Returns:
            SourcePackage: The requested source package.
        """
        res = self._get("/modelhosting/models/sourcepackages/{}".format(id))
        return SourcePackage._load(res.json())

    def delete_source_package(self, id: int) -> None:
        """Delete source package by id.

        Args:
            id (int): Id of soure package to delete.

        Returns:
            None
        """
        self._delete("/modelhosting/models/sourcepackages/{}".format(id))

    def deprecate_source_package(self, id: int) -> SourcePackage:
        """Deprecate a source package by id.

        Args:
            id (int): Id of soure package to get.

        Returns:
            SourcePackage: The requested source package.
        """
        res = self._post("/modelhosting/models/sourcepackages/{}/deprecate".format(id), json={})
        return SourcePackage._load(res.json())

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
        url = "/modelhosting/models/sourcepackages/{}/code".format(id)
        download_url = self._get(url).json()["downloadUrl"]
        with open(file_path, "wb") as fh:
            response = self._http_client.request("GET", download_url).content
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
        self._delete("/modelhosting/models/sourcepackages/{}/code".format(id))
