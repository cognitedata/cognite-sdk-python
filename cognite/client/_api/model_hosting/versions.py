import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Dict, List

from cognite.client._api_client import APIClient
from cognite.client.data_classes.model_hosting.versions import (
    ModelArtifactList,
    ModelVersion,
    ModelVersionList,
    ModelVersionLog,
)


class EmptyArtifactsDirectory(Exception):
    pass


class ModelVersionsAPI(APIClient):
    def create_model_version(
        self, model_name: str, version_name: str, source_package_id: int, description: str = None, metadata: Dict = None
    ) -> ModelVersion:
        """Create a model version without deploying it.

        Then you can optionally upload artifacts to the model version and later deploy it.

        Args:
            model_name (str): Create the version on the model with this name.
            version_name (str): Name of the the model version.
            source_package_id (int): Use the source package with this id. The source package must have an available
                predict operation.
            description (str):  Description of model version
            metadata (Dict[str, Any]):  Metadata about model version

        Returns:
            ModelVersion: The created model version.
        """
        url = "/modelhosting/models/{}/versions".format(model_name)
        body = {
            "name": version_name,
            "description": description or "",
            "sourcePackageId": source_package_id,
            "metadata": metadata or {},
        }
        res = self._post(url, json=body)
        return ModelVersion._load(res.json())

    def deploy_awaiting_model_version(self, model_name: str, version_name: str) -> ModelVersion:
        """Deploy an already created model version awaiting manual deployment.

        The model version must have status AWAITING_MANUAL_DEPLOYMENT in order for this to work.

        Args:
            model_name (str): The name of the model containing the version to deploy.
            version_name (str): The name of the model version to deploy.
        Returns:
            ModelVersion: The deployed model version.
        """
        url = "/modelhosting/models/{}/versions/{}/deploy".format(model_name, version_name)
        res = self._post(url, json={})
        return ModelVersion._load(res.json())

    def deploy_model_version(
        self,
        model_name: str,
        version_name: str,
        source_package_id: int,
        artifacts_directory: str = None,
        description: str = None,
        metadata: Dict = None,
    ) -> ModelVersion:
        """This will create and deploy a model version.

        If artifacts_directory is specified, it will traverse that directory recursively and
        upload all artifacts in that directory before deploying.

        Args:
            model_name (str): Create the version on the model with this name.
            version_name (str): Name of the the model version.
            source_package_id (int): Use the source package with this id. The source package must have an available
                predict operation.
            artifacts_directory (str, optional): Absolute path of directory containing artifacts.
            description (str, optional):  Description of model version
            metadata (Dict[str, Any], optional):  Metadata about model version

        Returns:
            ModelVersion: The created model version.
        """
        model_version = self.create_model_version(model_name, version_name, source_package_id, description, metadata)
        if artifacts_directory:
            self.upload_artifacts_from_directory(model_name, model_version.name, directory=artifacts_directory)
        return self.deploy_awaiting_model_version(model_name, model_version.name)

    def list_model_versions(
        self, model_name: str, limit: int = None, cursor: str = None, autopaging: bool = False
    ) -> ModelVersionList:
        """Get all versions of a specific model.

        Args:
            model_name (str): Get versions for the model with this name.
            limit (int): Maximum number of model versions to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            ModelVersionList: List of model versions
        """
        url = "/modelhosting/models/{}/versions".format(model_name)
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIST_LIMIT}
        res = self._get(url, params=params)
        return ModelVersionList._load(res.json()["items"])

    def get_model_version(self, model_name: str, version_name: str) -> ModelVersion:
        """Get a specific model version by name.

        Args:
            model_name (str): Name of model which has the model version.
            version_name (str): Name of model version.

        Returns:
            ModelVersion: The requested model version
        """
        url = "/modelhosting/models/{}/versions/{}".format(model_name, version_name)
        res = self._get(url)
        return ModelVersion._load(res.json())

    def update_model_version(
        self, model_name: str, version_name: str, description: str = None, metadata: Dict[str, str] = None
    ) -> ModelVersion:
        """Update description or metadata on a model version.

        Args:
            model_name (str): Name of model containing the model version.
            version_name (str): Name of model version to update.
            description (str): New description.
            metadata (Dict[str, str]): New metadata

        Returns:
            ModelVersion: The updated model version.
        """
        url = "/modelhosting/models/{}/versions/{}/update".format(model_name, version_name)
        body = {}
        if description:
            body.update({"description": {"set": description}})
        if metadata:
            body.update({"metadata": {"set": metadata}})

        res = self._post(url, json=body)
        return ModelVersion._load(res.json())

    def deprecate_model_version(self, model_name: str, version_name: str) -> ModelVersion:
        """Deprecate a model version

        Args:
            model_name (str): Name of model
            version_name (str): name of model version to deprecate

        Returns:
            ModelVersion: The deprecated model version
        """
        url = "/modelhosting/models/{}/versions/{}/deprecate".format(model_name, version_name)
        res = self._post(url)
        return ModelVersion._load(res.json())

    def delete_model_version(self, model_name: str, version_name: str) -> None:
        """Delete a model version by id.

        Args:
            model_name (str): Name of model which has the model version.
            version_name (str): Name of model version.

        Returns:
            None
        """
        url = "/modelhosting/models/{}/versions/{}".format(model_name, version_name)
        self._delete(url)

    def list_artifacts(self, model_name: str, version_name: str) -> ModelArtifactList:
        """List the artifacts associated with the specified model version.

        Args:
            model_name (str): Name of model
            version_name (str): Name of model version to get artifacts for

        Returns:
            ModelArtifactList: List of artifacts
        """
        url = "/modelhosting/models/{}/versions/{}/artifacts".format(model_name, version_name)
        res = self._get(url)
        return ModelArtifactList._load(res.json()["items"])

    def download_artifact(self, model_name: str, version_name: str, artifact_name: str, directory: str = None) -> None:
        """Download an artifact to a directory. Defaults to current working directory.

        Args:
            model_name (str): Name of model
            version_name (str): Name of model version.
            artifact_name (str): Name of artifact.
            directory (str): Directory to place artifact in. Defaults to current working directory.

        Returns:
            None
        """
        directory = directory or os.getcwd()
        file_path = os.path.join(directory, artifact_name)

        url = "/modelhosting/models/{}/versions/{}/artifacts/{}".format(model_name, version_name, artifact_name)
        download_url = self._get(url).json()["downloadUrl"]
        with open(file_path, "wb") as fh:
            response = self._http_client.request("GET", download_url).content
            fh.write(response)

    def upload_artifact_from_file(self, model_name: str, version_name: str, artifact_name: str, file_path: str) -> None:
        """Upload an artifact to a model version.

        The model version must have status AWAITING_MANUAL_DEPLOYMENT in order for this to work.

        Args:
            model_name (str): The name of the model.
            version_name (str): The name of the model version to upload the artifacts to.
            artifact_name (str): The name of the artifact.
            file_path (str): The local path of the artifact.
        Returns:
            None
        """
        url = "/modelhosting/models/{}/versions/{}/artifacts/upload".format(model_name, version_name)
        body = {"name": artifact_name}
        res = self._post(url, json=body)
        upload_url = res.json()["uploadUrl"]
        self._upload_file(upload_url, file_path)

    def upload_artifacts_from_directory(self, model_name: str, version_name: str, directory: str) -> None:
        """Upload all files in directory recursively.

        Args:
            model_name (str): The name of the model.
            version_name (str): The name of the model version to upload the artifacts to.
            directory (str): Absolute path of directory to upload artifacts from.
        Returns:
            None
        """
        upload_tasks = []
        for root, dirs, files in os.walk(directory):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                full_file_name = os.path.relpath(file_path, directory)
                upload_tasks.append((model_name, version_name, full_file_name, file_path))

        if len(upload_tasks) == 0:
            raise EmptyArtifactsDirectory("Artifacts directory is empty.")
        self._execute_tasks_concurrently(self.upload_artifact_from_file, upload_tasks)

    @staticmethod
    def _execute_tasks_concurrently(func, tasks):
        with ThreadPoolExecutor(16) as p:
            futures = [p.submit(func, *task) for task in tasks]
            return [future.result() for future in futures]

    def _upload_file(self, upload_url, file_path):
        with open(file_path, "rb") as fh:
            mydata = fh.read()
            response = self._http_client.request("PUT", upload_url, data=mydata)
        return response

    def get_logs(self, model_name: str, version_name: str, log_type: str = None) -> ModelVersionLog:
        """Get logs for prediction and/or training routine of a specific model version.

        Args:
            model_name (str): Name of model.
            version_name (str): Name of model version to get logs for.
            log_type (str): Which routine to get logs from. Must be 'train’, 'predict’, or ‘both’. Defaults to 'both'.

        Returns:
            ModelVersionLog: An object containing the requested logs.
        """
        url = "/modelhosting/models/{}/versions/{}/log".format(model_name, version_name)
        params = {"logType": log_type}
        res = self._get(url, params=params)
        return ModelVersionLog._load(res.json())
