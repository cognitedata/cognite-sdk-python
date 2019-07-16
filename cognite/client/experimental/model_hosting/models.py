import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Dict, List

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResponse
from cognite.client.exceptions import APIError


class ModelResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.id = item["id"]
        self.name = item["name"]
        self.project = item["project"]
        self.description = item["description"]
        self.created_time = item["createdTime"]
        self.metadata = item["metadata"]
        self.is_deprecated = item["isDeprecated"]
        self.active_version_id = item["activeVersionId"]
        self.input_fields = item["inputFields"]
        self.output_fields = item["outputFields"]
        self.webhook_url = item["webhookUrl"]


class ModelCollectionResponse(CogniteCollectionResponse):
    _RESPONSE_CLASS = ModelResponse


class ModelVersionResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.id = item["id"]
        self.is_deprecated = item["isDeprecated"]
        self.training_details = item["trainingDetails"]
        self.name = item["name"]
        self.error_msg = item["errorMsg"]
        self.model_id = item["modelId"]
        self.created_time = item["createdTime"]
        self.metadata = item["metadata"]
        self.source_package_id = item["sourcePackageId"]
        self.status = item["status"]
        self.description = item["description"]
        self.project = item["project"]


class ModelVersionCollectionResponse(CogniteCollectionResponse):
    _RESPONSE_CLASS = ModelVersionResponse


class ModelArtifactResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.name = item["name"]
        self.size = item["size"]


class ModelArtifactCollectionResponse(CogniteCollectionResponse):
    _RESPONSE_CLASS = ModelArtifactResponse


class ModelLogResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.prediction_logs = item["predict"]
        self.training_logs = item["train"]

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]


class PredictionError(APIError):
    pass


class EmptyAritfactsDirectory(Exception):
    pass


class ModelsClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)
        self._LIMIT = 1000

    def create_model(
        self,
        name: str,
        description: str = "",
        metadata: Dict[str, Any] = None,
        input_fields: List[Dict[str, str]] = None,
        output_fields: List[Dict[str, str]] = None,
        webhook_url: str = None,
    ) -> ModelResponse:
        """Creates a new model

        Args:
            name (str):             Name of model
            description (str):      Description
            metadata (Dict[str, Any]):          Metadata about model
            input_fields (List[str]):   List of input fields the model accepts
            output_fields (List[str]):   List of output fields the model produces
            webhook_url (str): Webhook url to send notifications to upon failing scheduled predictions

        Returns:
            experimental.model_hosting.models.ModelResponse: The created model.
        """
        url = "/analytics/models"
        model_body = {
            "name": name,
            "description": description,
            "metadata": metadata or {},
            "inputFields": input_fields or [],
            "outputFields": output_fields or [],
        }
        if webhook_url is not None:
            model_body["webhookUrl"] = webhook_url
        res = self._post(url, body=model_body)
        return ModelResponse(res.json())

    def list_models(self, limit: int = None, cursor: int = None, autopaging: bool = False) -> ModelCollectionResponse:
        """List all models.

        Args:
            limit (int): Maximum number of models to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            experimental.model_hosting.models.ModelCollectionResponse: List of models
        """
        url = "/analytics/models"
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIMIT}
        res = self._get(url, params=params, autopaging=autopaging)
        return ModelCollectionResponse(res.json())

    def get_model(self, id: int) -> ModelResponse:
        """Get a model by id.

        Args:
            id (int): Id of model to get.

        Returns:
            experimental.model_hosting.models.ModelResponse: The requested model
        """
        url = "/analytics/models/{}".format(id)
        res = self._get(url)
        return ModelResponse(res.json())

    def update_model(
        self,
        id: int,
        description: str = None,
        metadata: Dict[str, str] = None,
        active_version_id: int = None,
        webhook_url: str = None,
    ) -> ModelResponse:
        """Update a model.

        Args:
            id (int): Id of model to update.
            description (str): Description of model.
            metadata (Dict[str, str]): metadata about model.
            active_version_id (int): Active version of model.
            webhook_url (str): Webhook url to send notifications to upon failing scheduled predictions.

        Returns:
            experimental.model_hosting.models.ModelResponse: Updated model
        """
        url = "/analytics/models/{}/update".format(id)
        body = {}
        if description:
            body.update({"description": {"set": description}})
        if metadata:
            body.update({"metadata": {"set": metadata}})
        if active_version_id:
            body.update({"activeVersionId": {"set": active_version_id}})
        if webhook_url:
            body.update({"webhookUrl": {"set": webhook_url}})
        res = self._put(url, body=body)
        return ModelResponse(res.json())

    def deprecate_model(self, id: int) -> ModelResponse:
        """Deprecate a model.

        Args:
            id (int): Id of model to deprecate.

        Returns:
            experimental.model_hosting.models.ModelResponse: Deprecated model
        """
        url = "/analytics/models/{}/deprecate".format(id)
        res = self._put(url)
        return ModelResponse(res.json())

    def delete_model(self, id: int) -> None:
        """Delete a model.

        Will also delete all versions and schedules for this model.

        Args:
            id (int): Delete model with this id.

        Returns:
            None
        """
        url = "/analytics/models/{}".format(id)
        self._delete(url)

    def create_model_version(
        self, name: str, model_id: int, source_package_id: int, description: str = None, metadata: Dict = None
    ) -> ModelVersionResponse:
        """Create a model version without deploying it.

        Then you can optionally upload artifacts to the model version and later deploy it.

        Args:
            name (str): Name of the the model version.
            model_id (int): Create the version on the model with this id.
            source_package_id (int): Use the source package with this id. The source package must have an available
                predict operation.
            description (str):  Description of model version
            metadata (Dict[str, Any]):  Metadata about model version

        Returns:
            ModelVersionResponse: The created model version.
        """
        url = "/analytics/models/{}/versions".format(model_id)
        body = {
            "name": name,
            "description": description or "",
            "sourcePackageId": source_package_id,
            "metadata": metadata or {},
        }
        res = self._post(url, body=body)
        return ModelVersionResponse(res.json())

    def deploy_awaiting_model_version(self, model_id: int, version_id: int) -> ModelVersionResponse:
        """Deploy an already created model version awaiting manual deployment.

        The model version must have status AWAITING_MANUAL_DEPLOYMENT in order for this to work.

        Args:
            model_id (int): The id of the model containing the version to deploy.
            version_id (int): The id of the model version to deploy.
        Returns:
            ModelVersionResponse: The deployed model version.
        """
        url = "/analytics/models/{}/versions/{}/deploy".format(model_id, version_id)
        res = self._post(url, body={})
        return ModelVersionResponse(res.json())

    def deploy_model_version(
        self,
        name: str,
        model_id: int,
        source_package_id: int,
        artifacts_directory: str = None,
        description: str = None,
        metadata: Dict = None,
    ) -> ModelVersionResponse:
        """This will create and deploy a model version.

        If artifacts_directory is specified, it will traverse that directory recursively and
        upload all artifacts in that directory before deploying.

        Args:
            name (str): Name of the the model version.
            model_id (int): Create the version on the model with this id.
            source_package_id (int): Use the source package with this id. The source package must have an available
                predict operation.
            artifacts_directory (str, optional): Absolute path of directory containing artifacts.
            description (str, optional):  Description of model version
            metadata (Dict[str, Any], optional):  Metadata about model version
            
        Returns:
            ModelVersionResponse: The created model version.
        """
        model_version = self.create_model_version(name, model_id, source_package_id, description, metadata)
        if artifacts_directory:
            self.upload_artifacts_from_directory(model_id, model_version.id, directory=artifacts_directory)
        return self.deploy_awaiting_model_version(model_id, model_version.id)

    def train_and_deploy_model_version(
        self,
        name: str,
        model_id: int,
        source_package_id: int,
        train_source_package_id: int = None,
        metadata: Dict = None,
        description: str = None,
        args: Dict[str, Any] = None,
        scale_tier: str = None,
        machine_type: str = None,
    ) -> ModelVersionResponse:
        """Train and deploy a new version of a model.

        This will instantiate a training job and automatically deploy the model upon completion.

        Args:
            model_id (int): Create a new version under the model with this id
            name (str): Name of model version.
            source_package_id (int):    Use the source package with this id
            train_source_package_id (int):  Use this source package for training. If omitted, will default to
                                            source_package_id.
            metadata (Dict[str, Any]):  Metadata about model version
            description (str):  Description of model version
            args (Dict[str, Any]):   Dictionary of arguments to pass to the training job.
            scale_tier (str):   Which scale tier to use. Must be either "BASIC" or "CUSTOM".
            machine_type (str): Specify a machine type. Applies only if scale_tier is "CUSTOM".

        Returns:
            experimental.model_hosting.models.ModelVersionResponse: The created model version.
        """
        url = "/analytics/models/{}/versions/train".format(model_id)
        if args and "data_spec" in args:
            data_spec = args["data_spec"]
            if hasattr(data_spec, "dump"):
                args["data_spec"] = data_spec.dump()
        body = {
            "name": name,
            "description": description or "",
            "sourcePackageId": source_package_id,
            "trainingDetails": {
                "sourcePackageId": train_source_package_id or source_package_id,
                "args": args or {},
                "scaleTier": scale_tier or "BASIC",
                "machineType": machine_type,
            },
            "metadata": metadata or {},
        }
        res = self._post(url, body=body)
        return ModelVersionResponse(res.json())

    def list_model_versions(
        self, model_id: int, limit: int = None, cursor: str = None, autopaging: bool = False
    ) -> ModelVersionCollectionResponse:
        """Get all versions of a specific model.

        Args:
            model_id (int): Get versions for the model with this id.
            limit (int): Maximum number of model versions to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            experimental.model_hosting.models.ModelVersionCollectionResponse: List of model versions
        """
        url = "/analytics/models/{}/versions".format(model_id)
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIMIT}
        res = self._get(url, params=params, autopaging=True)
        return ModelVersionCollectionResponse(res.json())

    def get_model_version(self, model_id: int, version_id: int) -> ModelVersionResponse:
        """Get a specific model version by id.

        Args:
            model_id (int): Id of model which has the model version.
            version_id (int): Id of model version.

        Returns:
            experimental.model_hosting.models.ModelVersionResponse: The requested model version
        """
        url = "/analytics/models/{}/versions/{}".format(model_id, version_id)
        res = self._get(url)
        return ModelVersionResponse(res.json())

    def update_model_version(
        self, model_id: int, version_id: int, description: str = None, metadata: Dict[str, str] = None
    ) -> ModelVersionResponse:
        """Update description or metadata on a model version.

        Args:
            model_id (int): Id of model containing the model version.
            version_id (int): Id of model version to update.
            description (str): New description.
            metadata (Dict[str, str]): New metadata

        Returns:
            ModelVersionResponse: The updated model version.
        """
        url = "/analytics/models/{}/versions/{}/update".format(model_id, version_id)
        body = {}
        if description:
            body.update({"description": {"set": description}})
        if metadata:
            body.update({"metadata": {"set": metadata}})

        res = self._put(url, body=body)
        return ModelVersionResponse(res.json())

    def deprecate_model_version(self, model_id: int, version_id: int) -> ModelVersionResponse:
        """Deprecate a model version

        Args:
            model_id (int): Id of model
            version_id (int): Id of model version to deprecate

        Returns:
            ModelVersionResponse: The deprecated model version
        """
        url = "/analytics/models/{}/versions/{}/deprecate".format(model_id, version_id)
        res = self._put(url)
        return ModelVersionResponse(res.json())

    def delete_model_version(self, model_id: int, version_id: int) -> None:
        """Delete a model version by id.

        Args:
            model_id (int): Id of model which has the model version.
            version_id (int): Id of model version.

        Returns:
            None
        """
        url = "/analytics/models/{}/versions/{}".format(model_id, version_id)
        self._delete(url)

    def online_predict(
        self, model_id: int, version_id: int = None, instances: List = None, args: Dict[str, Any] = None
    ) -> List:
        """Perform online prediction on a models active version or a specified version.

        Args:
            model_id (int):     Perform a prediction on the model with this id. Will use active version.
            version_id (int):   Use this version instead of the active version. (optional)
            instances (List): List of JSON serializable instances to pass to your model one-by-one.
            args (Dict[str, Any])    Dictinoary of keyword arguments to pass to your predict method.

        Returns:
            List: List of predictions for each instance.
        """
        url = "/analytics/models/{}/predict".format(model_id)
        if instances:
            for i, instance in enumerate(instances):
                if hasattr(instance, "dump"):
                    instances[i] = instance.dump()
        if version_id:
            url = "/analytics/models/{}/versions/{}/predict".format(model_id, version_id)
        body = {"instances": instances, "args": args or {}}
        res = self._put(url, body=body).json()
        if "error" in res:
            raise PredictionError(message=res["error"]["message"], code=res["error"]["code"])
        return res["data"]["predictions"]

    def list_artifacts(self, model_id: int, version_id: int) -> ModelArtifactCollectionResponse:
        """List the artifacts associated with the specified model version.

        Args:
            model_id (int): Id of model
            version_id: Id of model version to get artifacts for

        Returns:
            experimental.model_hosting.models.ModelArtifactCollectionResponse: List of artifacts
        """
        url = "/analytics/models/{}/versions/{}/artifacts".format(model_id, version_id)
        res = self._get(url)
        return ModelArtifactCollectionResponse(res.json())

    def download_artifact(self, model_id: int, version_id: int, name: str, directory: str = None) -> None:
        """Download an artifact to a directory. Defaults to current working directory.

        Args:
            model_id (int): Id of model
            version_id (int): Id of model version.
            name (int): Name of artifact.
            directory (int): Directory to place artifact in. Defaults to current working directory.

        Returns:
            None
        """
        directory = directory or os.getcwd()
        file_path = os.path.join(directory, name)

        url = "/analytics/models/{}/versions/{}/artifacts/{}".format(model_id, version_id, name)
        download_url = self._get(url).json()["data"]["downloadUrl"]
        with open(file_path, "wb") as fh:
            response = self._request_session.get(download_url).content
            fh.write(response)

    def upload_artifact_from_file(self, model_id: int, version_id: int, name: str, file_path: str) -> None:
        """Upload an artifact to a model version.

        The model version must have status AWAITING_MANUAL_DEPLOYMENT in order for this to work.

        Args:
            model_id (int): The id of the model.
            version_id (int): The id of the model version to upload the artifacts to.
            name (str): The name of the artifact.
            file_path (str): The local path of the artifact.
        Returns:
            None
        """
        url = "/analytics/models/{}/versions/{}/artifacts/upload".format(model_id, version_id)
        body = {"name": name}
        res = self._post(url, body=body)
        upload_url = res.json()["data"]["uploadUrl"]
        self._upload_file(upload_url, file_path)

    def upload_artifacts_from_directory(self, model_id: int, version_id: int, directory: str) -> None:
        """Upload all files in directory recursively.

        Args:
            model_id (int): The id of the model.
            version_id (int): The id of the model version to upload the artifacts to.
            directory (int): Absolute path of directory to upload artifacts from.
        Returns:
            None
        """
        upload_tasks = []
        for root, dirs, files in os.walk(directory):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                full_file_name = os.path.relpath(file_path, directory)
                upload_tasks.append((model_id, version_id, full_file_name, file_path))

        if len(upload_tasks) == 0:
            raise EmptyAritfactsDirectory("Artifacts directory is empty.")
        self._execute_tasks_concurrently(self.upload_artifact_from_file, upload_tasks)

    @staticmethod
    def _execute_tasks_concurrently(func, tasks):
        with ThreadPoolExecutor(16) as p:
            futures = [p.submit(func, *task) for task in tasks]
            return [future.result() for future in futures]

    def _upload_file(self, upload_url, file_path):
        with open(file_path, "rb") as fh:
            mydata = fh.read()
            response = self._request_session.put(upload_url, data=mydata)
        return response

    def get_logs(self, model_id: int, version_id: int, log_type: str = None) -> ModelLogResponse:
        """Get logs for prediction and/or training routine of a specific model version.

        Args:
            model_id (int): Id of model.
            version_id (int): Id of model version to get logs for.
            log_type (str): Which routine to get logs from. Must be 'train’, 'predict’, or ‘both’. Defaults to 'both'.

        Returns:
            ModelLogResponse: An object containing the requested logs.
        """
        url = "/analytics/models/{}/versions/{}/log".format(model_id, version_id)
        params = {"logType": log_type}
        res = self._get(url, params=params)
        return ModelLogResponse(res.json())
