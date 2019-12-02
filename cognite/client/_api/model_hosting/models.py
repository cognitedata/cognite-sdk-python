from typing import Any, Dict, List

from cognite.client._api_client import APIClient
from cognite.client.data_classes.model_hosting.models import Model, ModelList
from cognite.client.exceptions import CogniteAPIError


class PredictionError(CogniteAPIError):
    pass


class ModelsAPI(APIClient):
    def create_model(
        self,
        name: str,
        description: str = "",
        metadata: Dict[str, Any] = None,
        input_fields: List[Dict[str, str]] = None,
        output_fields: List[Dict[str, str]] = None,
        webhook_url: str = None,
    ) -> Model:
        """Creates a new model

        Args:
            name (str):             Name of model
            description (str):      Description
            metadata (Dict[str, Any]):          Metadata about model
            input_fields (List[str]):   List of input fields the model accepts
            output_fields (List[str]):   List of output fields the model produces
            webhook_url (str): Webhook url to send notifications to upon failing scheduled predictions

        Returns:
            Model: The created model.
        """
        url = "/modelhosting/models"
        model_body = {
            "name": name,
            "description": description,
            "metadata": metadata or {},
            "inputFields": input_fields or [],
            "outputFields": output_fields or [],
        }
        if webhook_url is not None:
            model_body["webhookUrl"] = webhook_url
        res = self._post(url, json=model_body)
        return Model._load(res.json())

    def list_models(self, limit: int = None, cursor: int = None, autopaging: bool = False) -> ModelList:
        """List all models.

        Args:
            limit (int): Maximum number of models to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            ModelList: List of models
        """
        url = "/modelhosting/models"
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIST_LIMIT}
        res = self._get(url, params=params)
        return ModelList._load(res.json()["items"])

    def get_model(self, name: str) -> Model:
        """Get a model by name.

        Args:
            name (str): Name of model to get.

        Returns:
            Model: The requested model
        """
        url = "/modelhosting/models/{}".format(name)
        res = self._get(url)
        return Model._load(res.json())

    def update_model(
        self,
        name: str,
        description: str = None,
        metadata: Dict[str, str] = None,
        active_version_name: int = None,
        webhook_url: str = None,
    ) -> Model:
        """Update a model.

        Args:
            name (str): Name of model to update.
            description (str): Description of model.
            metadata (Dict[str, str]): metadata about model.
            active_version_name (str): Active version of model.
            webhook_url (str): Webhook url to send notifications to upon failing scheduled predictions.

        Returns:
            Model: Updated model
        """
        url = "/modelhosting/models/{}/update".format(name)
        body = {}
        if description:
            body.update({"description": {"set": description}})
        if metadata:
            body.update({"metadata": {"set": metadata}})
        if active_version_name:
            body.update({"activeVersionName": {"set": active_version_name}})
        if webhook_url:
            body.update({"webhookUrl": {"set": webhook_url}})
        res = self._post(url, json=body)
        return Model._load(res.json())

    def deprecate_model(self, name: str) -> Model:
        """Deprecate a model.

        Args:
            name (str): Name of model to deprecate.

        Returns:
            Model: Deprecated model
        """
        url = "/modelhosting/models/{}/deprecate".format(name)
        res = self._post(url, json={})
        return Model._load(res.json())

    def delete_model(self, name: str) -> None:
        """Delete a model.

        Will also delete all versions and schedules for this model.

        Args:
            name (str): Delete model with this name.

        Returns:
            None
        """
        url = "/modelhosting/models/{}".format(name)
        self._delete(url)

    def online_predict(
        self, model_name: str, version_name: str = None, instances: List = None, args: Dict[str, Any] = None
    ) -> List:
        """Perform online prediction on a models active version or a specified version.

        Args:
            model_name (str):     Perform a prediction on the model with this name. Will use active version.
            version_name (str):   Use this version instead of the active version. (optional)
            instances (List): List of JSON serializable instances to pass to your model one-by-one.
            args (Dict[str, Any])    Dictinoary of keyword arguments to pass to your predict method.

        Returns:
            List: List of predictions for each instance.
        """
        url = "/modelhosting/models/{}/predict".format(model_name)
        if instances:
            for i, instance in enumerate(instances):
                if hasattr(instance, "dump"):
                    instances[i] = instance.dump()
        if version_name:
            url = "/modelhosting/models/{}/versions/{}/predict".format(model_name, version_name)
        body = {"instances": instances, "args": args or {}}
        res = self._post(url, json=body).json()
        if "error" in res:
            raise PredictionError(message=res["error"]["message"], code=res["error"]["code"])
        return res["predictions"]
