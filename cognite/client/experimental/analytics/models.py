# -*- coding: utf-8 -*-
from typing import Any, Dict, List

import requests

from cognite.client._api_client import APIClient


class ModelsClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)

    def create_model(
        self,
        name: str,
        description: str = "",
        metadata: Dict[str, Any] = None,
        input_fields: List[str] = None,
        output_fields: List[str] = None,
    ) -> Dict:
        """Creates a new model

        Args:
            name (str):             Name of model
            description (str):      Description
            metadata (Dict[str, Any]):          Metadata about model
            input_fields (List[str]):   List of input fields the model accepts
            output_fields (List[str]:   List of output fields the model produces

        Returns:
            Dict: The created model.
        """
        url = "/analytics/models"
        model_body = {
            "name": name,
            "description": description,
            "metadata": metadata or {},
            "inputFields": input_fields or [],
            "outputFields": output_fields or [],
        }
        res = self._post(url, body=model_body)
        return res.json()

    def get_models(self) -> List[Dict]:
        """Get all models.

        Returns:
            List[Dict]: List of models
        """
        url = "/analytics/models"
        res = self._get(url)
        return res.json()

    def get_model(self, model_id: int) -> Dict:
        """Get a model by id.

        Args:
            model_id (int): Id of model to get.

        Returns:
            Dict: The requested model
        """
        url = "/analytics/models/{}".format(model_id)
        res = self._get(url)
        return res.json()

    def delete_model(self, model_id: int) -> None:
        """Delete a model.

        Will also delete all versions and schedules for this model.

        Args:
            model_id (int): Delete model with this id.

        Returns:
            None
        """
        url = "/analytics/models/{}".format(model_id)
        self._delete(url)

    def train_model_version(
        self,
        model_id: int,
        name: str,
        source_package_id: int,
        train_source_package_id: int = None,
        metadata: Dict = None,
        description: str = None,
        args: Dict[str, Any] = None,
        scale_tier: str = None,
        machine_type: str = None,
    ) -> Dict:
        """Train a new version of a model.

        Args:
            model_id (int): Create a new version under the model with this id
            name (str): Name of model version. Must be unique on the model.
            source_package_id (int):    Use the source package with this id
            train_source_package_id (int):  Use this source package for training. If omitted, will default to
                                            source_package_id.
            metadata (Dict[str, Any]):  Metadata about model version
            description (str):  Description of model version
            args (Dict[str, Any]):   Dictionary of arguments to pass to the training job.
            scale_tier (str):   Which scale tier to use. Must be either "BASIC" or "CUSTOM"
            machine_type (str): Specify a machiene type Applies only if scale_tier is "CUSTOM".

        Returns:
            Dict: The created model version.
        """
        url = "/analytics/models/{}/versions/train".format(model_id)
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
        res = self._post(url)
        return res.json()

    def get_versions(self, model_id: int) -> List[Dict]:
        """Get all versions of a specific model.

        Args:
            model_id (int): Get versions for the model with this id.

        Returns:
            List[Dict]: List of model versions
        """
        url = "/analytics/models/{}/versions".format(model_id)
        res = self._get(url)
        return res.json()

    def get_version(self, model_id: int, version_id: int) -> Dict:
        """Get a specific model version by id.

        Args:
            model_id (int): Id of model which has the model version.
            version_id (int): Id of model version.

        Returns:
            Dict: The requested model version
        """
        url = "/analytics/models/{}/versions/{}".format(model_id, version_id)
        res = self._get(url)
        return res.json()

    def delete_version(self, model_id: int, version_id: int) -> None:
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
        if version_id:
            url = "/analytics/models/{}/versions/{}/predict".format(model_id, version_id)
        body = {"instances": instances, "args": args or {}}
        res = self._put(url, body=body)
        return res.json()

    def create_source_package(
        self,
        name: str,
        package_name: str,
        available_operations: List[str],
        runtime_version: str,
        description: str = None,
        meta_data: Dict = None,
        file_path: str = None,
    ) -> Dict:
        """Upload a source package to the model hosting environment.

        Args:
            name (str): Name of source package
            package_name (str): name of root package for model
            available_operations (List[str]): List of routines which this source package supports ["predict", "train"]
            runtime_version (str): Version of environment in which the source-package should run. Currently only 0.1.
            description (str): Description for source package
            meta_data (Dict): User defined key value pair of additional information.
            file_path (str): File path of source package distribution. If not specified, a download url will be returned.

        Returns:
            Dict: Source package ID if file path was specified. Else, source package id and upload url.

        """
        url = "/analytics/models/sourcepackages"
        body = {
            "name": name,
            "description": description or "",
            "packageName": package_name,
            "availableOperations": available_operations,
            "metadata": meta_data or {},
            "runtimeVersion": runtime_version,
        }
        res = self._post(url, body=body)
        if file_path:
            self._upload_file(res.json().get("uploadUrl"), file_path)
            del res.json()["uploadUrl"]
            return res.json()
        return res.json()

    def _upload_file(self, upload_url, file_path):
        with open(file_path, "rb") as fh:
            mydata = fh.read()
            response = requests.put(upload_url, data=mydata)
        return response

    def get_source_packages(self) -> List[Dict]:
        """Get all model source packages.

        Returns:
            List[Dict]: List of source packages.
        """
        url = "/analytics/models/sourcepackages"
        res = self._get(url)
        return res.json()

    def get_source_package(self, source_package_id: int) -> Dict:
        """Get model source package by id.

        Args:
            source_package_id (int): Id of soure package to get.

        Returns:
            Dict: The requested source package.
        """
        url = "/analytics/models/sourcepackages/{}".format(source_package_id)
        res = self._get(url)
        return res.json()

    def delete_source_package(self, source_package_id: int) -> None:
        """Delete source package by id.

        Args:
            source_package_id (int): Id of soure package to delete.

        Returns:
            None
        """
        url = "/analytics/models/sourcepackages/{}".format(source_package_id)
        self._delete(url)

    def create_schedule(
        self,
        model_id: int,
        name: str,
        output_data_spec: Dict,
        input_data_spec: Dict,
        description: str = None,
        args: Dict = None,
        metadata: Dict = None,
    ) -> Dict:
        """Create a new schedule on a given model.

        Args:
            model_id (int): Id of model to create schedule on
            name (str): Name of schedule
            output_data_spec (Dict): Specification of output. Example below.
            input_data_spec (Dict): Specification of input. Example below.
            description (str): Description for schedule
            args (Dict): Dictionary of keyword arguments to pass to predict method.
            metadata (Dict): Dictionary of metadata about schedule

        Returns:
            Dict: The created schedule.

        Examples
            The output data spec must look like this::

                {
                    "timeSeries": [
                        {
                            "label": "string",
                            "id": 123456789
                        }
                    ]
                }

            The input data spec must look like this. The local aggregate and the missingDataStrategy fields are optional::

                {
                    "windowSize": "1s",
                    "stride": "1s",
                    "missingDataStrategy": "",
                    "timeSeries": [
                      {
                        "label": "string",
                        "id": 0,
                        "missingDataStrategy": "string",
                        "aggregate": "string"
                      }
                    ],
                    "aggregate": "string",
                    "granularity": "string"
                }
        """
        url = "/analytics/models/schedules"
        body = {
            "name": name,
            "description": description,
            "modelId": model_id,
            "args": args or {},
            "inputDataSpec": input_data_spec,
            "outputDataSpec": output_data_spec,
            "metadata": metadata or {},
        }
        res = self._post(url, body=body)
        return res.json()

    def delete_schedule(self, schedule_id: int) -> None:
        """Delete a schedule by id.

        Args:
            schedule_id (int):  The id of the schedule to delete.

        Returns:
            None
        """
        url = "/analytics/models/schedules/{}".format(schedule_id)
        self._delete(url=url)

    def get_schedules(self) -> List[Dict]:
        """Get all schedules.

        Returns:
            List[Dict]: The requested schedules.
        """
        url = "/analytics/models/schedules"
        res = self._get(url=url)
        return res.json()

    def get_schedule(self, schedule_id: int) -> Dict:
        """Get a schedule by id.

        Returns:
            Dict: The requested schedule.
        """
        url = "/analytics/models/schedules/{}".format(schedule_id)
        res = self._get(url=url)
        return res.json()
