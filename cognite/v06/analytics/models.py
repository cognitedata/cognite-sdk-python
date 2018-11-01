# -*- coding: utf-8 -*-
"""Models Module.

This module mirrors the Models API.

https://doc.cognitedata.com/0.6/models
"""
from typing import Any, Dict, List

import requests

from cognite import _utils as utils
from cognite import config


def create_model(
    name: str,
    description: str = "",
    metadata: Dict[str, Any] = None,
    input_fields: List[str] = None,
    output_fields: List[str] = None,
    **kwargs,
):
    """Creates a new model

    Args:
        name (str):             Name of model
        description (str):      Description
        metadata (Dict[str, Any]):          Metadata about model
        input_fields (List[str]):   List of input fields the model accepts
        output_fields (List[str]:   List of output fields the model produces

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The created model.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    model_body = {
        "name": name,
        "description": description,
        "metadata": metadata or {},
        "inputFields": input_fields or [],
        "outputFields": output_fields or [],
    }
    res = utils.post_request(url, body=model_body, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_models(**kwargs):
    """Get all models.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        List[Dict]: List of models
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_model(model_id: int, **kwargs):
    """Get a model by id.

    Args:
        model_id (int): Id of model to get.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The requested model
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def delete_model(model_id: int, **kwargs):
    """Delete a model.

    Will also delete all versions and schedules for this model.

    Args:
        model_id (int): Delete model with this id.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: Empty Response
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.delete_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def train_model_version(
    model_id: int,
    name: str,
    source_package_id: int,
    train_source_package_id: int = None,
    metadata: Dict = None,
    description: str = None,
    args: Dict[str, Any] = None,
    scale_tier: str = None,
    machine_type: str = None,
    **kwargs,
):
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

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The created model version.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/versions/train".format(project, model_id)
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
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_versions(model_id: int, **kwargs):
    """Get all versions of a specific model.

    Args:
        model_id (int): Get versions for the model with this id.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        List[Dict]: List of model versions
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/versions".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_version(model_id: int, version_id: int, **kwargs):
    """Get a specific model version by id.

    Args:
        model_id (int): Id of model which has the model version.
        version_id (int): Id of model version.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The requested model version
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/versions/{}".format(
        project, model_id, version_id
    )
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def delete_version(model_id: int, version_id: int, **kwargs):
    """Delete a model version by id.

    Args:
        model_id (int): Id of model which has the model version.
        version_id (int): Id of model version.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The requested model version
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/versions/{}".format(
        project, model_id, version_id
    )
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.delete_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def online_predict(
    model_id: int, version_id: int = None, instances: List = None, args: Dict[str, Any] = None, **kwargs
):
    """Perform online prediction on a models active version or a specified version.

    Args:
        model_id (int):     Perform a prediction on the model with this id. Will use active version.
        version_id (int):   Use this version instead of the active version. (optional)
        instances (List): List of JSON serializable instances to pass to your model one-by-one.
        args (Dict[str, Any])    Dictinoary of keyword arguments to pass to your predict method.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        List: List of predictions for each instance.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    if version_id:
        url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/versions/{}/predict".format(
            project, model_id, version_id
        )
    else:
        url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/{}/predict".format(project, model_id)

    body = {"instances": instances, "args": args or {}}
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.put_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def create_source_package(
    name: str,
    package_name: str,
    available_operations: List[str],
    runtime_version: str,
    description: str = None,
    meta_data: Dict = None,
    file_path: str = None,
    **kwargs,
):
    """Upload a source package to the model hosting environment.

    Args:
        name (str): Name of source package
        package_name (str): name of root package for model
        available_operations (List[str]): List of routines which this source package supports ["predict", "train"]
        runtime_version (str): Version of environment in which the source-package should run. Currently only 0.1.
        description (str): Description for source package
        meta_data (Dict): User defined key value pair of additional information.
        file_path (str): File path of source package distribution. If not specified, a download url will be returned.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: Source package ID if file path was specified. Else, source package id and upload url.

    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/sourcepackages".format(project)
    body = {
        "name": name,
        "description": description or "",
        "packageName": package_name,
        "availableOperations": available_operations,
        "metadata": meta_data or {},
        "runtimeVersion": runtime_version,
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    if file_path:
        _upload_file(res.json().get("uploadUrl"), file_path)
        del res.json()["uploadUrl"]
        return res.json()
    return res.json()


def _upload_file(upload_url, file_path):
    with open(file_path, "rb") as fh:
        mydata = fh.read()
        response = requests.put(upload_url, data=mydata)
    return response


def get_source_packages(**kwargs):
    """Get all model source packages.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        List[Dict]: List of source packages.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/sourcepackages".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_source_package(source_package_id: int, **kwargs):
    """Get model source package by id.

    Args:
        source_package_id (int): Id of soure package to get.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The requested source package.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/sourcepackages/{}".format(
        project, source_package_id
    )
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def delete_source_package(source_package_id: int, **kwargs):
    """Delete source package by id.

    Args:
        source_package_id (int): Id of soure package to delete.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: Empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/sourcepackages/{}".format(
        project, source_package_id
    )
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.delete_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def create_schedule(
    model_id: int,
    name: str,
    output_data_spec: Dict,
    input_data_spec: Dict,
    description: str = None,
    args: Dict = None,
    metadata: Dict = None,
    **kwargs,
):
    """Create a new schedule on a given model.

    Args:
        model_id (int): Id of model to create schedule on
        name (str): Name of schedule
        output_data_spec (Dict): Specification of output. Example below.
        input_data_spec (Dict): Specification of input. Example below.
        description (str): Description for schedule
        args (Dict): Dictionary of keyword arguments to pass to predict method.
        metadata (Dict): Dictionary of metadata about schedule

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

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
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/schedules".format(project)
    body = {
        "name": name,
        "description": description,
        "modelId": model_id,
        "args": args or {},
        "inputDataSpec": input_data_spec,
        "outputDataSpec": output_data_spec,
        "metadata": metadata or {},
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def delete_schedule(schedule_id: int, **kwargs):
    """Delete a schedule by id.

    Args:
        schedule_id (int):  The id of the schedule to delete.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: Empty response
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/schedules/{}".format(project, schedule_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.delete_request(url=url, headers=headers)
    return res.json()


def get_schedules(**kwargs):
    """Get all schedules.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        List[Dict]: The requested schedules.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/schedules".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url=url, headers=headers)
    return res.json()


def get_schedule(schedule_id: int, **kwargs):
    """Get a schedule by id.

    Keyword Arguments:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        Dict: The requested schedule.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/analytics/models/schedules/{}".format(project, schedule_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url=url, headers=headers)
    return res.json()
