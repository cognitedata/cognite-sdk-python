# -*- coding: utf-8 -*-
"""Models Module.

This module mirrors the Models API.

https://doc.cognitedata.com/0.6/models
"""
import requests

from cognite import _utils as utils
from cognite import config


def create_model(name, description="", **kwargs):
    """Create a new hosted models

    Args:
        name (str):             Name of model
        description (str):      Description

    Returns:
        The created model.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    model_body = {"name": name, "description": description}
    res = utils.post_request(url, body=model_body, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_models(**kwargs):
    """Get all models."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models".format(project)
    # url = "http://localhost:8000/api/0.1/project/{}/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_model_versions(model_id, **kwargs):
    """Get all versions of a specific model."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models/{}/versions".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def delete_model(model_id, **kwargs):
    """Delete a model."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models/{}".format(project, model_id)
    # url = "http://localhost:8000/api/0.1/project/{}/models/{}".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.delete_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def train_model_version(
    model_id,
    name,
    description=None,
    predict_source_package_id=None,
    train_source_package_id=None,
    data_spec=None,
    args=None,
    scale_tier=None,
    machine_type=None,
    **kwargs
):
    """Train a new version of a model."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models/{}/versions/train".format(project, model_id)
    body = {
        "name": name,
        "description": description or "",
        "source_package_id": predict_source_package_id,
        "training_details": {
            "source_package_id": train_source_package_id or predict_source_package_id,
            "data_spec": data_spec.to_JSON() if data_spec else None,
            "args": args or {},
            "scale_tier": scale_tier,
            "machine_type": machine_type,
        },
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def online_predict(model_id, version_id=None, instances=None, arguments=None, data_spec=None, **kwargs):
    """Perform online prediction on a models active version or a specified version."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    if version_id:
        url = config.get_base_url() + "/api/0.6/projects/{}/models/{}/versions/{}/predict".format(
            project, model_id, version_id
        )
    else:
        url = config.get_base_url() + "/api/0.6/projects/{}/models/{}/predict".format(project, model_id)

    body = {"instances": instances, "arguments": arguments or {}, "data_spec": data_spec.to_JSON() if data_spec else {}}
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.put_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_model_source_packages(**kwargs):
    """Get all model source packages."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models/sourcepackages".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def upload_source_package(
    name, description, package_name, available_operations, meta_data=None, file_path=None, **kwargs
):
    """Upload a source package to the model hosting environment.

    Args:
        name: Name of source package
        description: Description for source package
        package_name: name of root package for model
        available_operations: List of routines which this source package supports ["predict", "train"]
        meta_data: User defined key value pair of additional information.
        file_path (str): File path of source package distribution. If not sepcified, a download url will be returned.
        **kwargs:

    Returns:
        Source package ID if file path was specified. Else, source package id and upload url.

    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/models/sourcepackages".format(project)
    body = {
        "name": name,
        "description": description or "",
        "package_name": package_name,
        "available_operations": available_operations,
        "meta_data": meta_data or {},
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    if file_path:
        _upload_file(res.json().get("upload_url"), file_path)
        return res.json().get("id")
    return res.json()


def _upload_file(upload_url, file_path):
    with open(file_path, "rb") as fh:
        mydata = fh.read()
        response = requests.put(upload_url, data=mydata, params={"file": file_path})
    return response
