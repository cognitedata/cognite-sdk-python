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
    url = config.get_base_url() + "/api/0.6/project/{}/models".format(project)
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
