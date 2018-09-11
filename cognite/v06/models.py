# -*- coding: utf-8 -*-
"""Models Module.

This module mirrors the Models API.

https://doc.cognitedata.com/0.6/models
"""
from cognite import _utils as utils
from cognite import config


def create_model(name, description="", **kwargs):
    """Create a new hosted models."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    # url = config.get_base_url(0.6) + "/project/{}/models".format(project)
    url = "http://localhost:8000/api/0.1/project/{}/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    model_body = {"name": name, "description": description}
    res = utils.post_request(url, body=model_body, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_models(**kwargs):
    """Returns hosted models."""
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    # url = config.get_base_url(0.6) + "/project/{}/models".format(project)
    url = "http://localhost:8000/api/0.1/project/{}/models".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()


def get_versions(model_id, **kwargs):
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    # url = config.get_base_url(0.6) + "/project/{}/models".format(project)
    url = "http://localhost:8000/api/0.1/project/{}/models/{}/versions".format(project, model_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()
