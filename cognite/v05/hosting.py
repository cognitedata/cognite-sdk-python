# -*- coding: utf-8 -*-
"""Assets Module.

This module mirrors the Assets API.

https://doc.cognitedata.com/0.5/#Cognite-API-Assets
"""
from cognite import _utils as utils
from cognite import config
from cognite.v05 import api_version


def get_models(**kwargs):
    """Returns hosted models."""
    api_key, project = config.get_session_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/project/{}/models".format(project)
    print(url)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return res.json()
