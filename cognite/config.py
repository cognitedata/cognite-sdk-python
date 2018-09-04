# -*- coding: utf-8 -*-
"""Project Configuration Module.

This module allows you to set an api-key and a project for your python project.
"""

import json
import os

from cognite_logger import cognite_logger

from cognite._constants import BASE_URL, CONFIG_PATH, RETRY_LIMIT

_CONFIG_API_KEY = os.getenv("COGNITE_API_KEY", "")
_CONFIG_PROJECT = os.getenv("COGNITE_PROJECT", "")
_CONFIG_BASE_URL = BASE_URL
_CONFIG_RETRIES = RETRY_LIMIT
_CONFIG_COOKIES = {}


def load_from_file():
    """Loads config into session from ~/.cognite.conf.json
    """
    global _CONFIG_API_KEY, _CONFIG_COOKIES, _CONFIG_PROJECT, _CONFIG_BASE_URL, _CONFIG_RETRIES

    config_dict = {"cognite": {}}
    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            config_dict = json.load(f)
    else:
        with open(CONFIG_PATH, "w+") as f:
            config_dict["cognite"]["api_key"] = os.getenv("COGNITE_API_KEY", "")
            config_dict["cognite"]["project"] = os.getenv("COGNITE_PROJECT", "")
            config_dict["cognite"]["base_url"] = BASE_URL
            config_dict["cognite"]["num_of_retries"] = RETRY_LIMIT
            json.dump(config_dict, f)

    _CONFIG_API_KEY = config_dict["cognite"].get("api_key")
    _CONFIG_PROJECT = config_dict["cognite"].get("project")
    _CONFIG_BASE_URL = config_dict["cognite"].get("base_url")
    _CONFIG_RETRIES = config_dict["cognite"].get("num_of_retries")
    _CONFIG_COOKIES = {}


def configure_session(api_key=None, project=None, cookies=None, base_url=None, num_retries=None, debug=False):
    """Sets session variables.

    Args:
        api_key (str):  Api-key for current project.

        cookies (dict): Cookies to pass with requests.

        project (str):  Project name for current project.

        debug (str): Whether or not to ouptut a debug log.
    """
    global _CONFIG_API_KEY, _CONFIG_COOKIES, _CONFIG_PROJECT, _CONFIG_BASE_URL, _CONFIG_RETRIES
    _CONFIG_API_KEY = api_key or _CONFIG_API_KEY
    _CONFIG_PROJECT = project or _CONFIG_PROJECT
    _CONFIG_COOKIES = cookies or _CONFIG_COOKIES
    _CONFIG_BASE_URL = base_url or _CONFIG_BASE_URL
    _CONFIG_RETRIES = num_retries or _CONFIG_RETRIES

    cognite_logger.configure_logger(logger_name="cognite-sdk", log_json=True, log_level="DEBUG" if debug else "INFO")


def get_session_config_variables(api_key, project):
    """Returns current project config variables unless other is specified.

    Args:
        api_key (str):  Other specified api-key.

        project (str):  Other specified project name.

    Returns:
        tuple: api-key and project name belonging to current project unless other is specified.
    """
    if api_key is None:
        api_key = _CONFIG_API_KEY
    if project is None:
        project = _CONFIG_PROJECT
    return api_key, project


def get_cookies(cookies=None):
    """Returns cookies set for the current session.

    Returns:
        dict: Cookies for current session.
    """
    if cookies is None:
        cookies = _CONFIG_COOKIES
    return cookies


def set_base_url(url=None):
    """Sets the base url for requests made from the SDK.

    Args:
        url (str):  URL to set. Set this to None to use default url.
    """
    global _CONFIG_BASE_URL
    _CONFIG_BASE_URL = url


def get_base_url(api_version=None):
    """Returns the current base url for requests made from the SDK.
    Args:
        api_version (float): Version of API to use for base_url
    Returns:
        str: current base url.
    """
    if api_version:
        return _CONFIG_BASE_URL + str(api_version)
    return _CONFIG_BASE_URL


def set_number_of_retries(retries: int = None):
    """Sets the number of retries attempted for requests made from the SDK.

    Args:
        retries (int):  Number of retries to attempt. Set this to None to use default num of retries.
    """
    global _CONFIG_RETRIES
    _CONFIG_RETRIES = retries


def get_number_of_retries():
    """Returns the current number of retries attempted for requests made from the SDK.

    Returns:
        int: current number of retries attempted for requests.
    """
    return _CONFIG_RETRIES
