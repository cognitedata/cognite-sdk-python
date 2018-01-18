# -*- coding: utf-8 -*-
'''Project Configuration Module.

This module allows you to set an api-key and a project for your python project.
'''
from cognite._constants import BASE_URL

_CONFIG_API_KEY = ''
_CONFIG_PROJECT = ''
_CONFIG_BASE_URL = None

def configure_session(api_key='', project=''):
    '''Sets session variables.

    Args:
        api_key (str):  Api-key for current project.

        project (str):  Project name for current project.
    '''
    global _CONFIG_API_KEY, _CONFIG_PROJECT
    _CONFIG_API_KEY = api_key
    _CONFIG_PROJECT = project

def get_config_variables(api_key, project):
    '''Returns current project config variables unless other is specified.

    Args:
        api_key (str):  Other specified api-key.

        project (str):  Other specified project name.

    Returns:
        tuple: api-key and project name belonging to current project unless other is specified.
    '''
    if api_key is None:
        api_key = _CONFIG_API_KEY
    if project is None:
        project = _CONFIG_PROJECT
    return api_key, project

def set_base_url(url):
    '''Sets the base url for requests made from the SDK.

    Args:
        url (str):  URL to set. Set this to None to use default url.
    '''
    global _CONFIG_BASE_URL
    _CONFIG_BASE_URL = url

def get_base_url():
    '''Returns the current base url for requests made from the SDK.

    Returns:
        str: current base url.
    '''
    return BASE_URL if _CONFIG_BASE_URL is None else _CONFIG_BASE_URL