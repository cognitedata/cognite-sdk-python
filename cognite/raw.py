# -*- coding: utf-8 -*-
"""Raw Module

This module mirrors the Raw API. It allows the user to handle raw data.
"""

import cognite.config as config
import cognite._utils as _utils

from typing import List
from cognite.data_objects import RawObject, RawRowDTO


def get_databases(
        limit: int=None,
        cursor: str=None,
        api_key=None,
        project=None
):
    """Returns a RawObject containing a list of raw databases.

    Args:
        limit (int):    A limit on the amount of results to return.

        cursor (str):   A cursor can be provided to navigate through pages of results.

        api_key (str):  Your api-key.

        project (str):  Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw'.format(project)
    params = dict()
    if not limit:
        params['limit'] = limit
    if not cursor:
        params['cursor'] = cursor
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.get_request(url=url, params=params, headers=headers)
    return RawObject(res.json())


def create_databases(
        database_names: list,
        api_key=None,
        project=None
):
    """Creates databases in the Raw API and returns the created databases.

    Args:
        database_names (list):  A list of databases to create.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/create'.format(project)
    body = {
        'items': [{'dbName': '{}'.format(database_name)} for database_name in database_names]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return RawObject(res.json())


def delete_databases(
        database_names: list,
        api_key=None,
        project=None
):
    """Deletes databases in the Raw API.

    Args:
        database_names (list):  A list of databases to delete.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        An empty response.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/delete'.format(project)
    body = {
        'items': [{'dbName': '{}'.format(database_name)} for database_name in database_names]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return res # todo: return something else? Returning RawObject(res.json()) doesn't work


def get_tables(
        database_name: str=None,
        limit: int=None,
        cursor: str=None,
        api_key=None,
        project=None
):
    """Returns a RawObject containing a list of tables in a raw database.

    Args:
        database_name (str):   The database name to retrieve tables from.

        limit (int):    A limit on the amount of results to return.

        cursor (str):   A cursor can be provided to navigate through pages of results.

        api_key (str):  Your api-key.

        project (str):  Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}'.format(project, database_name)
    params = dict()
    if not limit:
        params['limit'] = limit
    if not cursor:
        params['cursor'] = cursor
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.get_request(url=url, params=params, headers=headers)
    return RawObject(res.json())


def create_tables(
        database_name: str=None,
        table_names: list=None,
        api_key=None,
        project=None
):
    """Creates tables in the given Raw API database.

    Args:
        database_name (str):    The database to create tables in.

        table_names (list):     The table names to create.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/create'.format(project, database_name)
    body = {
        'items': [{'tableName': '{}'.format(table_name)} for table_name in table_names]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return RawObject(res.json())


def delete_tables(
        database_name: str=None,
        table_names: list=None,
        api_key=None,
        project=None
):
    """Deletes databases in the Raw API.

    Args:
        database_name (str):    The database to create tables in.

        table_names (list):     The table names to create.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        An empty response.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/delete'.format(project, database_name)
    body = {
        'items': [{'tableName': '{}'.format(table_name)} for table_name in table_names]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return res # todo: return something else? Returning RawObject(res.json()) doesn't work


def get_rows(
        database_name: str=None,
        table_name: str=None,
        limit: int=None,
        cursor: str=None,
        api_key=None,
        project=None
):
    """Returns a RawObject containing a list of rows.

    Args:
        database_name (str):    The database name to retrieve rows from.

        table_name (str):       The table name to retrieve rows from.

        limit (int):            A limit on the amount of results to return.

        cursor (str):           A cursor can be provided to navigate through pages of results.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/{}'.format(project, database_name, table_name)
    params = dict()
    if not limit:
        params['limit'] = limit
    if not cursor:
        params['cursor'] = cursor
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.get_request(url=url, params=params, headers=headers)
    return RawObject(res.json())


def create_rows(
        database_name: str=None,
        table_name: str=None,
        rows: List[RawRowDTO]=None,
        api_key=None,
        project=None,
        ensure_parent=False,
        use_gzip=False
):
    """Creates tables in the given Raw API database.

    Args:
        database_name (str):    The database to create rows in.

        table_name (str):       The table names to create rows in.

        rows (list):            The rows to create.

        api_key (str):          Your api-key.

        project (str):          Project name.

        ensure_parent (bool):   Create database/table if it doesn't exist already

        use_gzip (bool):        Compress content using gzip

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/{}/create'.format(project, database_name, table_name)
    body = {
        'items': [
            {
                'key': '{}'.format(row.key),
                'columns': row.columns
            }
            for row in rows
        ]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    if ensure_parent:
        params = {'ensureParent': 'true'}
    else:
        params = {}
    res = _utils.post_request(url=url, body=body, headers=headers, params=params, use_gzip=use_gzip)
    return RawObject(res.json())


def delete_rows(
        database_name: str=None,
        table_name: str=None,
        rows: List[RawRowDTO]=None,
        api_key=None,
        project=None
):
    """Deletes rows in the Raw API.

    Args:
        database_name (str):    The database to create tables in.

        table_name (str):      The table name where the rows are at.

        rows (list):            The rows to delete.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        An empty response.

    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/{}/delete'.format(project, database_name, table_name)
    body = {
        'items': [
            {
                'key': '{}'.format(row.key),
                'columns': row.columns
            }
            for row in rows
        ]
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return res # todo: return something else? Returning RawObject(res.json()) doesn't work


def get_row(
        database_name: str=None,
        table_name: str=None,
        row_key: str=None,
        api_key=None,
        project=None
):
    """Returns a RawObject containing a list of rows.

    Args:
        database_name (str):    The database name to retrieve rows from.

        table_name (str):       The table name to retrieve rows from.

        row_key (str):          The key of the row to fetch.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        RawObject: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/raw/{}/{}/{}'.format(project, database_name, table_name, row_key)
    params = dict()
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.get_request(url=url, params=params, headers=headers)
    return RawObject(res.json())
