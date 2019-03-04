# -*- coding: utf-8 -*-
import json
from typing import List

import pandas as pd

from cognite.client._api_client import APIClient, CogniteResponse


class RawResponse(CogniteResponse):
    """Raw Response Object."""

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        return pd.DataFrame(self.to_json())


class RawRow(object):
    """DTO for a row in a raw database.

    The Raw API is a simple key/value-store. Each row in a table in a raw database consists of a
    unique row key and a set of columns.

    Args:
        key (str):      Unique key for the row.

        columns (dict):  A key/value-map consisting of the values in the row.
    """

    def __init__(self, key, columns):
        self.key = key
        self.columns = columns

    def __repr__(self):
        return json.dumps(self.repr_json())

    def repr_json(self):
        return self.__dict__


class RawClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.5", **kwargs)

    def get_databases(self, limit: int = None, cursor: str = None) -> RawResponse:
        """Returns a RawObject containing a list of raw databases.

        Args:
            limit (int):    A limit on the amount of results to return.

            cursor (str):   A cursor can be provided to navigate through pages of results.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/raw"
        params = {"limit": limit, "cursor": cursor}
        res = self._get(url=url, params=params, headers={"content-type": "*/*"})
        return RawResponse(res.json())

    def create_databases(self, database_names: list) -> RawResponse:
        """Creates databases in the Raw API and returns the created databases.

        Args:
            database_names (list):  A list of databases to create.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.

        """
        url = "/raw/create"
        body = {"items": [{"dbName": "{}".format(database_name)} for database_name in database_names]}
        res = self._post(url=url, body=body, headers={"content-type": "*/*"})
        return RawResponse(res.json())

    def delete_databases(self, database_names: list, recursive: bool = False) -> None:
        """Deletes databases in the Raw API.

        Args:
            database_names (list):  A list of databases to delete.

        Returns:
            None

        """
        url = "/raw/delete"
        body = {"items": [{"dbName": "{}".format(database_name)} for database_name in database_names]}
        params = {"recursive": recursive}
        self._post(url=url, body=body, params=params, headers={"content-type": "*/*"})

    def get_tables(self, database_name: str = None, limit: int = None, cursor: str = None) -> RawResponse:
        """Returns a RawObject containing a list of tables in a raw database.

        Args:
            database_name (str):   The database name to retrieve tables from.

            limit (int):    A limit on the amount of results to return.

            cursor (str):   A cursor can be provided to navigate through pages of results.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/raw/{}".format(database_name)
        params = dict()
        if limit is not None:
            params["limit"] = limit
        if cursor is not None:
            params["cursor"] = cursor
        res = self._get(url=url, params=params, headers={"content-type": "*/*"})
        return RawResponse(res.json())

    def create_tables(self, database_name: str = None, table_names: list = None) -> RawResponse:
        """Creates tables in the given Raw API database.

        Args:
            database_name (str):    The database to create tables in.

            table_names (list):     The table names to create.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.

        """
        url = "/raw/{}/create".format(database_name)
        body = {"items": [{"tableName": "{}".format(table_name)} for table_name in table_names]}
        res = self._post(url=url, body=body, headers={"content-type": "*/*"})
        return RawResponse(res.json())

    def delete_tables(self, database_name: str = None, table_names: list = None) -> None:
        """Deletes databases in the Raw API.

        Args:
            database_name (str):    The database to create tables in.

            table_names (list):     The table names to create.

        Returns:
            None

        """
        url = "/raw/{}/delete".format(database_name)
        body = {"items": [{"tableName": "{}".format(table_name)} for table_name in table_names]}
        self._post(url=url, body=body, headers={"content-type": "*/*"})

    def get_rows(
        self, database_name: str = None, table_name: str = None, limit: int = None, cursor: str = None
    ) -> RawResponse:
        """Returns a RawObject containing a list of rows.

        Args:
            database_name (str):    The database name to retrieve rows from.

            table_name (str):       The table name to retrieve rows from.

            limit (int):            A limit on the amount of results to return.

            cursor (str):           A cursor can be provided to navigate through pages of results.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/raw/{}/{}".format(database_name, table_name)
        params = dict()
        params["limit"] = limit
        params["cursor"] = cursor
        res = self._get(url=url, params=params, headers={"content-type": "*/*"})
        return RawResponse(res.json())

    def create_rows(
        self, database_name: str = None, table_name: str = None, rows: List[RawRow] = None, ensure_parent=False
    ) -> None:
        """Creates tables in the given Raw API database.

        Args:
            database_name (str):    The database to create rows in.

            table_name (str):       The table names to create rows in.

            rows (list[stable.raw.RawRow]):            The rows to create.

            ensure_parent (bool):   Create database/table if it doesn't exist already

        Returns:
            None

        """
        url = "/raw/{}/{}/create".format(database_name, table_name)

        params = {}
        if ensure_parent:
            params = {"ensureParent": "true"}

        ul_row_limit = 1000
        i = 0
        while i < len(rows):
            body = {
                "items": [{"key": "{}".format(row.key), "columns": row.columns} for row in rows[i : i + ul_row_limit]]
            }
            self._post(url=url, body=body, headers={"content-type": "*/*"}, params=params)
            i += ul_row_limit

    def delete_rows(self, database_name: str = None, table_name: str = None, rows: List[RawRow] = None) -> None:
        """Deletes rows in the Raw API.

        Args:
            database_name (str):    The database to create tables in.

            table_name (str):      The table name where the rows are at.

            rows (list):            The rows to delete.

        Returns:
            None

        """
        url = "/raw/{}/{}/delete".format(database_name, table_name)
        body = {"items": [{"key": "{}".format(row.key), "columns": row.columns} for row in rows]}
        self._post(url=url, body=body, headers={"content-type": "*/*"})

    def get_row(self, database_name: str = None, table_name: str = None, row_key: str = None) -> RawResponse:
        """Returns a RawObject containing a list of rows.

        Args:
            database_name (str):    The database name to retrieve rows from.

            table_name (str):       The table name to retrieve rows from.

            row_key (str):          The key of the row to fetch.

        Returns:
            stable.raw.RawResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/raw/{}/{}/{}".format(database_name, table_name, row_key)
        params = dict()
        res = self._get(url=url, params=params, headers={"content-type": "*/*"})
        return RawResponse(res.json())
