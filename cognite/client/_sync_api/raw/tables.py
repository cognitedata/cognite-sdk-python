"""
===============================================================================
d12a2c456dd92ebcfbac60d16f5ea51b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import raw
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncRawTablesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, db_name: str, chunk_size: None = None) -> Iterator[raw.Table]: ...

    @overload
    def __call__(self, db_name: str, chunk_size: int) -> Iterator[raw.TableList]: ...

    def __call__(
        self, db_name: str, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[raw.Table | raw.TableList]:
        """
        Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int | None): Number of tables to return in each chunk. Defaults to yielding one table a time.
            limit (int | None): Maximum number of tables to return. Defaults to return all items.

        Yields:
            raw.Table | raw.TableList: The tables in the database.
        """
        yield from SyncIterator(self.__async_client.raw.tables(db_name=db_name, chunk_size=chunk_size, limit=limit))  # type: ignore [call-overload]

    @overload
    def create(self, db_name: str, name: str) -> raw.Table: ...

    @overload
    def create(self, db_name: str, name: list[str]) -> raw.TableList: ...

    def create(self, db_name: str, name: str | list[str]) -> raw.Table | raw.TableList:
        """
        `Create one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/createTables>`_

        Args:
            db_name (str): Database to create the tables in.
            name (str | list[str]): A table name or list of table names to create.

        Returns:
            raw.Table | raw.TableList: raw.Table or list of tables that has been created.

        Examples:

            Create a new table in a database:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.raw.tables.create("db1", "table1")
        """
        return run_sync(self.__async_client.raw.tables.create(db_name=db_name, name=name))

    def delete(self, db_name: str, name: str | SequenceNotStr[str]) -> None:
        """
        `Delete one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/deleteTables>`_

        Args:
            db_name (str): Database to delete tables from.
            name (str | SequenceNotStr[str]): A table name or list of table names to delete.

        Examples:

            Delete a list of tables:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.raw.tables.delete("db1", ["table1", "table2"])
        """
        return run_sync(self.__async_client.raw.tables.delete(db_name=db_name, name=name))

    def list(self, db_name: str, limit: int | None = DEFAULT_LIMIT_READ) -> raw.TableList:
        """
        `List tables <https://developer.cognite.com/api#tag/Raw/operation/getTables>`_

        Args:
            db_name (str): The database to list tables from.
            limit (int | None): Maximum number of tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            raw.TableList: List of requested tables.

        Examples:

            List the first 5 tables:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> table_list = client.raw.tables.list("db1", limit=5)

            Iterate over tables, one-by-one:

                >>> for table in client.raw.tables(db_name="db1"):
                ...     table  # do something with the table

            Iterate over chunks of tables to reduce memory load:

                >>> for table_list in client.raw.tables(db_name="db1", chunk_size=25):
                ...     table_list # do something with the tables
        """
        return run_sync(self.__async_client.raw.tables.list(db_name=db_name, limit=limit))
