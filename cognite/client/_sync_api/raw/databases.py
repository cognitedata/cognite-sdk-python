"""
===============================================================================
9c32bc5498a4bfa575cd60f771cf81e9
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.raw import Database, DatabaseList
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncRawDatabasesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Database]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[DatabaseList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Database] | Iterator[DatabaseList]:
        """
        Iterate over databases

        Fetches dbs as they are iterated over, so you keep a limited number of dbs in memory.

        Args:
            chunk_size: Number of dbs to return in each chunk. Defaults to yielding one db a time.
            limit: Maximum number of dbs to return. Defaults to return all items.

        Yields:
            No description.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.raw.databases(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def create(self, name: str) -> Database: ...

    @overload
    def create(self, name: list[str]) -> DatabaseList: ...

    def create(self, name: str | list[str]) -> Database | DatabaseList:
        """
        `Create one or more databases. <https://developer.cognite.com/api#tag/Raw/operation/createDBs>`_

        Args:
            name: A db name or list of db names to create.

        Returns:
            Database or list of databases that has been created.

        Examples:

            Create a new database:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.raw.databases.create("db1")
        """
        return run_sync(self.__async_client.raw.databases.create(name=name))

    def delete(self, name: str | SequenceNotStr[str], recursive: bool = False) -> None:
        """
        `Delete one or more databases. <https://developer.cognite.com/api#tag/Raw/operation/deleteDBs>`_

        Args:
            name: A db name or list of db names to delete.
            recursive: Recursively delete all tables in the database(s).

        Examples:

            Delete a list of databases:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.raw.databases.delete(["db1", "db2"])
        """
        return run_sync(self.__async_client.raw.databases.delete(name=name, recursive=recursive))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> DatabaseList:
        """
        `List databases <https://developer.cognite.com/api#tag/Raw/operation/getDBs>`_

        Args:
            limit: Maximum number of databases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            List of requested databases.

        Examples:

            List the first 5 databases:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> db_list = client.raw.databases.list(limit=5)

            Iterate over databases, one-by-one:

                >>> for db in client.raw.databases():
                ...     db  # do something with the db

            Iterate over chunks of databases to reduce memory load:

                >>> for db_list in client.raw.databases(chunk_size=2500):
                ...     db_list # do something with the dbs
        """
        return run_sync(self.__async_client.raw.databases.list(limit=limit))
