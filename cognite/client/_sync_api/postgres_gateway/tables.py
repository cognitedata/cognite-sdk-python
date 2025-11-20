"""
===============================================================================
789d58776aa258529e2d0c9453d9e029
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

import cognite.client.data_classes.postgres_gateway.tables as pg
from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncTablesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[pg.Table]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[pg.TableList]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[pg.Table | pg.TableList]:
        """
        Iterate over custom tables

        Fetches custom tables as they are iterated over, so you keep a limited number of custom tables in memory.

        Args:
            chunk_size (int | None): Number of custom tables to return in each chunk. Defaults to yielding one custom table at a time.
            limit (int | None): Maximum number of custom tables to return. Defaults to return all.

        Yields:
            pg.Table | pg.TableList: yields Table one by one if chunk_size is not specified, else TableList objects.
        """
        yield from SyncIterator(self.__async_client.postgres_gateway.tables(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def create(self, username: str, items: pg.TableWrite) -> pg.Table: ...

    @overload
    def create(self, username: str, items: Sequence[pg.TableWrite]) -> pg.TableList: ...

    def create(self, username: str, items: pg.TableWrite | Sequence[pg.TableWrite]) -> pg.Table | pg.TableList:
        """
        `Create tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/create_tables>`_

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            items (pg.TableWrite | Sequence[pg.TableWrite]): The table(s) to create

        Returns:
            pg.Table | pg.TableList: Created tables

        Examples:

            Create custom table:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.postgres_gateway import ViewTableWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> table = ViewTableWrite(tablename="myCustom", options=ViewId(space="mySpace", external_id="myExternalId", version="v1"))
                >>> res = client.postgres_gateway.tables.create("myUserName",table)
        """
        return run_sync(self.__async_client.postgres_gateway.tables.create(username=username, items=items))

    @overload
    def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[False] = False) -> pg.Table: ...

    @overload
    def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[True]) -> pg.Table | None: ...

    @overload
    def retrieve(
        self, username: str, tablename: SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.TableList: ...

    def retrieve(
        self, username: str, tablename: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.Table | pg.TableList | None:
        """
        `Retrieve a list of tables by their tables names <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/retrieve_tables>`_

        Retrieve a list of Postgres tables for a user by their table names, optionally ignoring unknown table names

        Args:
            username (str): The username (a.k.a. database) to be managed from the API
            tablename (str | SequenceNotStr[str]): The name of the table(s) to be retrieved
            ignore_unknown_ids (bool): Ignore table names not found

        Returns:
            pg.Table | pg.TableList | None: Foreign tables

        Examples:

            Retrieve  custom table:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.postgres_gateway.tables.retrieve("myUserName", 'myCustom')

            Get multiple custom tables by id:

                >>> res = client.postgres_gateway.tables.retrieve("myUserName", ["myCustom", "myCustom2"])
        """
        return run_sync(
            self.__async_client.postgres_gateway.tables.retrieve(
                username=username,
                tablename=tablename,  # type: ignore [arg-type]
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )

    def delete(self, username: str, tablename: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete postgres table(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/delete_tables>`_

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            tablename (str | SequenceNotStr[str]): The name of the table(s) to be deleted
            ignore_unknown_ids (bool): Ignore table names that are not found

        Examples:

            Delete custom table:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.postgres_gateway.tables.delete("myUserName", ["myCustom", "myCustom2"])
        """
        return run_sync(
            self.__async_client.postgres_gateway.tables.delete(
                username=username, tablename=tablename, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(
        self,
        username: str,
        include_built_ins: Literal["yes", "no"] | None = "no",
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> pg.TableList:
        """
        `List postgres tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/list_tables>`_

        List all tables in a given project.

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            include_built_ins (Literal['yes', 'no'] | None): Determines if API should return built-in tables or not
            limit (int | None): Limits the number of results to be returned.

        Returns:
            pg.TableList: Foreign tables

        Examples:

            List tables:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> custom_table_list = client.postgres_gateway.tables.list("myUserName", limit=5)

            Iterate over tables, one-by-one:

                >>> for table in client.postgres_gateway.tables():
                ...     table  # do something with the custom table

            Iterate over chunks of tables to reduce memory load:

                >>> for table_list in client.postgres_gateway.tables(chunk_size=25):
                ...     table_list # do something with the custom tables
        """
        return run_sync(
            self.__async_client.postgres_gateway.tables.list(
                username=username, include_built_ins=include_built_ins, limit=limit
            )
        )
