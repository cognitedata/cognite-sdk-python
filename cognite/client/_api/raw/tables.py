from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import raw
from cognite.client.utils._auxiliary import split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._url import interpolate_and_url_encode
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"

    @overload
    async def __call__(
        self, db_name: str, chunk_size: None = None, limit: int | None = None
    ) -> AsyncIterator[raw.Table]: ...

    @overload
    async def __call__(
        self, db_name: str, chunk_size: int, limit: int | None = None
    ) -> AsyncIterator[raw.TableList]: ...

    async def __call__(
        self, db_name: str, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[raw.Table] | AsyncIterator[raw.TableList]:
        """Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int | None): Number of tables to return in each chunk. Defaults to yielding one table a time.
            limit (int | None): Maximum number of tables to return. Defaults to return all items.

        Returns:
            AsyncIterator[raw.Table] | AsyncIterator[raw.TableList]: No description.
        """
        table_iterator = self._list_generator(
            list_cls=raw.TableList,
            resource_cls=raw.Table,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        )
        return self._set_db_name_on_tables_generator(table_iterator, db_name)

    @overload
    async def create(self, db_name: str, name: str) -> raw.Table: ...

    @overload
    async def create(self, db_name: str, name: list[str]) -> raw.TableList: ...

    async def create(self, db_name: str, name: str | list[str]) -> raw.Table | raw.TableList:
        """`Create one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/createTables>`_

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
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: dict[str, Any] | list[dict[str, Any]] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        tb = self._create_multiple(
            list_cls=raw.TableList,
            resource_cls=raw.Table,
            input_resource_cls=raw.TableWrite,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            items=items,
        )
        return self._set_db_name_on_tables(tb, db_name)

    async def delete(self, db_name: str, name: str | SequenceNotStr[str]) -> None:
        """`Delete one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/deleteTables>`_

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
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]

        url_path = interpolate_and_url_encode(self._RESOURCE_PATH, db_name) + "/delete"
        tasks = [
            AsyncSDKTask(self._post, url_path=url_path, json={"items": [{"name": n} for n in chunk]})
            for chunk in split_into_chunks(name, self._DELETE_LIMIT)
        ]
        summary = await execute_async_tasks(tasks)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda el: el["name"]
        )

    def _set_db_name_on_tables(self, tb: raw.Table | raw.TableList, db_name: str) -> raw.Table | raw.TableList:
        if isinstance(tb, raw.Table):
            tb._db_name = db_name
            return tb
        elif isinstance(tb, raw.TableList):
            for t in tb:
                t._db_name = db_name
            return tb
        raise TypeError("tb must be raw.Table or raw.TableList")

    async def _set_db_name_on_tables_generator(
        self, table_iterator: AsyncIterator[raw.Table] | AsyncIterator[raw.TableList], db_name: str
    ) -> AsyncIterator[raw.Table] | AsyncIterator[raw.TableList]:
        async for tbl in table_iterator:
            yield self._set_db_name_on_tables(tbl, db_name)

    async def list(self, db_name: str, limit: int | None = DEFAULT_LIMIT_READ) -> raw.TableList:
        """`List tables <https://developer.cognite.com/api#tag/Raw/operation/getTables>`_

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
        tb = await self._list(
            list_cls=raw.TableList,
            resource_cls=raw.Table,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )
        return cast(raw.TableList, self._set_db_name_on_tables(tb, db_name))
