from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.raw import Database, DatabaseList
from cognite.client.utils._auxiliary import split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr


class RawDatabasesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs"

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Database]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[DatabaseList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Database] | Iterator[DatabaseList]:
        """Iterate over databases

        Fetches dbs as they are iterated over, so you keep a limited number of dbs in memory.

        Args:
            chunk_size (int | None): Number of dbs to return in each chunk. Defaults to yielding one db a time.
            limit (int | None): Maximum number of dbs to return. Defaults to return all items.

        Returns:
            Iterator[Database] | Iterator[DatabaseList]: No description.
        """
        return self._list_generator(
            list_cls=DatabaseList, resource_cls=Database, chunk_size=chunk_size, method="GET", limit=limit
        )

    def __iter__(self) -> Iterator[Database]:
        """Iterate over databases

        Returns:
            Iterator[Database]: yields Database one by one.
        """
        return self()

    @overload
    def create(self, name: str) -> Database: ...

    @overload
    def create(self, name: list[str]) -> DatabaseList: ...

    def create(self, name: str | list[str]) -> Database | DatabaseList:
        """`Create one or more databases. <https://developer.cognite.com/api#tag/Raw/operation/createDBs>`_

        Args:
            name (str | list[str]): A db name or list of db names to create.

        Returns:
            Database | DatabaseList: Database or list of databases that has been created.

        Examples:

            Create a new database:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.raw.databases.create("db1")
        """
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: dict[str, Any] | list[dict[str, Any]] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(list_cls=DatabaseList, resource_cls=Database, items=items)

    def delete(self, name: str | SequenceNotStr[str], recursive: bool = False) -> None:
        """`Delete one or more databases. <https://developer.cognite.com/api#tag/Raw/operation/deleteDBs>`_

        Args:
            name (str | SequenceNotStr[str]): A db name or list of db names to delete.
            recursive (bool): Recursively delete all tables in the database(s).

        Examples:

            Delete a list of databases:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.raw.databases.delete(["db1", "db2"])
        """
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {"url_path": self._RESOURCE_PATH + "/delete", "json": {"items": chunk, "recursive": recursive}}
            for chunk in chunks
        ]
        summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda el: el["name"]
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> DatabaseList:
        """`List databases <https://developer.cognite.com/api#tag/Raw/operation/getDBs>`_

        Args:
            limit (int | None): Maximum number of databases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            DatabaseList: List of requested databases.

        Examples:

            List the first 5 databases:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> db_list = client.raw.databases.list(limit=5)

            Iterate over databases:

                >>> for db in client.raw.databases:
                ...     db # do something with the db

            Iterate over chunks of databases to reduce memory load:

                >>> for db_list in client.raw.databases(chunk_size=2500):
                ...     db_list # do something with the dbs
        """
        return self._list(list_cls=DatabaseList, resource_cls=Database, method="GET", limit=limit)
