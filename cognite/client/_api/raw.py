from __future__ import annotations

import math
import random
import threading
import time
from collections import defaultdict, deque
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import _RUNNING_IN_BROWSER, DEFAULT_LIMIT_READ
from cognite.client.data_classes import Database, DatabaseList, Row, RowList, RowWrite, Table, TableList
from cognite.client.data_classes.raw import RowCore
from cognite.client.utils._auxiliary import (
    find_duplicates,
    interpolate_and_url_encode,
    is_finite,
    is_unlimited,
    split_into_chunks,
    unpack_items_in_payload,
)
from cognite.client.utils._concurrency import ConcurrencySettings, execute_tasks
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._importing import local_import
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from concurrent.futures import Future

    import pandas as pd

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class RawAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.databases = RawDatabasesAPI(config, api_version, cognite_client)
        self.tables = RawTablesAPI(config, api_version, cognite_client)
        self.rows = RawRowsAPI(config, api_version, cognite_client)


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

            Create a new database::

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

            Delete a list of databases::

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

            List the first 5 databases::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> db_list = client.raw.databases.list(limit=5)

            Iterate over databases::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for db in client.raw.databases:
                ...     db # do something with the db

            Iterate over chunks of databases to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for db_list in client.raw.databases(chunk_size=2500):
                ...     db_list # do something with the dbs
        """
        return self._list(list_cls=DatabaseList, resource_cls=Database, method="GET", limit=limit)


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"

    @overload
    def __call__(self, db_name: str, chunk_size: None = None, limit: int | None = None) -> Iterator[Table]: ...

    @overload
    def __call__(self, db_name: str, chunk_size: int, limit: int | None = None) -> Iterator[TableList]: ...

    def __call__(
        self, db_name: str, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Table] | Iterator[TableList]:
        """Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int | None): Number of tables to return in each chunk. Defaults to yielding one table a time.
            limit (int | None): Maximum number of tables to return. Defaults to return all items.

        Returns:
            Iterator[Table] | Iterator[TableList]: No description.
        """
        table_iterator = self._list_generator(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        )
        return self._set_db_name_on_tables_generator(table_iterator, db_name)

    @overload
    def create(self, db_name: str, name: str) -> Table: ...

    @overload
    def create(self, db_name: str, name: list[str]) -> TableList: ...

    def create(self, db_name: str, name: str | list[str]) -> Table | TableList:
        """`Create one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/createTables>`_

        Args:
            db_name (str): Database to create the tables in.
            name (str | list[str]): A table name or list of table names to create.

        Returns:
            Table | TableList: Table or list of tables that has been created.

        Examples:

            Create a new table in a database::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.raw.tables.create("db1", "table1")
        """
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: dict[str, Any] | list[dict[str, Any]] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        tb = self._create_multiple(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            items=items,
        )
        return self._set_db_name_on_tables(tb, db_name)

    def delete(self, db_name: str, name: str | SequenceNotStr[str]) -> None:
        """`Delete one or more tables. <https://developer.cognite.com/api#tag/Raw/operation/deleteTables>`_

        Args:
            db_name (str): Database to delete tables from.
            name (str | SequenceNotStr[str]): A table name or list of table names to delete.

        Examples:

            Delete a list of tables::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.raw.tables.delete("db1", ["table1", "table2"])
        """
        assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {
                "url_path": interpolate_and_url_encode(self._RESOURCE_PATH, db_name) + "/delete",
                "json": {"items": chunk},
            }
            for chunk in chunks
        ]
        summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda el: el["name"]
        )

    def _set_db_name_on_tables(self, tb: Table | TableList, db_name: str) -> Table | TableList:
        if isinstance(tb, Table):
            tb._db_name = db_name
            return tb
        elif isinstance(tb, TableList):
            for t in tb:
                t._db_name = db_name
            return tb
        raise TypeError("tb must be Table or TableList")

    def _set_db_name_on_tables_generator(
        self, table_iterator: Iterator[Table] | Iterator[TableList], db_name: str
    ) -> Iterator[Table] | Iterator[TableList]:
        for tbl in table_iterator:
            yield self._set_db_name_on_tables(tbl, db_name)

    def list(self, db_name: str, limit: int | None = DEFAULT_LIMIT_READ) -> TableList:
        """`List tables <https://developer.cognite.com/api#tag/Raw/operation/getTables>`_

        Args:
            db_name (str): The database to list tables from.
            limit (int | None): Maximum number of tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TableList: List of requested tables.

        Examples:

            List the first 5 tables::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> table_list = client.raw.tables.list("db1", limit=5)

            Iterate over tables::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for table in client.raw.tables(db_name="db1"):
                ...     table # do something with the table

            Iterate over chunks of tables to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for table_list in client.raw.tables(db_name="db1", chunk_size=2500):
                ...     table_list # do something with the tables
        """
        tb = self._list(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )
        return cast(TableList, self._set_db_name_on_tables(tb, db_name))


class RawRowsAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables/{}/rows"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 5000
        self._LIST_LIMIT = 10000

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: None = None,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
        partitions: int | None = None,
    ) -> Iterator[Row]: ...

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
        partitions: int | None = None,
    ) -> Iterator[RowList]: ...

    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int | None = None,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
        partitions: int | None = None,
    ) -> Iterator[Row] | Iterator[RowList]:
        """Iterate over rows.

        Fetches rows as they are iterated over, so you keep a limited number of rows in memory.

        Args:
            db_name (str): Name of the database
            table_name (str): Name of the table to iterate over rows for
            chunk_size (int | None): Number of rows to return in each chunk (may be lower). Defaults to yielding one row at a time.
                Note: When used together with 'partitions' the default is 10000 (matching the API limit) and there's an implicit minimum of 1000 rows.
            limit (int | None): Maximum number of rows to return. Can be used with partitions. Defaults to returning all items.
            min_last_updated_time (int | None): Rows must have been last updated after this time (exclusive). ms since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time (inclusive). ms since epoch.
            columns (list[str] | None): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Defaults to not use concurrency.
                The setting is capped at ``global_config.max_workers`` and _can_ be used with a finite limit. To prevent unexpected
                problems and maximize read throughput, check out `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_

        Returns:
            Iterator[Row] | Iterator[RowList]: An iterator yielding the requested row or rows.

        Note:
            When iterating using partitions > 1, the memory usage is bounded at 2 x partitions x chunk_size. This is implemented
            by halting retrieval speed when the callers code can't keep up.
        """
        if partitions is None or _RUNNING_IN_BROWSER:
            return self._list_generator(
                list_cls=RowList,
                resource_cls=Row,
                resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                chunk_size=chunk_size,
                method="GET",
                limit=limit,
                filter={
                    "minLastUpdatedTime": min_last_updated_time,
                    "maxLastUpdatedTime": max_last_updated_time,
                    "columns": self._make_columns_param(columns),
                },
            )
        return self._list_generator_concurrent(
            db_name=db_name,
            table_name=table_name,
            chunk_size=chunk_size,
            limit=limit,
            min_last_updated_time=min_last_updated_time,
            max_last_updated_time=max_last_updated_time,
            columns=columns,
            partitions=partitions,
        )

    def _list_generator_concurrent(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int | None,
        limit: int | None,
        min_last_updated_time: int | None,
        max_last_updated_time: int | None,
        columns: list[str] | None,
        partitions: int,
    ) -> Iterator[RowList]:
        # We are a bit restrictive on partitioning - especially for "small" limits:
        partitions = min(partitions, self._config.max_workers)
        if finite_limit := is_finite(limit):
            partitions = min(partitions, self._config.max_workers, math.ceil(limit / 20_000))
            if chunk_size is not None and limit < chunk_size:
                raise ValueError(f"chunk_size ({chunk_size}) should be much smaller than limit ({limit})")

        cursors = self._get_parallel_cursors(
            db_name, table_name, min_last_updated_time, max_last_updated_time, n_cursors=partitions
        )
        chunk_size = max(1000, chunk_size or 10_000)
        column_string = self._make_columns_param(columns)
        read_iterators = [
            self._list_generator(
                list_cls=RowList,
                resource_cls=Row,
                resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                chunk_size=chunk_size,
                method="GET",
                filter={"columns": column_string},  # we don't need X_last_updated_time
                limit=None,
                initial_cursor=initial,
            )
            for initial in cursors
        ]

        def exhaust(iterator: Iterator) -> None:
            for res in iterator:
                results.append(res)
                if quit_early.is_set():
                    return
                # User code might be processing slower than the rate of row-fetching, and we want this
                # iteration-based method to have a upper-bounded memory impact, so we keep a max queue
                # size of unprocessed row-chunks:
                while len(results) >= partitions:
                    # Sleep randomly per thread to avoid sending all new fetch requests at the same time
                    time.sleep(random.uniform(0, partitions))
                    if quit_early.is_set():
                        return

        quit_early = threading.Event()
        results: deque[RowList] = deque()  # fifo, not that ordering matters anyway...
        pool = ConcurrencySettings.get_thread_pool_executor_or_raise(max_workers=self._config.max_workers)
        futures = [pool.submit(exhaust, task) for task in read_iterators]

        if finite_limit:
            yield from self._read_rows_limited(futures, results, cast(int, limit), quit_early)
        else:
            yield from self._read_rows_unlimited(futures, results)

        for f in futures:
            f.cancelled() or f.result()  # Visibility in case anything failed

    def _read_rows_unlimited(self, futures: list[Future], results: deque[RowList]) -> Iterator[RowList]:
        while not all(f.done() for f in futures):
            while results:
                yield results.popleft()
        yield from results

    def _read_rows_limited(
        self, futures: list[Future], results: deque[RowList], limit: int, quit_early: threading.Event
    ) -> Iterator[RowList]:
        n_total = 0
        while True:
            while results:
                n_new = len(part := results.popleft())
                if n_total + n_new < limit:
                    n_total += n_new
                    yield part
                else:
                    for f in futures:
                        f.cancel()
                    quit_early.set()
                    yield part[: limit - n_total]
                    return
            if all(f.done() for f in futures) and not results:
                return

    def insert(
        self,
        db_name: str,
        table_name: str,
        row: Sequence[Row] | Sequence[RowWrite] | Row | RowWrite | dict,
        ensure_parent: bool = False,
    ) -> None:
        """`Insert one or more rows into a table. <https://developer.cognite.com/api#tag/Raw/operation/postRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            row (Sequence[Row] | Sequence[RowWrite] | Row | RowWrite | dict): The row(s) to insert
            ensure_parent (bool): Create database/table if they don't already exist.

        Examples:

            Insert new rows into a table:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RowWrite
                >>> client = CogniteClient()
                >>> rows = [RowWrite(key="r1", columns={"col1": "val1", "col2": "val1"}),
                ...         RowWrite(key="r2", columns={"col1": "val2", "col2": "val2"})]
                >>> client.raw.rows.insert("db1", "table1", rows)

            You may also insert a dictionary directly:

                >>> rows = {
                ...     "key-1": {"col1": 1, "col2": 2},
                ...     "key-2": {"col1": 3, "col2": 4, "col3": "high five"},
                ... }
                >>> client.raw.rows.insert("db1", "table1", rows)
        """
        tasks = [
            {
                "url_path": interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                "json": {"items": chunk},
                "params": {"ensureParent": ensure_parent},
            }
            for chunk in self._process_row_input(row)
        ]
        summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda row: row.get("key")
        )

    def insert_dataframe(
        self,
        db_name: str,
        table_name: str,
        dataframe: pd.DataFrame,
        ensure_parent: bool = False,
        dropna: bool = True,
    ) -> None:
        """`Insert pandas dataframe into a table <https://developer.cognite.com/api#tag/Raw/operation/postRows>`_

        Uses index for row keys.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            dataframe (pd.DataFrame): The dataframe to insert. Index will be used as row keys.
            ensure_parent (bool): Create database/table if they don't already exist.
            dropna (bool): Remove NaNs (but keep None's in dtype=object columns) before inserting. Done individually per column. Default: True

        Examples:

            Insert new rows into a table:

                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>>
                >>> client = CogniteClient()
                >>> df = pd.DataFrame(
                ...     {"col-a": [1, 3, None], "col-b": [2, -1, 9]},
                ...     index=["r1", "r2", "r3"])
                >>> res = client.raw.rows.insert_dataframe(
                ...     "db1", "table1", df, dropna=True)
        """
        if not dataframe.index.is_unique:
            raise ValueError("Dataframe index is not unique (used for the row keys)")
        elif not dataframe.columns.is_unique:
            raise ValueError(f"Dataframe columns are not unique: {sorted(find_duplicates(dataframe.columns))}")

        rows = self._df_to_rows_skip_nans(dataframe) if dropna else dataframe.to_dict(orient="index")
        self.insert(db_name=db_name, table_name=table_name, row=rows, ensure_parent=ensure_parent)

    @staticmethod
    def _df_to_rows_skip_nans(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        np = local_import("numpy")
        rows: defaultdict[str, dict[str, Any]] = defaultdict(dict)
        object_cols = df.select_dtypes("object").columns

        for column_id, col in df.items():
            if column_id not in object_cols:
                col = col.dropna()
            else:
                # pandas treat None as NaN, but numpy does not:
                mask = np.logical_or(col.to_numpy() == None, col.notna())  # noqa: E711
                col = col[mask]

            for idx, val in col.items():
                rows[idx][column_id] = val
        return dict(rows)

    def _process_row_input(self, row: Sequence[Row] | Sequence[RowWrite] | Row | RowWrite | dict) -> list[list[dict]]:
        assert_type(row, "row", [Sequence, dict, RowCore])
        rows = []
        if isinstance(row, dict):
            for key, columns in row.items():
                rows.append({"key": key, "columns": columns})
        elif isinstance(row, Sequence):
            for elem in row:
                if isinstance(elem, Row):
                    rows.append(elem.as_write().dump(camel_case=True))
                elif isinstance(elem, RowWrite):
                    rows.append(elem.dump(camel_case=True))
                else:
                    raise TypeError("list elements must be Row objects.")
        elif isinstance(row, Row):
            rows.append(row.as_write().dump(camel_case=True))
        elif isinstance(row, RowWrite):
            rows.append(row.dump(camel_case=True))
        return split_into_chunks(rows, self._CREATE_LIMIT)

    def delete(self, db_name: str, table_name: str, key: str | SequenceNotStr[str]) -> None:
        """`Delete rows from a table. <https://developer.cognite.com/api#tag/Raw/operation/deleteRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str | SequenceNotStr[str]): The key(s) of the row(s) to delete.

        Examples:

            Delete rows from table::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> keys_to_delete = ["k1", "k2", "k3"]
                >>> client.raw.rows.delete("db1", "table1", keys_to_delete)
        """
        assert_type(key, "key", [str, Sequence])
        if isinstance(key, str):
            key = [key]
        to_delete = [{"key": k} for k in key]
        tasks = [
            {
                "url_path": interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name) + "/delete",
                "json": {"items": chunk},
            }
            for chunk in split_into_chunks(to_delete, self._DELETE_LIMIT)
        ]
        summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda el: el["key"]
        )

    def retrieve(self, db_name: str, table_name: str, key: str) -> Row | None:
        """`Retrieve a single row by key. <https://developer.cognite.com/api#tag/Raw/operation/getRow>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str): The key of the row to retrieve.

        Returns:
            Row | None: The requested row.

        Examples:

            Retrieve a row with key 'k1' from table 't1' in database 'db1':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> row = client.raw.rows.retrieve("db1", "t1", "k1")

            You may access the data directly on the row (like a dict), or use '.get' when keys can be missing:

                >>> val1 = row["col1"]
                >>> val2 = row.get("col2")

        """
        return self._retrieve(
            cls=Row,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            identifier=Identifier(key),
        )

    def _make_columns_param(self, columns: list[str] | None) -> str | None:
        if columns is None:
            return None
        if not isinstance(columns, list):
            raise TypeError("Expected a list for argument columns")
        if len(columns) == 0:
            return ","
        else:
            return ",".join(str(x) for x in columns)

    def retrieve_dataframe(
        self,
        db_name: str,
        table_name: str,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
        last_updated_time_in_index: bool = False,
    ) -> pd.DataFrame:
        """`Retrieve rows in a table as a pandas dataframe. <https://developer.cognite.com/api#tag/Raw/operation/getRows>`_

        Rowkeys are used as the index.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int | None): Rows must have been last updated after this time. ms since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time. ms since epoch.
            columns (list[str] | None): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
            limit (int | None): The number of rows to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Can be used together with a (large) finite limit.
                When partitions is not passed, it defaults to 1, i.e. no concurrency for a finite limit and ``global_config.max_workers`` for an unlimited query
                (will be capped at this value). To prevent unexpected problems and maximize read throughput, check out
                `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_
            last_updated_time_in_index (bool): Use a MultiIndex with row keys and last_updated_time as index.

        Returns:
            pd.DataFrame: The requested rows in a pandas dataframe.

        Examples:

            Get dataframe::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = client.raw.rows.retrieve_dataframe("db1", "t1", limit=5)
        """
        pd = local_import("pandas")
        rows = self.list(db_name, table_name, min_last_updated_time, max_last_updated_time, columns, limit, partitions)
        if last_updated_time_in_index:
            idx = pd.MultiIndex.from_tuples(
                [(r.key, pd.Timestamp(r.last_updated_time, unit="ms")) for r in rows],
                names=["key", "last_updated_time"],
            )
        else:
            idx = [r.key for r in rows]
        cols = [r.columns for r in rows]
        return pd.DataFrame(cols, index=idx)

    def _get_parallel_cursors(
        self,
        db_name: str,
        table_name: str,
        min_last_updated_time: int | None,
        max_last_updated_time: int | None,
        n_cursors: int,
    ) -> list[str]:
        return self._get(
            url_path=interpolate_and_url_encode("/raw/dbs/{}/tables/{}/cursors", db_name, table_name),
            params={
                "minLastUpdatedTime": min_last_updated_time,
                "maxLastUpdatedTime": max_last_updated_time,
                "numberOfCursors": n_cursors,
            },
        ).json()["items"]

    def list(
        self,
        db_name: str,
        table_name: str,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
    ) -> RowList:
        """`List rows in a table. <https://developer.cognite.com/api#tag/Raw/operation/getRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int | None): Rows must have been last updated after this time (exclusive). ms since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time (inclusive). ms since epoch.
            columns (list[str] | None): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
            limit (int | None): The number of rows to retrieve. Can be used with partitions. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Can be used together with a (large) finite limit.
                When partitions is not passed, it defaults to 1, i.e. no concurrency for a finite limit and ``global_config.max_workers`` for an unlimited query
                (will be capped at this value). To prevent unexpected problems and maximize read throughput, check out
                `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_

        Returns:
            RowList: The requested rows.

        Examples:

            List a few rows:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> row_list = client.raw.rows.list("db1", "tbl1", limit=5)

            Read an entire table efficiently by using concurrency (default behavior when ``limit=None``):

                >>> row_list = client.raw.rows.list("db1", "tbl1", limit=None)

            Iterate through all rows one-by-one to reduce memory load (no concurrency used):

                >>> for row in client.raw.rows("db1", "t1", columns=["col1","col2"]):
                ...     val1 = row["col1"]  # You may access the data directly
                ...     val2 = row.get("col2")  # ...or use '.get' when keys can be missing

            Iterate through all rows, one chunk at a time, to reduce memory load (no concurrency used):

                >>> for row_list in client.raw.rows("db1", "t1", chunk_size=2500):
                ...     row_list  # Do something with the rows

            Iterate through a massive table to reduce memory load while using concurrency for high throughput.
            Note: ``partitions`` must be specified for concurrency to be used (this is different from ``list()``
            to keep backward compatibility). Supplying a finite ``limit`` does not affect concurrency settings
            (except for very small values).

                >>> rows_iterator = client.raw.rows(
                ...     db_name="db1", table_name="t1", partitions=5, chunk_size=5000, limit=1_000_000
                ... )
                >>> for row_list in rows_iterator:
                ...     row_list  # Do something with the rows
        """
        chunk_size = None
        if _RUNNING_IN_BROWSER:
            chunk_size = 10_000
        elif partitions is None:
            if is_unlimited(limit):
                # Before 'partitions' was introduced, existing logic was that 'limit=None' meant 'partitions=max_workers'.
                partitions = self._config.max_workers
            else:
                chunk_size = limit  # We fetch serially, but don't want rows one-by-one

        rows_iterator = self(
            db_name=db_name,
            table_name=table_name,
            chunk_size=chunk_size,
            limit=limit,
            min_last_updated_time=min_last_updated_time,
            max_last_updated_time=max_last_updated_time,
            columns=columns,
            partitions=partitions,
        )
        return RowList(
            [row for row_list in cast(Iterator[RowList], rows_iterator) for row in row_list],
            cognite_client=self._cognite_client,
        )
