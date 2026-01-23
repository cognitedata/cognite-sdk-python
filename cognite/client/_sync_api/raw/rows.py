"""
===============================================================================
65855d21c810dfa250de3af741093053
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.raw import Row, RowList, RowWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas as pd


class SyncRawRowsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: None = None,
        partitions: None = None,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterator[Row]: ...

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: None,
        partitions: int,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterator[RowList]: ...

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int,
        partitions: None,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterator[RowList]: ...

    @overload
    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int,
        partitions: int,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterator[RowList]: ...

    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int | None = None,
        partitions: int | None = None,
        limit: int | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterator[Row] | Iterator[RowList]:
        """
        Iterate over rows.

        Fetches rows as they are iterated over, so you keep a limited number of rows in memory.

        Note:
            When iterating using partitions > 1, the memory usage is bounded at 2 x partitions x chunk_size. This is implemented
            by halting retrieval speed when the callers code can't keep up.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            chunk_size (int | None): Number of rows to return in each chunk (may be lower). Defaults to yielding one row at a time.
                Note: When used together with 'partitions' the default is 10000 (matching the API limit) and there's an implicit minimum of 1000 rows.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Defaults to not use concurrency.
                The setting is capped at ``global_config.concurrency_settings.raw.read`` and _can_ be used with a finite limit. To prevent unexpected problems
                and maximize read throughput, check out `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_
            limit (int | None): Maximum number of rows to return. Can be used with partitions. Defaults to returning all items.
            min_last_updated_time (int | None): Rows must have been last updated after this time (exclusive). Milliseconds since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time (inclusive). Milliseconds since epoch.
            columns (list[str] | None): List of column keys. Set to `None` to retrieving all, use empty list, [], to retrieve only row keys.

        Yields:
            Row | RowList: An iterator yielding the requested row or rows.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.raw.rows(
                db_name=db_name,
                table_name=table_name,
                chunk_size=chunk_size,
                partitions=partitions,
                limit=limit,
                min_last_updated_time=min_last_updated_time,
                max_last_updated_time=max_last_updated_time,
                columns=columns,
            )
        )  # type: ignore [misc]

    def insert(
        self,
        db_name: str,
        table_name: str,
        row: Sequence[Row] | Sequence[RowWrite] | Row | RowWrite | dict,
        ensure_parent: bool = False,
    ) -> None:
        """
        `Insert one or more rows into a table. <https://developer.cognite.com/api#tag/Raw/operation/postRows>`_

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
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.raw.rows.insert(
                db_name=db_name, table_name=table_name, row=row, ensure_parent=ensure_parent
            )
        )

    def insert_dataframe(
        self, db_name: str, table_name: str, dataframe: pd.DataFrame, ensure_parent: bool = False, dropna: bool = True
    ) -> None:
        """
        `Insert pandas dataframe into a table <https://developer.cognite.com/api#tag/Raw/operation/postRows>`_

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
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = pd.DataFrame(
                ...     {"col-a": [1, 3, None], "col-b": [2, -1, 9]},
                ...     index=["r1", "r2", "r3"])
                >>> res = client.raw.rows.insert_dataframe(
                ...     "db1", "table1", df, dropna=True)
        """
        return run_sync(
            self.__async_client.raw.rows.insert_dataframe(
                db_name=db_name, table_name=table_name, dataframe=dataframe, ensure_parent=ensure_parent, dropna=dropna
            )
        )

    def delete(self, db_name: str, table_name: str, key: str | SequenceNotStr[str]) -> None:
        """
        `Delete rows from a table. <https://developer.cognite.com/api#tag/Raw/operation/deleteRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str | SequenceNotStr[str]): The key(s) of the row(s) to delete.

        Examples:

            Delete rows from table:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> keys_to_delete = ["k1", "k2", "k3"]
                >>> client.raw.rows.delete("db1", "table1", keys_to_delete)
        """
        return run_sync(self.__async_client.raw.rows.delete(db_name=db_name, table_name=table_name, key=key))

    def retrieve(self, db_name: str, table_name: str, key: str) -> Row | None:
        """
        `Retrieve a single row by key. <https://developer.cognite.com/api#tag/Raw/operation/getRow>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str): The key of the row to retrieve.

        Returns:
            Row | None: The requested row.

        Examples:

            Retrieve a row with key 'k1' from table 't1' in database 'db1':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> row = client.raw.rows.retrieve("db1", "t1", "k1")

            You may access the data directly on the row (like a dict), or use '.get' when keys can be missing:

                >>> val1 = row["col1"]
                >>> val2 = row.get("col2")
        """
        return run_sync(self.__async_client.raw.rows.retrieve(db_name=db_name, table_name=table_name, key=key))

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
        infer_dtypes: bool = True,
    ) -> pd.DataFrame:
        """
        `Retrieve rows in a table as a pandas dataframe. <https://developer.cognite.com/api#tag/Raw/operation/getRows>`_

        Rowkeys are used as the index.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int | None): Rows must have been last updated after this time. Milliseconds since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time. Milliseconds since epoch.
            columns (list[str] | None): List of column keys. Set to `None` to retrieving all, use empty list, [], to retrieve only row keys.
            limit (int | None): The number of rows to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Can be used together with a (large) finite limit.
                When partitions is not passed, it defaults to 1, i.e. no concurrency for a finite limit and ``global_config.concurrency_settings.raw.read``
                for an unlimited query (will be capped at this value). To prevent unexpected problems and maximize read throughput, check out
                `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_
            last_updated_time_in_index (bool): Use a MultiIndex with row keys and last_updated_time as index.
            infer_dtypes (bool): If True, pandas will try to infer dtypes of the columns. Defaults to True.

        Returns:
            pd.DataFrame: The requested rows in a pandas dataframe.

        Examples:

            Get dataframe:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = client.raw.rows.retrieve_dataframe("db1", "t1", limit=5)
        """
        return run_sync(
            self.__async_client.raw.rows.retrieve_dataframe(
                db_name=db_name,
                table_name=table_name,
                min_last_updated_time=min_last_updated_time,
                max_last_updated_time=max_last_updated_time,
                columns=columns,
                limit=limit,
                partitions=partitions,
                last_updated_time_in_index=last_updated_time_in_index,
                infer_dtypes=infer_dtypes,
            )
        )

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
        """
        `List rows in a table. <https://developer.cognite.com/api#tag/Raw/operation/getRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int | None): Rows must have been last updated after this time (exclusive). Milliseconds since epoch.
            max_last_updated_time (int | None): Rows must have been last updated before this time (inclusive). Milliseconds since epoch.
            columns (list[str] | None): List of column keys. Set to `None` to retrieving all, use empty list, [], to retrieve only row keys.
            limit (int | None): The number of rows to retrieve. Can be used with partitions. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve rows in parallel using this number of workers. Can be used together with a (large) finite limit.
                When partitions is not passed, it defaults to 1, i.e. no concurrency for a finite limit and ``global_config.concurrency_settings.raw.read``
                for an unlimited query (will be capped at this value). To prevent unexpected problems and maximize read throughput, check out
                `concurrency limits in the API documentation. <https://developer.cognite.com/api#tag/Raw/#section/Request-and-concurrency-limits>`_

        Returns:
            RowList: The requested rows.

        Examples:

            List a few rows:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.raw.rows.list(
                db_name=db_name,
                table_name=table_name,
                min_last_updated_time=min_last_updated_time,
                max_last_updated_time=max_last_updated_time,
                columns=columns,
                limit=limit,
                partitions=partitions,
            )
        )
