from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Sequence, Union, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.data_classes import Database, DatabaseList, Row, RowList, Table, TableList
from cognite.client.utils._auxiliary import local_import
from cognite.client.utils._identifier import Identifier

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class RawAPI(APIClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.databases = RawDatabasesAPI(*args, **kwargs)
        self.tables = RawTablesAPI(*args, **kwargs)
        self.rows = RawRowsAPI(*args, **kwargs)


class RawDatabasesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs"

    def __call__(self, chunk_size: int = None, limit: int = None) -> Union[Iterator[Database], Iterator[DatabaseList]]:
        """Iterate over databases

        Fetches dbs as they are iterated over, so you keep a limited number of dbs in memory.

        Args:
            chunk_size (int, optional): Number of dbs to return in each chunk. Defaults to yielding one db a time.
            limit (int, optional): Maximum number of dbs to return. Defaults to return all items.
        """
        return self._list_generator(
            list_cls=DatabaseList, resource_cls=Database, chunk_size=chunk_size, method="GET", limit=limit
        )

    def __iter__(self) -> Iterator[Database]:
        return cast(Iterator[Database], self())

    @overload
    def create(self, name: str) -> Database:
        ...

    @overload
    def create(self, name: List[str]) -> DatabaseList:
        ...

    def create(self, name: Union[str, List[str]]) -> Union[Database, DatabaseList]:
        """`Create one or more databases. <https://docs.cognite.com/api/v1/#operation/createDBs>`_

        Args:
            name (Union[str, List[str]]): A db name or list of db names to create.

        Returns:
            Union[Database, DatabaseList]: Database or list of databases that has been created.

        Examples:

            Create a new database::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.raw.databases.create("db1")
        """
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: Union[Dict[str, Any], List[Dict[str, Any]]] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(list_cls=DatabaseList, resource_cls=Database, items=items)

    def delete(self, name: Union[str, Sequence[str]], recursive: bool = False) -> None:
        """`Delete one or more databases. <https://docs.cognite.com/api/v1/#operation/deleteDBs>`_

        Args:
            name (Union[str, Sequence[str]]): A db name or list of db names to delete.
            recursive (bool): Recursively delete all tables in the database(s).

        Returns:
            None

        Examples:

            Delete a list of databases::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.raw.databases.delete(["db1", "db2"])
        """
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {"url_path": self._RESOURCE_PATH + "/delete", "json": {"items": chunk, "recursive": recursive}}
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"], task_list_element_unwrap_fn=lambda el: el["name"]
        )

    def list(self, limit: int = 25) -> DatabaseList:
        """`List databases <https://docs.cognite.com/api/v1/#operation/getDBs>`_

        Args:
            limit (int, optional): Maximum number of databases to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DatabaseList: List of requested databases.

        Examples:

            List the first 5 databases::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> db_list = c.raw.databases.list(limit=5)

            Iterate over databases::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for db in c.raw.databases:
                ...     db # do something with the db

            Iterate over chunks of databases to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for db_list in c.raw.databases(chunk_size=2500):
                ...     db_list # do something with the dbs
        """
        return self._list(list_cls=DatabaseList, resource_cls=Database, method="GET", limit=limit)


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"

    def __call__(
        self, db_name: str, chunk_size: int = None, limit: int = None
    ) -> Union[Iterator[Table], Iterator[TableList]]:
        """Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int, optional): Number of tables to return in each chunk. Defaults to yielding one table a time.
            limit (int, optional): Maximum number of tables to return. Defaults to return all items.
        """
        for tb in self._list_generator(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        ):
            yield self._set_db_name_on_tables(tb, db_name)

    @overload
    def create(self, db_name: str, name: str) -> Table:
        ...

    @overload
    def create(self, db_name: str, name: List[str]) -> TableList:
        ...

    def create(self, db_name: str, name: Union[str, List[str]]) -> Union[Table, TableList]:
        """`Create one or more tables. <https://docs.cognite.com/api/v1/#operation/createTables>`_

        Args:
            db_name (str): Database to create the tables in.
            name (Union[str, List[str]]): A table name or list of table names to create.

        Returns:
            Union[Table, TableList]: Table or list of tables that has been created.

        Examples:

            Create a new table in a database::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.raw.tables.create("db1", "table1")
        """
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: Union[Dict[str, Any], List[Dict[str, Any]]] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        tb = self._create_multiple(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            items=items,
        )
        return self._set_db_name_on_tables(tb, db_name)

    def delete(self, db_name: str, name: Union[str, Sequence[str]]) -> None:
        """`Delete one or more tables. <https://docs.cognite.com/api/v1/#operation/deleteTables>`_

        Args:
            db_name (str): Database to delete tables from.
            name (Union[str, Sequence[str]]): A table name or list of table names to delete.

        Returns:
            None

        Examples:

            Delete a list of tables::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.raw.tables.delete("db1", ["table1", "table2"])
        """
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {
                "url_path": utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name) + "/delete",
                "json": {"items": chunk},
            }
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"], task_list_element_unwrap_fn=lambda el: el["name"]
        )

    def list(self, db_name: str, limit: int = 25) -> TableList:
        """`List tables <https://docs.cognite.com/api/v1/#operation/getTables>`_

        Args:
            db_name (str): The database to list tables from.
            limit (int, optional): Maximum number of tables to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            TableList: List of requested tables.

        Examples:

            List the first 5 tables::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> table_list = c.raw.tables.list("db1", limit=5)

            Iterate over tables::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for table in c.raw.tables(db_name="db1"):
                ...     table # do something with the table

            Iterate over chunks of tables to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for table_list in c.raw.tables(db_name="db1", chunk_size=2500):
                ...     table_list # do something with the tables
        """
        tb = self._list(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )
        return cast(TableList, self._set_db_name_on_tables(tb, db_name))

    def _set_db_name_on_tables(self, tb: Union[Table, TableList], db_name: str) -> Union[Table, TableList]:
        if isinstance(tb, Table):
            tb._db_name = db_name
            return tb
        elif isinstance(tb, TableList):
            for t in tb:
                t._db_name = db_name
            return tb
        raise TypeError("tb must be Table or TableList")


class RawRowsAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables/{}/rows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: Optional[str] = None,
        cognite_client: "CogniteClient" = None,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 5000
        self._LIST_LIMIT = 10000

    def __call__(
        self,
        db_name: str,
        table_name: str,
        chunk_size: int = None,
        limit: int = None,
        min_last_updated_time: int = None,
        max_last_updated_time: int = None,
        columns: List[str] = None,
    ) -> Union[Iterator[Row], Iterator[RowList]]:
        """Iterate over rows.

        Fetches rows as they are iterated over, so you keep a limited number of rows in memory.

        Args:
            db_name (str): Name of the database
            table_name (str): Name of the table to iterate over rows for
            chunk_size (int, optional): Number of rows to return in each chunk. Defaults to yielding one row a time.
            limit (int, optional): Maximum number of rows to return. Defaults to return all items.
            min_last_updated_time (int): Rows must have been last updated after this time. ms since epoch.
            max_last_updated_time (int): Rows must have been last updated before this time. ms since epoch.
            columns (List[str]): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
        """
        return self._list_generator(
            list_cls=RowList,
            resource_cls=Row,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
            filter={
                "minLastUpdatedTime": min_last_updated_time,
                "maxLastUpdatedTime": max_last_updated_time,
                "columns": self._make_columns_param(columns),
            },
        )

    def insert(
        self, db_name: str, table_name: str, row: Union[Sequence[Row], Row, Dict], ensure_parent: bool = False
    ) -> None:
        """`Insert one or more rows into a table. <https://docs.cognite.com/api/v1/#operation/postRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            row (Union[Sequence[Row], Row, Dict]): The row(s) to insert
            ensure_parent (bool): Create database/table if they don't already exist.

        Returns:
            None

        Examples:

            Insert new rows into a table::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> rows = {"r1": {"col1": "val1", "col2": "val1"}, "r2": {"col1": "val2", "col2": "val2"}}
                >>> c.raw.rows.insert("db1", "table1", rows)
        """
        chunks = self._process_row_input(row)

        tasks = [
            {
                "url_path": utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                "json": {"items": chunk},
                "params": {"ensureParent": ensure_parent},
            }
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"], task_list_element_unwrap_fn=lambda row: row.get("key")
        )

    def insert_dataframe(self, db_name: str, table_name: str, dataframe: Any, ensure_parent: bool = False) -> None:
        """`Insert pandas dataframe into a table <https://docs.cognite.com/api/v1/#operation/postRows>`_

        Use index as rowkeys.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            dataframe (pandas.DataFrame): The dataframe to insert. Index will be used as rowkeys.
            ensure_parent (bool): Create database/table if they don't already exist.

        Returns:
            None

        Examples:

            Insert new rows into a table::

                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>>
                >>> c = CogniteClient()
                >>> df = pd.DataFrame(data={"a": 1, "b": 2}, index=["r1", "r2", "r3"])
                >>> res = c.raw.rows.insert_dataframe("db1", "table1", df)
        """
        df_dict = dataframe.to_dict(orient="index")
        rows = [Row(key=key, columns=cols) for key, cols in df_dict.items()]
        self.insert(db_name=db_name, table_name=table_name, row=rows, ensure_parent=ensure_parent)

    def _process_row_input(self, row: Union[Sequence[Row], Row, Dict]) -> List[Union[List, Dict]]:
        utils._auxiliary.assert_type(row, "row", [Sequence, dict, Row])
        rows = []
        if isinstance(row, dict):
            for key, columns in row.items():
                rows.append({"key": key, "columns": columns})
        elif isinstance(row, list):
            for elem in row:
                if isinstance(elem, Row):
                    rows.append(elem.dump(camel_case=True))
                else:
                    raise TypeError("list elements must be Row objects.")
        elif isinstance(row, Row):
            rows.append(row.dump(camel_case=True))
        return utils._auxiliary.split_into_chunks(rows, self._CREATE_LIMIT)

    def delete(self, db_name: str, table_name: str, key: Union[str, Sequence[str]]) -> None:
        """`Delete rows from a table. <https://docs.cognite.com/api/v1/#operation/deleteRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (Union[str, Sequence[str]]): The key(s) of the row(s) to delete.

        Returns:
            None

        Examples:

            Delete rows from table::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> keys_to_delete = ["k1", "k2", "k3"]
                >>> c.raw.rows.delete("db1", "table1", keys_to_delete)
        """
        utils._auxiliary.assert_type(key, "key", [str, Sequence])
        if isinstance(key, str):
            key = [key]
        items = [{"key": k} for k in key]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            (
                {
                    "url_path": utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name)
                    + "/delete",
                    "json": {"items": chunk},
                }
            )
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"], task_list_element_unwrap_fn=lambda el: el["key"]
        )

    def retrieve(self, db_name: str, table_name: str, key: str) -> Optional[Row]:
        """`Retrieve a single row by key. <https://docs.cognite.com/api/v1/#operation/getRow>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str): The key of the row to retrieve.

        Returns:
            Optional[Row]: The requested row.

        Examples:

            Retrieve a row with key 'k1' from tablew 't1' in database 'db1'::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> row = c.raw.rows.retrieve("db1", "t1", "k1")
        """
        return self._retrieve(
            cls=Row,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            identifier=Identifier(key),
        )

    def list(
        self,
        db_name: str,
        table_name: str,
        min_last_updated_time: int = None,
        max_last_updated_time: int = None,
        columns: List[str] = None,
        limit: int = 25,
    ) -> RowList:
        """`List rows in a table. <https://docs.cognite.com/api/v1/#operation/getRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int): Rows must have been last updated after this time. ms since epoch.
            max_last_updated_time (int): Rows must have been last updated before this time. ms since epoch.
            columns (List[str]): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
            limit (int): The number of rows to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            RowList: The requested rows.

        Examples:

            List rows::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> row_list = c.raw.rows.list("db1", "t1", limit=5)

            Iterate over rows::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for row in c.raw.rows(db_name="db1", table_name="t1", columns=["col1","col2"]):
                ...     row # do something with the row

            Iterate over chunks of rows to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for row_list in c.raw.rows(db_name="db1", table_name="t1", chunk_size=2500):
                ...     row_list # do something with the rows
        """
        if limit in {None, -1, float("inf")}:
            cursors = self._get(
                url_path=utils._auxiliary.interpolate_and_url_encode(
                    "/raw/dbs/{}/tables/{}/cursors", db_name, table_name
                ),
                params={
                    "minLastUpdatedTime": min_last_updated_time,
                    "maxLastUpdatedTime": max_last_updated_time,
                    "numberOfCursors": self._config.max_workers,
                },
            ).json()["items"]
        else:
            cursors = [None]
        tasks = [
            dict(
                list_cls=RowList,
                resource_cls=Row,
                resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                method="GET",
                filter={
                    "columns": self._make_columns_param(columns),
                    "minLastUpdatedTime": min_last_updated_time,
                    "maxLastUpdatedTime": max_last_updated_time,
                },
                limit=limit,
                initial_cursor=cursor,
            )
            for cursor in cursors
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._list, tasks, max_workers=self._config.max_workers)
        if summary.exceptions:
            raise summary.exceptions[0]
        return RowList(summary.joined_results())

    def _make_columns_param(self, columns: Optional[List[str]]) -> Optional[str]:
        if columns is None:
            return None
        if not isinstance(columns, List):
            raise ValueError("Expected a list for argument columns")
        if len(columns) == 0:
            return ","
        else:
            return ",".join([str(x) for x in columns])

    def retrieve_dataframe(
        self,
        db_name: str,
        table_name: str,
        min_last_updated_time: int = None,
        max_last_updated_time: int = None,
        columns: List[str] = None,
        limit: int = 25,
    ) -> "pandas.DataFrame":
        """`Retrieve rows in a table as a pandas dataframe. <https://docs.cognite.com/api/v1/#operation/getRows>`_

        Rowkeys are used as the index.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            min_last_updated_time (int): Rows must have been last updated after this time. ms since epoch.
            max_last_updated_time (int): Rows must have been last updated before this time. ms since epoch.
            columns (List[str]): List of column keys. Set to `None` for retrieving all, use [] to retrieve only row keys.
            limit (int): The number of rows to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            pandas.DataFrame: The requested rows in a pandas dataframe.

        Examples:

            Get dataframe::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.raw.rows.retrieve_dataframe("db1", "t1", limit=5)
        """
        pd = cast(Any, local_import("pandas"))
        rows = self.list(db_name, table_name, min_last_updated_time, max_last_updated_time, columns, limit)
        idx = [r.key for r in rows]
        cols = [r.columns for r in rows]
        return pd.DataFrame(cols, index=idx)
