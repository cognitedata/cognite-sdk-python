from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Database, DatabaseList, Row, RowList, Table, TableList


class RawAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.databases = RawDatabasesAPI(*args, **kwargs)
        self.tables = RawTablesAPI(*args, **kwargs)
        self.rows = RawRowsAPI(*args, **kwargs)


class RawDatabasesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs"
    _LIST_CLASS = DatabaseList

    def __call__(
        self, chunk_size: int = None, limit: int = None
    ) -> Generator[Union[Database, DatabaseList], None, None]:
        """Iterate over databases

        Fetches dbs as they are iterated over, so you keep a limited number of dbs in memory.

        Args:
            chunk_size (int, optional): Number of dbs to return in each chunk. Defaults to yielding one db a time.
            limit (int, optional): Maximum number of dbs to return. Defaults to return all items.
        """
        return self._list_generator(chunk_size=chunk_size, method="GET", limit=limit)

    def __iter__(self) -> Generator[Database, None, None]:
        return self.__call__()

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
        utils._auxiliary.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            items = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(items=items)

    def delete(self, name: Union[str, List[str]], recursive: bool = False) -> None:
        """`Delete one or more databases. <https://docs.cognite.com/api/v1/#operation/deleteDBs>`_

        Args:
            name (Union[str, List[str]]): A db name or list of db names to delete.
            recursive (bool): Recursively delete all tables in the database(s).

        Returns:
            None

        Examples:

            Delete a list of databases::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.raw.databases.delete(["db1", "db2"])
        """
        utils._auxiliary.assert_type(name, "name", [str, list])
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
        return self._list(method="GET", limit=limit)


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"
    _LIST_CLASS = TableList

    def __call__(
        self, db_name: str, chunk_size: int = None, limit: int = None
    ) -> Generator[Union[Table, TableList], None, None]:
        """Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int, optional): Number of tables to return in each chunk. Defaults to yielding one table a time.
            limit (int, optional): Maximum number of tables to return. Defaults to return all items.
        """
        for tb in self._list_generator(
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        ):
            yield self._set_db_name_on_tables(tb, db_name)

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
        utils._auxiliary.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            items = {"name": name}
        else:
            items = [{"name": n} for n in name]
        tb = self._create_multiple(
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name), items=items
        )
        return self._set_db_name_on_tables(tb, db_name)

    def delete(self, db_name: str, name: Union[str, List[str]]) -> None:
        """`Delete one or more tables. <https://docs.cognite.com/api/v1/#operation/deleteTables>`_

        Args:
            db_name (str): Database to delete tables from.
            name (Union[str, List[str]]): A table name or list of table names to delete.

        Returns:
            None

        Examples:

            Delete a list of tables::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.raw.tables.delete("db1", ["table1", "table2"])
        """
        utils._auxiliary.assert_type(name, "name", [str, list])
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
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )
        return self._set_db_name_on_tables(tb, db_name)

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
    _LIST_CLASS = RowList

    def __init__(self, config: utils._client_config.ClientConfig, api_version: str = None, cognite_client=None):
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 10000
        self._LIST_LIMIT = 10000

    def __call__(
        self, db_name: str, table_name: str, chunk_size: int = None, limit: int = None
    ) -> Generator[Union[Row, RowList], None, None]:
        """Iterate over rows.

        Fetches rows as they are iterated over, so you keep a limited number of rows in memory.

        Args:
            db_name (str): Name of the database
            table_name (str): Name of the table to iterate over rows for
            chunk_size (int, optional): Number of rows to return in each chunk. Defaults to yielding one row a time.
            limit (int, optional): Maximum number of rows to return. Defaults to return all items.
        """
        return self._list_generator(
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        )

    def insert(
        self, db_name: str, table_name: str, row: Union[List[Row], Row, Dict], ensure_parent: bool = False
    ) -> None:
        """`Insert one or more rows into a table. <https://docs.cognite.com/api/v1/#operation/postRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            row (Union[List[Row], Row, Dict]): The row(s) to insert
            ensure_parent (bool): Create database/table if they don't already exist.

        Returns:
            None

        Examples:

            Insert new rows into a table::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> rows = {"r1": {"col1": "val1", "col2": "val1"}, "r2": {"col1": "val2", "col2": "val2"}}
                >>> res = c.raw.rows.insert("db1", "table1", rows)
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

    def _process_row_input(self, row: List[Union[List, Dict, Row]]):
        utils._auxiliary.assert_type(row, "row", [list, dict, Row])
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

    def delete(self, db_name: str, table_name: str, key: Union[str, List[str]]) -> None:
        """`Delete rows from a table. <https://docs.cognite.com/api/v1/#operation/deleteRows>`_

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (Union[str, List[str]]): The key(s) of the row(s) to delete.

        Returns:
            None

        Examples:

            Delete rows from table::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> keys_to_delete = ["k1", "k2", "k3"]
                >>> c.raw.rows.delete("db1", "table1", keys_to_delete)
        """
        utils._auxiliary.assert_type(key, "key", [str, list])
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
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name), id=key
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
        if columns is not None:
            if not isinstance(columns, List):
                raise ValueError("Expected a list for argument columns")
            if len(columns) == 0:
                columns = ","
            else:
                columns = ",".join([str(x) for x in columns])

        return self._list(
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            limit=limit,
            method="GET",
            filter={
                "minLastUpdatedTime": min_last_updated_time,
                "maxLastUpdatedTime": max_last_updated_time,
                "columns": columns,
            },
        )
