from collections import defaultdict

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import *


# GenClass: RawDBRow
class Row(CogniteResource):
    """A row to store in the Raw DB.

    Args:
        key (str): Unique row key
        columns (Dict[str, Any]): Row data stored as a JSON object.
    """

    def __init__(self, key: str = None, columns: Dict[str, Any] = None, **kwargs):
        self.key = key
        self.columns = columns

    # GenStop
    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = utils.local_import("pandas")
        return pd.DataFrame([self.columns], [self.key])


class RowList(CogniteResourceList):
    _RESOURCE = Row
    _ASSERT_CLASSES = False

    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = utils.local_import("pandas")
        index = [row.key for row in self.data]
        data = defaultdict(lambda: [])
        for row in self.data:
            for col_name, value in row.columns.items():
                data[col_name].append(value)
        return pd.DataFrame(data, index)


# GenClass: RawDBTable
class Table(CogniteResource):
    """A NoSQL database table to store customer data

    Args:
        name (str): Unique name of the table
    """

    def __init__(self, name: str = None, **kwargs):
        self.name = name

    # GenStop
    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        return super().to_pandas([])


class TableList(CogniteResourceList):
    _RESOURCE = Table
    _ASSERT_CLASSES = False


# GenClass: RawDB
class Database(CogniteResource):
    """A NoSQL database to store customer data.

    Args:
        name (str): Unique name of a database.
    """

    def __init__(self, name: str = None, **kwargs):
        self.name = name

    # GenStop

    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        return super().to_pandas([])


class DatabaseList(CogniteResourceList):
    _RESOURCE = Database
    _ASSERT_CLASSES = False


class RawAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.databases = RawDatabasesAPI(*args, **kwargs)
        self.tables = RawTablesAPI(*args, **kwargs)
        self.rows = RawRowsAPI(*args, **kwargs)


class RawDatabasesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs"
    _LIMIT = 10000

    def __call__(self, chunk_size: int = None) -> Generator[Union[Database, DatabaseList], None, None]:
        """Iterate over databases

        Fetches dbs as they are iterated over, so you keep a limited number of dbs in memory.

        Args:
            chunk_size (int, optional): Number of dbs to return in each chunk. Defaults to yielding one db a time.
        """
        return self._list_generator(
            cls=DatabaseList, resource_path=self._RESOURCE_PATH, chunk_size=chunk_size, method="GET"
        )

    def __iter__(self) -> Generator[Database, None, None]:
        return self.__call__()

    def create(self, name: Union[str, List[str]]) -> Union[Database, DatabaseList]:
        """Create one or more databases.

        Args:
            name (Union[str, List[str]]: A db name or list of db names to create.

        Returns:
            Union[Database, DatabaseList]: Database or list of databases that has been created.
        """
        utils.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            items = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(cls=DatabaseList, resource_path=self._RESOURCE_PATH, items=items)

    def delete(self, name: Union[str, List[str]]) -> None:
        """Delete one or more databases.

        Args:
            name (Union[str, List[str]]: A db name or list of db names to delete.

        Returns:
            None
        """
        utils.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        self._post(self._RESOURCE_PATH + "/delete", json={"items": items})

    def list(self, limit: int = None) -> DatabaseList:
        """List databases

        Args:
            limit (int, optional): Maximum number of databases to return. If not specified, all dbs will be returned.

        Returns:
            DatabaseList: List of requested databases.
        """
        return self._list(cls=DatabaseList, resource_path=self._RESOURCE_PATH, method="GET", limit=limit)


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"
    _LIMIT = 10000

    def __call__(self, db_name: str, chunk_size: int = None) -> Generator[Union[Table, TableList], None, None]:
        """Iterate over tables

        Fetches tables as they are iterated over, so you keep a limited number of tables in memory.

        Args:
            db_name (str): Name of the database to iterate over tables for
            chunk_size (int, optional): Number of tables to return in each chunk. Defaults to yielding one table a time.
        """
        return self._list_generator(
            cls=TableList,
            resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
        )

    def create(self, db_name: str, name: Union[str, List[str]]) -> Union[Table, TableList]:
        """Create one or more tables.

        Args:
            db_name (str): Database to create the tables in.
            name (Union[str, List[str]]: A table name or list of table names to create.

        Returns:
            Union[Table, TableList]: Table or list of tables that has been created.
        """
        utils.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            items = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(
            cls=TableList, resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name), items=items
        )

    def delete(self, db_name: str, name: Union[str, List[str]]) -> None:
        """Delete one or more tables.

        Args:
            db_name (str): Database to delete tables from.
            name (Union[str, List[str]]: A table name or list of table names to delete.

        Returns:
            None
        """
        utils.assert_type(name, "name", [str, list])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        self._post(utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name) + "/delete", json={"items": items})

    def list(self, db_name: str, limit: int = None) -> TableList:
        """List tables

        Args:
            db_name (str): The database to list tables from.
            limit (int, optional): Maximum number of tables to return. If not specified, all tables will be returned.

        Returns:
            TableList: List of requested tables.
        """
        return self._list(
            cls=TableList,
            resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )


class RawRowsAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables/{}/rows"
    _LIMIT = 10000

    def __call__(
        self, db_name: str, table_name: str, chunk_size: int = None
    ) -> Generator[Union[Row, RowList], None, None]:
        """Iterate over rows.

        Fetches rows as they are iterated over, so you keep a limited number of rows in memory.

        Args:
            db_name (str): Name of the database
            table_name (str): Name of the table to iterate over rows for
            chunk_size (int, optional): Number of rows to return in each chunk. Defaults to yielding one row a time.
        """
        return self._list_generator(
            cls=RowList,
            resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            chunk_size=chunk_size,
            method="GET",
        )

    def insert(
        self, db_name: str, table_name: str, row: Union[List[Row], Row, Dict], ensure_parent: bool = False
    ) -> Union[Row, RowList]:
        """Insert one or more rows into a table.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            row (Union[List[Row], Row, Dict]): The row(s) to insert
            ensure_parent (bool): Create database/table if they don't already exist.

        Returns:
            Union[Row, RowList]: The created row(s).
        """
        items, is_single = self._process_row_input(row)
        if is_single:
            items = items[0]
        return self._create_multiple(
            cls=RowList,
            resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            items=items,
            params={"ensureParent": ensure_parent},
        )

    def _process_row_input(self, row: Union[List, Dict, Row]):
        utils.assert_type(row, "row", [list, dict, Row])
        rows = []
        is_single = False
        if isinstance(row, dict):
            is_single = len(row.keys()) == 1
            for key, columns in row.items():
                rows.append({"key": key, "columns": columns})
        elif isinstance(row, list):
            for elem in row:
                if isinstance(elem, Row):
                    rows.append(elem.dump(camel_case=True))
                else:
                    raise TypeError("list elements must be Row objects.")
        elif isinstance(row, Row):
            is_single = True
            rows.append(row.dump(camel_case=True))
        return rows, is_single

    def delete(self, db_name: str, table_name: str, key: Union[str, List[str]]) -> None:
        """Delete rows from a table.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (Union[str, List[str]]): The key(s) of the row(s) to delete.

        Returns:
            None
        """
        utils.assert_type(key, "key", [str, list])
        if isinstance(key, str):
            key = [key]
        items = [{"key": k} for k in key]
        self._post(
            utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name) + "/delete",
            json={"items": items},
        )

    def get(self, db_name: str, table_name: str, key: str) -> Row:
        """Retrieve a single row by key.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            key (str): The key of the row to retrieve.

        Returns:
            Row: The requested row.
        """
        return self._retrieve(
            cls=Row, resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name), id=key
        )

    def list(self, db_name: str, table_name: str, limit: int = None) -> RowList:
        """List rows in a table.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table.
            limit (int): The number of rows to retrieve.

        Returns:
            RowList: The requested rows.
        """
        return self._list(
            cls=RowList,
            resource_path=utils.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            limit=limit,
            method="GET",
        )
