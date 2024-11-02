from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from cognite.client.data_classes._base import (
    CogniteResourceList,
    NameTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._importing import local_import

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class RowCore(WriteableCogniteResource["RowWrite"], ABC):
    """No description.

    Args:
        key (str | None): Unique row key
        columns (dict[str, Any] | None): Row data stored as a JSON object.
    """

    def __init__(
        self,
        key: str | None = None,
        columns: dict[str, Any] | None = None,
    ) -> None:
        self.key = key
        self.columns = columns

    def get(self, attr: str, default: Any = None) -> Any:
        return (self.columns or {}).get(attr, default)

    def __getitem__(self, attr: str) -> Any:
        return (self.columns or {})[attr]

    def __setitem__(self, attr: str, value: Any) -> None:
        if self.columns is not None:
            self.columns[attr] = value
        else:
            raise RuntimeError("columns not set on Row instance")

    def __delitem__(self, attr: str) -> None:
        if self.columns is not None:
            del self.columns[attr]
        else:
            raise RuntimeError("columns not set on Row instance")

    def __contains__(self, attr: str) -> bool:
        return self.columns is not None and attr in self.columns

    def to_pandas(self) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = local_import("pandas")
        return pd.DataFrame([self.columns], [self.key])


T_Row = TypeVar("T_Row", bound=RowCore)


class Row(RowCore):
    """This represents a row in a NO-SQL table.
    This is the reading version of the Row class, which is used when retrieving a row.

    Args:
        key (str | None): Unique row key
        columns (dict[str, Any] | None): Row data stored as a JSON object.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        key: str | None = None,
        columns: dict[str, Any] | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(key, columns)
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> RowWrite:
        """Returns this Row as a RowWrite"""
        if self.key is None or self.columns is None:
            raise ValueError("key and columns are required to create a Row")
        return RowWrite(key=self.key, columns=self.columns)


class RowWrite(RowCore):
    """This represents a row in a NO-SQL table.
    This is the writing version of the Row class, which is used when creating a row.

    Args:
        key (str): Unique row key
        columns (dict[str, Any]): Row data stored as a JSON object.
    """

    def __init__(
        self,
        key: str,
        columns: dict[str, Any],
    ) -> None:
        super().__init__(key, columns)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RowWrite:
        return cls(resource["key"], resource["columns"])

    def as_write(self) -> RowWrite:
        """Returns this RowWrite instance."""
        return self


class RowListCore(WriteableCogniteResourceList[RowWrite, T_Row], ABC):
    def to_pandas(self) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = local_import("pandas")
        if not self:
            return pd.DataFrame(columns=[], index=[])
        index, data = zip(*((row.key, row.columns) for row in self))
        return pd.DataFrame.from_records(data, index=index)


class RowWriteList(RowListCore[RowWrite]):
    _RESOURCE = RowWrite

    def as_write(self) -> RowWriteList:
        return self


class RowList(RowListCore[Row]):
    _RESOURCE = Row

    def as_write(self) -> RowWriteList:
        """Returns this RowList as a RowWriteList"""
        return RowWriteList([row.as_write() for row in self.data])


class TableCore(WriteableCogniteResource["TableWrite"]):
    """A NoSQL database table to store customer data

    Args:
        name (str | None): Unique name of the table
    """

    def __init__(
        self,
        name: str | None = None,
    ) -> None:
        self.name = name


class Table(TableCore):
    """A NoSQL database table to store customer data.
    This is the reading version of the Table class, which is used when retrieving a table.

    Args:
        name (str | None): Unique name of the table
        created_time (int | None): Time the table was created.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(name)
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

        self._db_name: str | None = None

    def as_write(self) -> TableWrite:
        """Returns this Table as a TableWrite"""
        if self.name is None:
            raise ValueError("name is required to create a Table")
        return TableWrite(name=self.name)

    @overload
    def rows(self, key: str, limit: int | None = None) -> Row | None: ...

    @overload
    def rows(self, key: None = None, limit: int | None = None) -> RowList: ...

    def rows(self, key: str | None = None, limit: int | None = None) -> Row | RowList | None:
        """Get the rows in this table.

        Args:
            key (str | None): Specify a key to return only that row.
            limit (int | None): The number of rows to return.

        Returns:
            Row | RowList | None: List of tables in this database.
        """
        if self._db_name is None:
            raise ValueError("Table is not linked to a database, did you instantiate it yourself?")
        elif self.name is None:
            raise ValueError("Table 'name' is missing")

        if key is not None:
            return self._cognite_client.raw.rows.retrieve(db_name=self._db_name, table_name=self.name, key=key)
        return self._cognite_client.raw.rows.list(db_name=self._db_name, table_name=self.name, limit=limit)


class TableWrite(TableCore):
    """A NoSQL database table to store customer data
    This is the writing version of the Table class, which is used when creating a table.

    Args:
        name (str): Unique name of the table
    """

    def __init__(
        self,
        name: str,
    ) -> None:
        super().__init__(name)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TableWrite:
        return cls(resource["name"])

    def as_write(self) -> TableWrite:
        """Returns this TableWrite instance."""
        return self


class TableWriteList(CogniteResourceList[TableWrite], NameTransformerMixin):
    _RESOURCE = TableWrite


class TableList(WriteableCogniteResourceList[TableWrite, Table], NameTransformerMixin):
    _RESOURCE = Table

    def as_write(self) -> TableWriteList:
        """Returns this TableList as a TableWriteList"""
        return TableWriteList([table.as_write() for table in self.data])


class DatabaseCore(WriteableCogniteResource["DatabaseWrite"], ABC):
    """A NoSQL database to store customer data.

    Args:
        name (str | None): Unique name of a database.
    """

    def __init__(
        self,
        name: str | None = None,
    ) -> None:
        self.name = name


class Database(DatabaseCore):
    """A NoSQL database to store customer data.

    Args:
        name (str | None): Unique name of a database.
        created_time (int | None): Time the database was created.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(name)
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> DatabaseWrite:
        """Returns this Database as a DatabaseWrite"""
        if self.name is None:
            raise ValueError("name is required to create a Database")
        return DatabaseWrite(name=self.name)

    def tables(self, limit: int | None = None) -> TableList:
        """Get the tables in this database.

        Args:
            limit (int | None): The number of tables to return.

        Returns:
            TableList: List of tables in this database.
        """
        if self.name is None:
            raise ValueError("Unable to list tables, 'name' is not set on instance")
        return self._cognite_client.raw.tables.list(db_name=self.name, limit=limit)


class DatabaseWrite(DatabaseCore):
    """A NoSQL database to store customer data.

    Args:
        name (str): Unique name of a database.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> DatabaseWrite:
        return cls(resource["name"])

    def as_write(self) -> DatabaseWrite:
        """Returns this DatabaseWrite instance."""
        return self


class DatabaseWriteList(CogniteResourceList[DatabaseWrite], NameTransformerMixin):
    _RESOURCE = DatabaseWrite


class DatabaseList(WriteableCogniteResourceList[DatabaseWrite, Database], NameTransformerMixin):
    _RESOURCE = Database

    def as_write(self) -> DatabaseWriteList:
        """Returns this DatabaseList as a DatabaseWriteList"""
        return DatabaseWriteList([db.as_write() for db in self.data])
