from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any, cast, overload

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import local_import

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class Row(CogniteResource):
    """No description.

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
        self.key = key
        self.columns = columns
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def to_pandas(self) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = cast(Any, local_import("pandas"))
        return pd.DataFrame([self.columns], [self.key])


class RowList(CogniteResourceList[Row]):
    _RESOURCE = Row

    def to_pandas(self) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = cast(Any, local_import("pandas"))
        return pd.DataFrame.from_dict(OrderedDict((d.key, d.columns) for d in self.data), orient="index")


class Table(CogniteResource):
    """A NoSQL database table to store customer data

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
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

        self._db_name: str | None = None

    @overload
    def rows(self, key: str, limit: int | None = None) -> Row | None:
        ...

    @overload
    def rows(self, key: None = None, limit: int | None = None) -> RowList:
        ...

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


class TableList(CogniteResourceList[Table]):
    _RESOURCE = Table


class Database(CogniteResource):
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
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

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


class DatabaseList(CogniteResourceList[Database]):
    _RESOURCE = Database
