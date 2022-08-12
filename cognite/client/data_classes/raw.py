from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, Optional, Union, cast

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class Row(CogniteResource):
    """No description.

    Args:
        key (str): Unique row key
        columns (Dict[str, Any]): Row data stored as a JSON object.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        key: str = None,
        columns: Dict[str, Any] = None,
        last_updated_time: int = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.key = key
        self.columns = columns
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def to_pandas(self) -> "pandas.DataFrame":  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
        return pd.DataFrame([self.columns], [self.key])

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()


class RowList(CogniteResourceList):
    _RESOURCE = Row

    def to_pandas(self) -> "pandas.DataFrame":  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
        return pd.DataFrame.from_dict(OrderedDict(((d.key, d.columns) for d in self.data)), orient="index")

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()


class Table(CogniteResource):
    """A NoSQL database table to store customer data

    Args:
        name (str): Unique name of the table
        created_time (int): Time the table was created.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, created_time: int = None, cognite_client: "CogniteClient" = None):
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

        self._db_name: Optional[str] = None

    def rows(self, key: str = None, limit: int = None) -> Union[Row, RowList]:
        """Get the rows in this table.

        Args:
            key (str): Specify a key to return only that row.
            limit (int): The number of rows to return.

        Returns:
            Union[Row, RowList]: List of tables in this database.
        """
        if key:
            return self._cognite_client.raw.rows.retrieve(db_name=self._db_name, table_name=self.name, key=key)
        return self._cognite_client.raw.rows.list(db_name=self._db_name, table_name=self.name, limit=limit)


class TableList(CogniteResourceList):
    _RESOURCE = Table


class Database(CogniteResource):
    """A NoSQL database to store customer data.

    Args:
        name (str): Unique name of a database.
        created_time (int): Time the database was created.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, created_time: int = None, cognite_client: "CogniteClient" = None):
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def tables(self, limit: int = None) -> TableList:
        """Get the tables in this database.

        Args:
            limit (int): The number of tables to return.

        Returns:
            TableList: List of tables in this database.
        """
        return self._cognite_client.raw.tables.list(db_name=self.name, limit=limit)


class DatabaseList(CogniteResourceList):
    _RESOURCE = Database
