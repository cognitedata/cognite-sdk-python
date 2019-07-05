from collections import defaultdict
from typing import *

from cognite.client.data_classes._base import *


# GenClass: RawDBRow
class Row(CogniteResource):
    """No description.

    Args:
        key (str): Unique row key
        columns (Dict[str, Any]): Row data stored as a JSON object.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self, key: str = None, columns: Dict[str, Any] = None, last_updated_time: int = None, cognite_client=None
    ):
        self.key = key
        self.columns = columns
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop
    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = utils._auxiliary.local_import("pandas")
        return pd.DataFrame([self.columns], [self.key])


class RowList(CogniteResourceList):
    _RESOURCE = Row
    _ASSERT_CLASSES = False

    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        pd = utils._auxiliary.local_import("pandas")
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
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, cognite_client=None):
        self.name = name
        self._cognite_client = cognite_client
        # GenStop
        self._db_name = None

    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        return super().to_pandas([])

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
    _ASSERT_CLASSES = False


# GenClass: RawDB
class Database(CogniteResource):
    """A NoSQL database to store customer data.

    Args:
        name (str): Unique name of a database.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, cognite_client=None):
        self.name = name
        self._cognite_client = cognite_client

    # GenStop

    def to_pandas(self):
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The pandas DataFrame representing this instance.
        """
        return super().to_pandas([])

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
    _ASSERT_CLASSES = False
