
from collections import OrderedDict
from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
if TYPE_CHECKING:
    import pandas
    from cognite.client import CogniteClient

class Row(CogniteResource):

    def __init__(self, key=None, columns=None, last_updated_time=None, cognite_client=None):
        self.key = key
        self.columns = columns
        self.last_updated_time = last_updated_time
        self._cognite_client = cast('CogniteClient', cognite_client)

    def to_pandas(self):
        pd = cast(Any, utils._auxiliary.local_import('pandas'))
        return pd.DataFrame([self.columns], [self.key])

class RowList(CogniteResourceList):
    _RESOURCE = Row

    def to_pandas(self):
        pd = cast(Any, utils._auxiliary.local_import('pandas'))
        return pd.DataFrame.from_dict(OrderedDict(((d.key, d.columns) for d in self.data)), orient='index')

class Table(CogniteResource):

    def __init__(self, name=None, created_time=None, cognite_client=None):
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast('CogniteClient', cognite_client)
        self._db_name: Optional[str] = None

    def rows(self, key=None, limit=None):
        if key:
            return self._cognite_client.raw.rows.retrieve(db_name=self._db_name, table_name=self.name, key=key)
        return self._cognite_client.raw.rows.list(db_name=self._db_name, table_name=self.name, limit=limit)

class TableList(CogniteResourceList):
    _RESOURCE = Table

class Database(CogniteResource):

    def __init__(self, name=None, created_time=None, cognite_client=None):
        self.name = name
        self.created_time = created_time
        self._cognite_client = cast('CogniteClient', cognite_client)

    def tables(self, limit=None):
        return self._cognite_client.raw.tables.list(db_name=self.name, limit=limit)

class DatabaseList(CogniteResourceList):
    _RESOURCE = Database
