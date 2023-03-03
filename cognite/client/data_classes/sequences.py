
import json
import math
from cognite.client import utils
from cognite.client.data_classes._base import CogniteFilter, CogniteLabelUpdate, CogniteListUpdate, CogniteObjectUpdate, CognitePrimitiveUpdate, CognitePropertyClassUtil, CogniteResource, CogniteResourceList, CogniteUpdate
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier
if TYPE_CHECKING:
    import pandas
    from cognite.client import CogniteClient

class Sequence(CogniteResource):

    def __init__(self, id=None, name=None, description=None, asset_id=None, external_id=None, metadata=None, columns=None, created_time=None, last_updated_time=None, data_set_id=None, cognite_client=None):
        self.id = id
        self.name = name
        self.description = description
        self.asset_id = asset_id
        self.external_id = external_id
        self.metadata = metadata
        self.columns = columns
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_id = data_set_id
        self._cognite_client = cast('CogniteClient', cognite_client)

    def rows(self, start, end):
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        return self._cognite_client.sequences.data.retrieve(**identifier, start=start, end=end)

    @property
    def column_external_ids(self):
        assert (self.columns is not None)
        return [cast(str, c.get('externalId')) for c in self.columns]

    @property
    def column_value_types(self):
        assert (self.columns is not None)
        return [cast(str, c.get('valueType')) for c in self.columns]

class SequenceFilter(CogniteFilter):

    def __init__(self, name=None, external_id_prefix=None, metadata=None, asset_ids=None, asset_subtree_ids=None, created_time=None, last_updated_time=None, data_set_ids=None, cognite_client=None):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_ids = data_set_ids
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if (instance.created_time is not None):
                instance.created_time = TimestampRange(**instance.created_time)
            if (instance.last_updated_time is not None):
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

class SequenceColumnUpdate(CogniteUpdate):

    class _PrimitiveSequenceColumnUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectSequenceColumnUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def description(self):
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, 'description')

    @property
    def external_id(self):
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, 'externalId')

    @property
    def name(self):
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, 'name')

    @property
    def metadata(self):
        return SequenceColumnUpdate._ObjectSequenceColumnUpdate(self, 'metadata')

class SequenceUpdate(CogniteUpdate):

    class _PrimitiveSequenceUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectSequenceUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListSequenceUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelSequenceUpdate(CogniteLabelUpdate):

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ColumnsSequenceUpdate(CogniteListUpdate):

        def add(self, value):
            single_item = (not isinstance(value, list))
            if single_item:
                value_list = cast(List[str], [value])
            else:
                value_list = cast(List[str], value)
            return self._add(value_list)

        def remove(self, value):
            single_item = (not isinstance(value, list))
            if single_item:
                value_list = cast(List[str], [value])
            else:
                value_list = cast(List[str], value)
            return self._remove([{'externalId': id} for id in value_list])

        def modify(self, value):
            return self._modify([col.dump() for col in value])

    @property
    def name(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, 'name')

    @property
    def description(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, 'description')

    @property
    def asset_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, 'assetId')

    @property
    def external_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, 'externalId')

    @property
    def metadata(self):
        return SequenceUpdate._ObjectSequenceUpdate(self, 'metadata')

    @property
    def data_set_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, 'dataSetId')

    @property
    def columns(self):
        return SequenceUpdate._ColumnsSequenceUpdate(self, 'columns')

class SequenceAggregate(dict):

    def __init__(self, count=None, **kwargs: Any):
        self.count = count
        self.update(kwargs)
    count = CognitePropertyClassUtil.declare_property('count')

class SequenceList(CogniteResourceList):
    _RESOURCE = Sequence

class SequenceData(CogniteResource):

    def __init__(self, id=None, external_id=None, rows=None, row_numbers=None, values=None, columns=None):
        if rows:
            row_numbers = [r['rowNumber'] for r in rows]
            values = [r['values'] for r in rows]
        self.id = id
        self.external_id = external_id
        self.row_numbers = (row_numbers or [])
        self.values = (values or [])
        self.columns = columns

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def __len__(self):
        return len(self.row_numbers)

    def __eq__(self, other):
        return ((type(self) == type(other)) and (self.id == other.id) and (self.external_id == other.external_id) and (self.row_numbers == other.row_numbers) and (self.values == other.values))

    def __getitem__(self, item):
        if isinstance(item, slice):
            raise TypeError('Slicing SequenceData not supported')
        return self.values[self.row_numbers.index(item)]

    def get_column(self, external_id):
        try:
            ix = self.column_external_ids.index(external_id)
        except ValueError:
            raise ValueError(f'Column {external_id} not found, Sequence column external ids are {self.column_external_ids}')
        return [r[ix] for r in self.values]

    def items(self):
        for (row, values) in zip(self.row_numbers, self.values):
            (yield (row, list(values)))

    def dump(self, camel_case=False):
        dumped = {'id': self.id, 'external_id': self.external_id, 'columns': self.columns, 'rows': [{'rowNumber': r, 'values': v} for (r, v) in zip(self.row_numbers, self.values)]}
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for (key, value) in dumped.items()}
        return {key: value for (key, value) in dumped.items() if (value is not None)}

    def to_pandas(self, column_names='columnExternalId'):
        pd = utils._auxiliary.local_import('pandas')
        options = ['externalId', 'id', 'columnExternalId', 'id|columnExternalId', 'externalId|columnExternalId']
        if (column_names not in options):
            raise ValueError(f"Invalid column_names value '{column_names}', should be one of {options}")
        column_names = column_names.replace('columnExternalId', '{columnExternalId}').replace('externalId', '{externalId}').replace('id', '{id}')
        df_columns = [column_names.format(id=str(self.id), externalId=str(self.external_id), columnExternalId=eid) for eid in self.column_external_ids]
        return pd.DataFrame([[(x if (x is not None) else math.nan) for x in r] for r in self.values], index=self.row_numbers, columns=df_columns)

    @property
    def column_external_ids(self):
        assert (self.columns is not None)
        return [cast(str, c.get('externalId')) for c in self.columns]

    @property
    def column_value_types(self):
        assert (self.columns is not None)
        return [cast(str, c.get('valueType')) for c in self.columns]

class SequenceDataList(CogniteResourceList):
    _RESOURCE = SequenceData

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def to_pandas(self, column_names='externalId|columnExternalId'):
        pd = utils._auxiliary.local_import('pandas')
        return pd.concat([seq_data.to_pandas(column_names=column_names) for seq_data in self.data], axis=1)
