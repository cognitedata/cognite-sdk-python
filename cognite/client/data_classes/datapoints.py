
import json
import operator as op
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, convert_all_keys_to_snake_case, find_duplicates, local_import, to_camel_case
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._pandas_helpers import concat_dataframes_with_nullable_int_cols, notebook_display_with_fallback
ALL_SORTED_DP_AGGS = sorted(['average', 'continuous_variance', 'count', 'discrete_variance', 'interpolation', 'max', 'min', 'step_interpolation', 'sum', 'total_variation'])
if TYPE_CHECKING:
    import pandas
    from cognite.client import CogniteClient
try:
    import numpy as np
    import numpy.typing as npt
    NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
    NumpyInt64Array = npt.NDArray[np.int64]
    NumpyFloat64Array = npt.NDArray[np.float64]
    NumpyObjArray = npt.NDArray[np.object_]
    NUMPY_IS_AVAILABLE = True
except ImportError:
    NUMPY_IS_AVAILABLE = False

@dataclass(frozen=True)
class LatestDatapointQuery():
    id: Optional[int] = None
    external_id: Optional[str] = None
    before: Union[(None, int, str, datetime)] = None

    def __post_init__(self):
        Identifier.of_either(self.id, self.external_id)

class Datapoint(CogniteResource):

    def __init__(self, timestamp=None, value=None, average=None, max=None, min=None, count=None, sum=None, interpolation=None, step_interpolation=None, continuous_variance=None, discrete_variance=None, total_variation=None):
        self.timestamp = timestamp
        self.value = value
        self.average = average
        self.max = max
        self.min = min
        self.count = count
        self.sum = sum
        self.interpolation = interpolation
        self.step_interpolation = step_interpolation
        self.continuous_variance = continuous_variance
        self.discrete_variance = discrete_variance
        self.total_variation = total_variation

    def to_pandas(self, camel_case=False):
        pd = cast(Any, local_import('pandas'))
        dumped = self.dump(camel_case=camel_case)
        timestamp = dumped.pop('timestamp')
        return pd.DataFrame(dumped, index=[pd.Timestamp(timestamp, unit='ms')])

class DatapointsArray(CogniteResource):

    def __init__(self, id=None, external_id=None, is_string=None, is_step=None, unit=None, granularity=None, timestamp=None, value=None, average=None, max=None, min=None, count=None, sum=None, interpolation=None, step_interpolation=None, continuous_variance=None, discrete_variance=None, total_variation=None):
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
        self.granularity = granularity
        self.timestamp = (timestamp if (timestamp is not None) else np.array([], dtype='datetime64[ns]'))
        self.value = value
        self.average = average
        self.max = max
        self.min = min
        self.count = count
        self.sum = sum
        self.interpolation = interpolation
        self.step_interpolation = step_interpolation
        self.continuous_variance = continuous_variance
        self.discrete_variance = discrete_variance
        self.total_variation = total_variation

    @property
    def _ts_info(self):
        return {'id': self.id, 'external_id': self.external_id, 'is_string': self.is_string, 'is_step': self.is_step, 'unit': self.unit, 'granularity': self.granularity}

    @classmethod
    def _load(cls, dps_dct):
        assert isinstance(dps_dct['timestamp'], np.ndarray)
        dps_dct['timestamp'] = dps_dct['timestamp'].astype('datetime64[ms]').astype('datetime64[ns]')
        return cls(**convert_all_keys_to_snake_case(dps_dct))

    def __len__(self):
        return len(self.timestamp)

    def __eq__(self, other):
        return (id(self) == id(other))

    def __str__(self):
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    @overload
    def __getitem__(self, item):
        ...

    @overload
    def __getitem__(self, item):
        ...

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self._slice(item)
        (attrs, arrays) = self._data_fields()
        return Datapoint(timestamp=(self._dtype_fix(arrays[0][item]) // 1000000), **{attr: self._dtype_fix(arr[item]) for (attr, arr) in zip(attrs[1:], arrays[1:])})

    def _slice(self, part):
        data: Dict[(str, Any)] = {attr: arr[part] for (attr, arr) in zip(*self._data_fields())}
        return DatapointsArray(**self._ts_info, **data)

    def __iter__(self):
        (attrs, arrays) = self._data_fields()
        (yield from (Datapoint(timestamp=(self._dtype_fix(row[0]) // 1000000), **dict(zip(attrs[1:], map(self._dtype_fix, row[1:])))) for row in zip(*arrays)))

    @cached_property
    def _dtype_fix(self):
        if self.is_string:
            return (lambda s: s)
        return op.methodcaller('item')

    def _data_fields(self):
        data_field_tuples = [(attr, getattr(self, attr)) for attr in ('timestamp', 'value', *ALL_SORTED_DP_AGGS) if (getattr(self, attr) is not None)]
        (attrs, arrays) = map(list, zip(*data_field_tuples))
        return (attrs, arrays)

    def dump(self, camel_case=False, convert_timestamps=False):
        (attrs, arrays) = self._data_fields()
        if (not convert_timestamps):
            arrays[0] = arrays[0].astype('datetime64[ms]').astype(np.int64)
        else:
            arrays[0] = arrays[0].astype('datetime64[ms]').astype(datetime).astype(str)
        if camel_case:
            attrs = list(map(to_camel_case, attrs))
        dumped = {**self._ts_info, 'datapoints': [dict(zip(attrs, map(self._dtype_fix, row))) for row in zip(*arrays)]}
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {k: v for (k, v) in dumped.items() if (v is not None)}

    def to_pandas(self, column_names='external_id', include_aggregate_name=True, include_granularity_name=False):
        pd = cast(Any, local_import('pandas'))
        if (column_names == 'id'):
            if (self.id is None):
                raise ValueError('Unable to use `id` as column name(s), not set on object')
            identifier = str(self.id)
        elif (column_names == 'external_id'):
            if (self.external_id is not None):
                identifier = self.external_id
            elif (self.id is not None):
                identifier = str(self.id)
                warnings.warn(f'Time series does not have an external ID, so its ID ({self.id}) was used instead as the column name in the DataFrame. If this is expected, consider passing `column_names="id"` to silence this warning.', UserWarning)
            else:
                raise ValueError('Object missing both `id` and `external_id` attributes')
        else:
            raise ValueError("Argument `column_names` must be either 'external_id' or 'id'")
        if (self.value is not None):
            return pd.DataFrame({identifier: self.value}, index=self.timestamp, copy=False)
        ((_, *agg_names), (_, *arrays)) = self._data_fields()
        columns = [((str(identifier) + (include_aggregate_name * f'|{agg}')) + (include_granularity_name * f'|{self.granularity}')) for agg in agg_names]
        df = pd.DataFrame(dict(enumerate(arrays)), index=self.timestamp, copy=False)
        df.columns = columns
        return df

class Datapoints(CogniteResource):

    def __init__(self, id=None, external_id=None, is_string=None, is_step=None, unit=None, granularity=None, timestamp=None, value=None, average=None, max=None, min=None, count=None, sum=None, interpolation=None, step_interpolation=None, continuous_variance=None, discrete_variance=None, total_variation=None, error=None):
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
        self.granularity = granularity
        self.timestamp = (timestamp or [])
        self.value = value
        self.average = average
        self.max = max
        self.min = min
        self.count = count
        self.sum = sum
        self.interpolation = interpolation
        self.step_interpolation = step_interpolation
        self.continuous_variance = continuous_variance
        self.discrete_variance = discrete_variance
        self.total_variation = total_variation
        self.error = error
        self.__datapoint_objects: Optional[List[Datapoint]] = None

    def __str__(self):
        item = self.dump()
        item['datapoints'] = utils._time.convert_time_attributes_to_datetime(item['datapoints'])
        return json.dumps(item, indent=4)

    def __len__(self):
        return len(self.timestamp)

    def __eq__(self, other):
        return ((type(self) == type(other)) and (self.id == other.id) and (self.external_id == other.external_id) and (list(self._get_non_empty_data_fields()) == list(other._get_non_empty_data_fields())))

    @overload
    def __getitem__(self, item):
        ...

    @overload
    def __getitem__(self, item):
        ...

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self._slice(item)
        dp_args = {}
        for (attr, values) in self._get_non_empty_data_fields():
            dp_args[attr] = values[item]
        return Datapoint(**dp_args)

    def __iter__(self):
        (yield from self.__get_datapoint_objects())

    def dump(self, camel_case=False):
        dumped = {'id': self.id, 'external_id': self.external_id, 'is_string': self.is_string, 'is_step': self.is_step, 'unit': self.unit, 'datapoints': [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()]}
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for (key, value) in dumped.items()}
        return {key: value for (key, value) in dumped.items() if (value is not None)}

    def to_pandas(self, column_names='external_id', include_aggregate_name=True, include_granularity_name=False, include_errors=False):
        pd = cast(Any, local_import('pandas'))
        if (column_names in ['external_id', 'externalId']):
            identifier = (self.external_id if (self.external_id is not None) else self.id)
        elif (column_names == 'id'):
            identifier = self.id
        else:
            raise ValueError("column_names must be 'external_id' or 'id'")
        if (include_errors and (self.error is None)):
            raise ValueError("Unable to 'include_errors', only available for data from synthetic datapoint queries")
        (field_names, data_lists) = ([], [])
        data_fields = self._get_non_empty_data_fields(get_empty_lists=True, get_error=include_errors)
        if (not include_errors):
            data_fields = sorted(data_fields)
        for (attr, data) in data_fields:
            if (attr == 'timestamp'):
                continue
            id_col_name = str(identifier)
            if (attr == 'value'):
                field_names.append(id_col_name)
                data_lists.append(data)
                continue
            if include_aggregate_name:
                id_col_name += f'|{attr}'
            if (include_granularity_name and (self.granularity is not None)):
                id_col_name += f'|{self.granularity}'
            field_names.append(id_col_name)
            if (attr == 'error'):
                data_lists.append(data)
                continue
            data = pd.to_numeric(data, errors='coerce')
            if (attr == 'count'):
                data_lists.append(data.astype('int64'))
            else:
                data_lists.append(data.astype('float64'))
        idx = pd.to_datetime(self.timestamp, unit='ms')
        df = pd.DataFrame(dict(enumerate(data_lists)), index=idx)
        df.columns = field_names
        return df

    @classmethod
    def _load(cls, dps_object, expected_fields=None, cognite_client=None):
        del cognite_client
        instance = cls(id=dps_object.get('id'), external_id=dps_object.get('externalId'), is_string=dps_object['isString'], is_step=dps_object.get('isStep'), unit=dps_object.get('unit'))
        expected_fields = ((expected_fields or ['value']) + ['timestamp'])
        if (len(dps_object['datapoints']) == 0):
            for key in expected_fields:
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, [])
        else:
            for key in expected_fields:
                data = [dp.get(key) for dp in dps_object['datapoints']]
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, data)
        return instance

    def _extend(self, other_dps):
        if ((self.id is None) and (self.external_id is None)):
            self.id = other_dps.id
            self.external_id = other_dps.external_id
            self.is_string = other_dps.is_string
            self.is_step = other_dps.is_step
            self.unit = other_dps.unit
        for (attr, other_value) in other_dps._get_non_empty_data_fields(get_empty_lists=True):
            value = getattr(self, attr)
            if (not value):
                setattr(self, attr, other_value)
            else:
                value.extend(other_value)

    def _get_non_empty_data_fields(self, get_empty_lists=False, get_error=True):
        non_empty_data_fields = []
        skip_attrs = {'id', 'external_id', 'is_string', 'is_step', 'unit', 'granularity'}
        for (attr, value) in self.__dict__.copy().items():
            if ((attr not in skip_attrs) and (attr[0] != '_') and ((attr != 'error') or get_error)):
                if ((value is not None) or (attr == 'timestamp')):
                    if ((len(value) > 0) or get_empty_lists or (attr == 'timestamp')):
                        non_empty_data_fields.append((attr, value))
        return non_empty_data_fields

    def __get_datapoint_objects(self):
        if (self.__datapoint_objects is not None):
            return self.__datapoint_objects
        fields = self._get_non_empty_data_fields(get_error=False)
        new_dps_objects = []
        for i in range(len(self)):
            dp_args = {}
            for (attr, value) in fields:
                dp_args[attr] = value[i]
            new_dps_objects.append(Datapoint(**dp_args))
        self.__datapoint_objects = new_dps_objects
        return self.__datapoint_objects

    def _slice(self, slice):
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for (attr, value) in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        return truncated_datapoints

    def _repr_html_(self):
        is_synthetic_dps = (self.error is not None)
        return notebook_display_with_fallback(self, include_errors=is_synthetic_dps)

class DatapointsArrayList(CogniteResourceList):
    _RESOURCE = DatapointsArray

    def __init__(self, resources, cognite_client=None):
        super().__init__(resources, cognite_client)
        ids = [dps.id for dps in self if (dps.id is not None)]
        xids = [dps.external_id for dps in self if (dps.external_id is not None)]
        (dupe_ids, id_dct) = (find_duplicates(ids), defaultdict(list))
        (dupe_xids, xid_dct) = (find_duplicates(xids), defaultdict(list))
        for dps in self:
            id_ = dps.id
            if ((id_ is not None) and (id_ in dupe_ids)):
                id_dct[id_].append(dps)
            xid = dps.external_id
            if ((xid is not None) and (xid in dupe_xids)):
                xid_dct[xid].append(dps)
        self._id_to_item.update(id_dct)
        self._external_id_to_item.update(xid_dct)

    def get(self, id=None, external_id=None):
        return super().get(id, external_id)

    def __str__(self):
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    def to_pandas(self, column_names='external_id', include_aggregate_name=True, include_granularity_name=False):
        pd = cast(Any, local_import('pandas'))
        dfs = [dps.to_pandas(column_names, include_aggregate_name, include_granularity_name) for dps in self]
        if (not dfs):
            return pd.DataFrame(index=pd.to_datetime([]))
        return concat_dataframes_with_nullable_int_cols(dfs)

    def dump(self, camel_case=False, convert_timestamps=False):
        return [dps.dump(camel_case, convert_timestamps) for dps in self]

class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints

    def __init__(self, resources, cognite_client=None):
        super().__init__(resources, cognite_client)
        ids = [dps.id for dps in self if (dps.id is not None)]
        xids = [dps.external_id for dps in self if (dps.external_id is not None)]
        (dupe_ids, id_dct) = (find_duplicates(ids), defaultdict(list))
        (dupe_xids, xid_dct) = (find_duplicates(xids), defaultdict(list))
        for dps in self:
            id_ = dps.id
            if ((id_ is not None) and (id_ in dupe_ids)):
                id_dct[id_].append(dps)
            xid = dps.external_id
            if ((xid is not None) and (xid in dupe_xids)):
                xid_dct[xid].append(dps)
        self._id_to_item.update(id_dct)
        self._external_id_to_item.update(xid_dct)

    def get(self, id=None, external_id=None):
        return super().get(id, external_id)

    def __str__(self):
        item = self.dump()
        for i in item:
            i['datapoints'] = utils._time.convert_time_attributes_to_datetime(i['datapoints'])
        return json.dumps(item, default=(lambda x: x.__dict__), indent=4)

    def to_pandas(self, column_names='external_id', include_aggregate_name=True, include_granularity_name=False):
        pd = cast(Any, local_import('pandas'))
        dfs = [dps.to_pandas(column_names, include_aggregate_name, include_granularity_name) for dps in self]
        if (not dfs):
            return pd.DataFrame(index=pd.to_datetime([]))
        return concat_dataframes_with_nullable_int_cols(dfs)
