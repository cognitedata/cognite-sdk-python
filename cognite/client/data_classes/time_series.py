from typing import Dict

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS


class TimeSeries(CogniteResource):
    def __init__(
        self,
        id=None,
        external_id=None,
        name=None,
        is_string=None,
        metadata=None,
        unit=None,
        asset_id=None,
        is_step=None,
        description=None,
        security_categories=None,
        data_set_id=None,
        created_time=None,
        last_updated_time=None,
        legacy_name=None,
        cognite_client=None,
    ):
        self.id = id
        self.external_id = external_id
        self.name = name
        self.is_string = is_string
        self.metadata = metadata
        self.unit = unit
        self.asset_id = asset_id
        self.is_step = is_step
        self.description = description
        self.security_categories = security_categories
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.legacy_name = legacy_name
        self._cognite_client = cognite_client

    def count(self):
        if self.is_string:
            raise ValueError("String time series does not support count aggregate.")
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=(MAX_TIMESTAMP_MS + 1), aggregates="count", granularity="100d"
        )
        return sum(dps.count)

    def latest(self, before=None):
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve_latest(**identifier, before=before)
        if dps:
            return dps[0]
        return None

    def first(self):
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=(MAX_TIMESTAMP_MS + 1), limit=1
        )
        if dps:
            return dps[0]
        return None

    def asset(self):
        if self.asset_id is None:
            raise ValueError("asset_id is None")
        return self._cognite_client.assets.retrieve(id=self.asset_id)


class TimeSeriesFilter(CogniteFilter):
    def __init__(
        self,
        name=None,
        unit=None,
        is_string=None,
        is_step=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        data_set_ids=None,
        external_id_prefix=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.name = name
        self.unit = unit
        self.is_string = is_string
        self.is_step = is_step
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance


class TimeSeriesUpdate(CogniteUpdate):
    class _PrimitiveTimeSeriesUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _ObjectTimeSeriesUpdate(CogniteObjectUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListTimeSeriesUpdate(CogniteListUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelTimeSeriesUpdate(CogniteLabelUpdate):
        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def external_id(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "externalId")

    @property
    def name(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "name")

    @property
    def metadata(self):
        return TimeSeriesUpdate._ObjectTimeSeriesUpdate(self, "metadata")

    @property
    def unit(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "unit")

    @property
    def asset_id(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "assetId")

    @property
    def description(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "description")

    @property
    def is_step(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "isStep")

    @property
    def security_categories(self):
        return TimeSeriesUpdate._ListTimeSeriesUpdate(self, "securityCategories")

    @property
    def data_set_id(self):
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "dataSetId")


class TimeSeriesAggregate(dict):
    def __init__(self, count=None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class TimeSeriesList(CogniteResourceList):
    _RESOURCE = TimeSeries
