
from cognite.client.data_classes._base import CogniteFilter, CogniteLabelUpdate, CogniteListUpdate, CogniteObjectUpdate, CognitePrimitiveUpdate, CognitePropertyClassUtil, CogniteResource, CogniteResourceList, CogniteUpdate
from cognite.client.data_classes.shared import TimestampRange
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class EndTimeFilter(dict):

    def __init__(self, max=None, min=None, is_null=None, **kwargs: Any):
        self.max = max
        self.min = min
        self.is_null = is_null
        self.update(kwargs)
    max = CognitePropertyClassUtil.declare_property('max')
    min = CognitePropertyClassUtil.declare_property('min')
    is_null = CognitePropertyClassUtil.declare_property('isNull')

class Event(CogniteResource):

    def __init__(self, external_id=None, data_set_id=None, start_time=None, end_time=None, type=None, subtype=None, description=None, metadata=None, asset_ids=None, source=None, id=None, last_updated_time=None, created_time=None, cognite_client=None):
        self.external_id = external_id
        self.data_set_id = data_set_id
        self.start_time = start_time
        self.end_time = end_time
        self.type = type
        self.subtype = subtype
        self.description = description
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source
        self.id = id
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast('CogniteClient', cognite_client)

class EventFilter(CogniteFilter):

    def __init__(self, start_time=None, end_time=None, active_at_time=None, metadata=None, asset_ids=None, asset_external_ids=None, asset_subtree_ids=None, data_set_ids=None, source=None, type=None, subtype=None, created_time=None, last_updated_time=None, external_id_prefix=None, cognite_client=None):
        self.start_time = start_time
        self.end_time = end_time
        self.active_at_time = active_at_time
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.source = source
        self.type = type
        self.subtype = subtype
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if (instance.start_time is not None):
                instance.start_time = TimestampRange(**instance.start_time)
            if (instance.end_time is not None):
                instance.end_time = EndTimeFilter(**instance.end_time)
            if (instance.active_at_time is not None):
                instance.active_at_time = TimestampRange(**instance.active_at_time)
            if (instance.created_time is not None):
                instance.created_time = TimestampRange(**instance.created_time)
            if (instance.last_updated_time is not None):
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

class EventUpdate(CogniteUpdate):

    class _PrimitiveEventUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectEventUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListEventUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelEventUpdate(CogniteLabelUpdate):

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def external_id(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'externalId')

    @property
    def data_set_id(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'dataSetId')

    @property
    def start_time(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'startTime')

    @property
    def end_time(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'endTime')

    @property
    def description(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'description')

    @property
    def metadata(self):
        return EventUpdate._ObjectEventUpdate(self, 'metadata')

    @property
    def asset_ids(self):
        return EventUpdate._ListEventUpdate(self, 'assetIds')

    @property
    def source(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'source')

    @property
    def type(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'type')

    @property
    def subtype(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'subtype')

class EventList(CogniteResourceList):
    _RESOURCE = Event
