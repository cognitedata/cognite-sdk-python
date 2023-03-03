
from cognite.client.data_classes._base import CogniteFilter, CogniteLabelUpdate, CogniteListUpdate, CogniteObjectUpdate, CognitePrimitiveUpdate, CognitePropertyClassUtil, CogniteResource, CogniteResourceList, CogniteUpdate
from cognite.client.data_classes.shared import TimestampRange
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class DataSet(CogniteResource):

    def __init__(self, external_id=None, name=None, description=None, metadata=None, write_protected=None, id=None, created_time=None, last_updated_time=None, cognite_client=None):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.metadata = metadata
        self.write_protected = write_protected
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast('CogniteClient', cognite_client)

class DataSetFilter(CogniteFilter):

    def __init__(self, metadata=None, created_time=None, last_updated_time=None, external_id_prefix=None, write_protected=None, cognite_client=None):
        self.metadata = metadata
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self.write_protected = write_protected
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

class DataSetUpdate(CogniteUpdate):

    class _PrimitiveDataSetUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectDataSetUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListDataSetUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelDataSetUpdate(CogniteLabelUpdate):

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def external_id(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, 'externalId')

    @property
    def name(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, 'name')

    @property
    def description(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, 'description')

    @property
    def metadata(self):
        return DataSetUpdate._ObjectDataSetUpdate(self, 'metadata')

    @property
    def write_protected(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, 'writeProtected')

class DataSetAggregate(dict):

    def __init__(self, count=None, **kwargs: Any):
        self.count = count
        self.update(kwargs)
    count = CognitePropertyClassUtil.declare_property('count')

class DataSetList(CogniteResourceList):
    _RESOURCE = DataSet
