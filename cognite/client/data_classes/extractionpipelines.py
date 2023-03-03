
from cognite.client.data_classes._base import CogniteFilter, CogniteListUpdate, CogniteObjectUpdate, CognitePrimitiveUpdate, CognitePropertyClassUtil, CogniteResource, CogniteResourceList, CogniteUpdate
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class ExtractionPipelineContact(dict):

    def __init__(self, name, email, role, send_notification):
        self.name = name
        self.email = email
        self.role = role
        self.send_notification = send_notification
    name = CognitePropertyClassUtil.declare_property('name')
    email = CognitePropertyClassUtil.declare_property('email')
    role = CognitePropertyClassUtil.declare_property('role')
    send_notification = CognitePropertyClassUtil.declare_property('sendNotification')

    def dump(self, camel_case=False):
        return (convert_all_keys_to_camel_case(self) if camel_case else dict(self))

class ExtractionPipeline(CogniteResource):

    def __init__(self, id=None, external_id=None, name=None, description=None, data_set_id=None, raw_tables=None, last_success=None, last_failure=None, last_message=None, last_seen=None, schedule=None, contacts=None, metadata=None, source=None, documentation=None, created_time=None, last_updated_time=None, created_by=None, cognite_client=None):
        self.id = id
        self.external_id = external_id
        self.name = name
        self.description = description
        self.data_set_id = data_set_id
        self.raw_tables = raw_tables
        self.schedule = schedule
        self.contacts = contacts
        self.metadata = metadata
        self.source = source
        self.documentation = documentation
        self.last_success = last_success
        self.last_failure = last_failure
        self.last_message = last_message
        self.last_seen = last_seen
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.created_by = created_by
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        return instance

    def __hash__(self):
        return hash(self.external_id)

class ExtractionPipelineUpdate(CogniteUpdate):

    class _PrimitiveExtractionPipelineUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectExtractionPipelineUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListExtractionPipelineUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def external_id(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'externalId')

    @property
    def name(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'name')

    @property
    def description(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'description')

    @property
    def data_set_id(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'dataSetId')

    @property
    def raw_tables(self):
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, 'rawTables')

    @property
    def metadata(self):
        return ExtractionPipelineUpdate._ObjectExtractionPipelineUpdate(self, 'metadata')

    @property
    def source(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'source')

    @property
    def documentation(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'documentation')

    @property
    def schedule(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, 'schedule')

    @property
    def contacts(self):
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, 'contacts')

class ExtractionPipelineList(CogniteResourceList):
    _RESOURCE = ExtractionPipeline

class ExtractionPipelineRun(CogniteResource):

    def __init__(self, external_id=None, status=None, message=None, created_time=None, cognite_client=None):
        self.external_id = external_id
        self.status = status
        self.message = message
        self.created_time = created_time
        self._cognite_client = cast('CogniteClient', cognite_client)

class ExtractionPipelineRunUpdate(CogniteUpdate):

    class _PrimitiveExtractionPipelineRunUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

class ExtractionPipelineRunList(CogniteResourceList):
    _RESOURCE = ExtractionPipelineRun

class StringFilter(CogniteFilter):

    def __init__(self, substring=None):
        self.substring = substring

class ExtractionPipelineRunFilter(CogniteFilter):

    def __init__(self, external_id=None, statuses=None, message=None, created_time=None, cognite_client=None):
        self.external_id = external_id
        self.statuses = statuses
        self.message = message
        self.created_time = created_time
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if (instance.created_time is not None):
                instance.created_time = TimestampRange(**instance.created_time)
        return instance

class ExtractionPipelineConfigRevision(CogniteResource):

    def __init__(self, external_id=None, revision=None, description=None, created_time=None, cognite_client=None):
        self.external_id = external_id
        self.revision = revision
        self.description = description
        self.created_time = created_time
        self._cognite_client = cognite_client

class ExtractionPipelineConfig(ExtractionPipelineConfigRevision):

    def __init__(self, external_id=None, config=None, revision=None, description=None, created_time=None, cognite_client=None):
        super().__init__(external_id=external_id, revision=revision, description=description, created_time=created_time, cognite_client=cognite_client)
        self.config = config

class ExtractionPipelineConfigRevisionList(CogniteResourceList):
    _RESOURCE = ExtractionPipelineConfigRevision
