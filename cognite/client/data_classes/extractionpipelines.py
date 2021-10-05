from typing import Any, Dict, List, Union

from cognite.client.data_classes._base import *
from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.shared import TimestampRange


class ExtractionPipelineContact(dict):
    """A contact for an extraction pipeline

    Args:
        name (str): Name of contact
        email (str): Email address of contact
        role (str): Role of contact, such as Owner, Maintainer, etc.
        send_notification (bool): Whether to send notifications to this contact or not
    """

    def __init__(self, name: str, email: str, role: str, send_notification: bool):
        self.name = name
        self.email = email
        self.role = role
        self.send_notification = send_notification

    name = CognitePropertyClassUtil.declare_property("name")
    email = CognitePropertyClassUtil.declare_property("email")
    role = CognitePropertyClassUtil.declare_property("role")
    send_notification = CognitePropertyClassUtil.declare_property("sendNotification")

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class ExtractionPipeline(CogniteResource):
    """An extraction pipeline is a representation of a process writing data to CDF, such as an extractor or an ETL tool.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the extraction pipepeline.
        description (str): The description of the extraction pipepeline.
        data_set_id (int): The id of the dataset this extraction pipepeline related with.
        raw_tables (List[Dict[str, str]): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        last_success (int): Milliseconds value of last success status.
        last_failure (int): Milliseconds value of last failure status.
        last_message (str): Message of last failure.
        last_seen (int): Milliseconds value of last seen status.
        schedule (str): None/On trigger/Continuous/cron regex.
        contacts (List[ExtractionPipelineContact]): list of contacts
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str): Source text value for extraction pipepeline.
        documentation (str): Documentation text value for extraction pipepeline.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_by (str): Extraction pipepeline creator, usually an email.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        name: str = None,
        description: str = None,
        data_set_id: int = None,
        raw_tables: List[Dict[str, str]] = None,
        last_success: int = None,
        last_failure: int = None,
        last_message: str = None,
        last_seen: int = None,
        schedule: str = None,
        contacts: List[ExtractionPipelineContact] = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        documentation: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        created_by: str = None,
        cognite_client=None,
    ):
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
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(ExtractionPipeline, cls)._load(resource, cognite_client)
        return instance

    def __hash__(self):
        return hash(self.external_id)


class ExtractionPipelineUpdate(CogniteUpdate):
    """Changes applied to an extraction pipeline

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveExtractionPipelineUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "ExtractionPipelineUpdate":
            return self._set(value)

    class _ObjectExtractionPipelineUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "ExtractionPipelineUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "ExtractionPipelineUpdate":
            return self._add(value)

        def remove(self, value: List) -> "ExtractionPipelineUpdate":
            return self._remove(value)

    class _ListExtractionPipelineUpdate(CogniteListUpdate):
        def set(self, value: List) -> "ExtractionPipelineUpdate":
            return self._set(value)

        def add(self, value: List) -> "ExtractionPipelineUpdate":
            return self._add(value)

        def remove(self, value: List) -> "ExtractionPipelineUpdate":
            return self._remove(value)

    @property
    def external_id(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "externalId")

    @property
    def name(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "name")

    @property
    def description(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "description")

    @property
    def data_set_id(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "dataSetId")

    @property
    def raw_tables(self):
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, "rawTables")

    @property
    def metadata(self):
        return ExtractionPipelineUpdate._ObjectExtractionPipelineUpdate(self, "metadata")

    @property
    def source(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "source")

    @property
    def documentation(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "documentation")

    @property
    def schedule(self):
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "schedule")

    @property
    def contacts(self):
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, "contacts")


class ExtractionPipelineList(CogniteResourceList):
    _RESOURCE = ExtractionPipeline
    _UPDATE = ExtractionPipelineUpdate


class ExtractionPipelineRun(CogniteResource):
    """A representation of an extraction pipeline run.

    Args:
        external_id (str): The external ID of the extraction pipeline.
        status (str): success/failure/seen.
        message (str): Optional status message.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        status: str = None,
        message: str = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.status = status
        self.message = message
        self.created_time = created_time
        self._cognite_client = cognite_client


class ExtractionPipelineRunUpdate(CogniteUpdate):
    class _PrimitiveExtractionPipelineRunUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "ExtractionPipelineRunUpdate":
            return self._set(value)


class ExtractionPipelineRunList(CogniteResourceList):
    _RESOURCE = ExtractionPipelineRun
    _UPDATE = ExtractionPipelineRunUpdate


class StringFilter(CogniteFilter):
    """Filter runs on substrings of the message

    Args:
        substring (str): Part of message
    """

    def __init__(self, substring: str = None):
        self.substring = substring


class ExtractionPipelineRunFilter(CogniteFilter):
    """Filter runs with exact matching

    Args:
        external_id (str): The external ID of related ExtractionPipeline provided by the client. Must be unique for the resource type.
        statuses (List[str]): success/failure/seen.
        message (StringFilter): message filter.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        statuses: List[str] = None,
        message: StringFilter = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.statuses = statuses
        self.message = message
        self.created_time = created_time
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(ExtractionPipelineRunFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
        return instance
