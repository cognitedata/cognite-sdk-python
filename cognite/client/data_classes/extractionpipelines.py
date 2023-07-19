from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class ExtractionPipeline(CogniteResource):
    """An extraction pipeline is a representation of a process writing data to CDF, such as an extractor or an ETL tool.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the extraction pipeline.
        description (str): The description of the extraction pipeline.
        data_set_id (int): The id of the dataset this extraction pipeline related with.
        raw_tables (List[Dict[str, str]): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        last_success (int): Milliseconds value of last success status.
        last_failure (int): Milliseconds value of last failure status.
        last_message (str): Message of last failure.
        last_seen (int): Milliseconds value of last seen status.
        schedule (str): None/On trigger/Continuous/cron regex.
        contacts (List[ExtractionPipelineContact]): list of contacts
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str): Source text value for extraction pipeline.
        documentation (str): Documentation text value for extraction pipeline.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_by (str): Extraction pipeline creator, usually an email.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: Optional[int] = None,
        external_id: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        data_set_id: Optional[int] = None,
        raw_tables: Optional[List[Dict[str, str]]] = None,
        last_success: Optional[int] = None,
        last_failure: Optional[int] = None,
        last_message: Optional[str] = None,
        last_seen: Optional[int] = None,
        schedule: Optional[str] = None,
        contacts: Optional[List[ExtractionPipelineContact]] = None,
        metadata: Optional[Dict[str, str]] = None,
        source: Optional[str] = None,
        documentation: Optional[str] = None,
        created_time: Optional[int] = None,
        last_updated_time: Optional[int] = None,
        created_by: Optional[str] = None,
        cognite_client: Optional[CogniteClient] = None,
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
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: Optional[CogniteClient] = None) -> ExtractionPipeline:
        instance = super()._load(resource, cognite_client)
        return instance

    def __hash__(self) -> int:
        return hash(self.external_id)


class ExtractionPipelineUpdate(CogniteUpdate):
    """Changes applied to an extraction pipeline

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveExtractionPipelineUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> ExtractionPipelineUpdate:
            return self._set(value)

    class _ObjectExtractionPipelineUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> ExtractionPipelineUpdate:
            return self._set(value)

        def add(self, value: Dict) -> ExtractionPipelineUpdate:
            return self._add(value)

        def remove(self, value: List) -> ExtractionPipelineUpdate:
            return self._remove(value)

    class _ListExtractionPipelineUpdate(CogniteListUpdate):
        def set(self, value: List) -> ExtractionPipelineUpdate:
            return self._set(value)

        def add(self, value: List) -> ExtractionPipelineUpdate:
            return self._add(value)

        def remove(self, value: List) -> ExtractionPipelineUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "description")

    @property
    def data_set_id(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "dataSetId")

    @property
    def raw_tables(self) -> _ListExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, "rawTables")

    @property
    def metadata(self) -> _ObjectExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._ObjectExtractionPipelineUpdate(self, "metadata")

    @property
    def source(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "source")

    @property
    def documentation(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "documentation")

    @property
    def schedule(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "schedule")

    @property
    def contacts(self) -> _ListExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, "contacts")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("data_set_id", is_nullable=False),
            PropertySpec("schedule", is_nullable=False),
            PropertySpec("raw_tables", is_container=True),
            PropertySpec("contacts", is_container=True),
            PropertySpec("metadata", is_container=True),
            PropertySpec("source", is_nullable=False),
            PropertySpec("documentation", is_nullable=False),
            # Not supported yet
            # PropertySpec("notification_config", is_nullable=False),
        ]


class ExtractionPipelineList(CogniteResourceList[ExtractionPipeline]):
    _RESOURCE = ExtractionPipeline


class ExtractionPipelineRun(CogniteResource):
    """A representation of an extraction pipeline run.

    Args:
        id (int): A server-generated ID for the object.
        extpipe_external_id (str): The external ID of the extraction pipeline.
        status (str): success/failure/seen.
        message (str): Optional status message.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        extpipe_external_id: Optional[str] = None,
        status: Optional[str] = None,
        message: Optional[str] = None,
        created_time: Optional[int] = None,
        cognite_client: Optional[CogniteClient] = None,
        id: Optional[int] = None,
    ):
        self.id = id
        self.extpipe_external_id = extpipe_external_id
        self.status = status
        self.message = message
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: Optional[CogniteClient] = None) -> ExtractionPipelineRun:
        obj = super()._load(resource, cognite_client)
        # Note: The API ONLY returns IDs, but if they chose to change this, we're ready:
        if isinstance(resource, dict):
            obj.extpipe_external_id = resource.get("externalId")
        return obj

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dct = super().dump(camel_case=camel_case)
        # Note: No way to make this id/xid API mixup completely correct. Either:
        # 1. We use id / external_id for respecively "self id" / "ext.pipe external id"
        #   - Problem: Only dataclass in the SDK where id and external_id does not point to same object...
        # 2. We rename external_id to extpipe_external_id in the SDK only
        #   - Problem: This dump method might be surprising to the user - if used (its public)...
        # ...and 2 was chosen:
        if camel_case:
            dct["externalId"] = dct.pop("extpipeExternalId", None)
        else:
            dct["external_id"] = dct.pop("extpipe_external_id", None)
        return dct


class ExtractionPipelineRunList(CogniteResourceList[ExtractionPipelineRun]):
    _RESOURCE = ExtractionPipelineRun


class StringFilter(CogniteFilter):
    """Filter runs on substrings of the message

    Args:
        substring (str): Part of message
    """

    def __init__(self, substring: Optional[str] = None):
        self.substring = substring


class ExtractionPipelineRunFilter(CogniteFilter):
    """Filter runs with exact matching

    Args:
        external_id (str): The external ID of related ExtractionPipeline provided by the client. Must be unique for the resource type.
        statuses (Sequence[str]): success/failure/seen.
        message (StringFilter): message filter.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: Optional[str] = None,
        statuses: Optional[Sequence[str]] = None,
        message: Optional[StringFilter] = None,
        created_time: Optional[Union[Dict[str, Any], TimestampRange]] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
        self.external_id = external_id
        self.statuses = statuses
        self.message = message
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str]) -> ExtractionPipelineRunFilter:
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
        return instance


class ExtractionPipelineConfigRevision(CogniteResource):
    """An extraction pipeline config revision

    Args:
        external_id (str): The external ID of the associated extraction pipeline.
        revision (int): The revision number of this config as a positive integer.
        description (str): Short description of this configuration revision.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: Optional[str] = None,
        revision: Optional[int] = None,
        description: Optional[str] = None,
        created_time: Optional[int] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
        self.external_id = external_id
        self.revision = revision
        self.description = description
        self.created_time = created_time
        self._cognite_client = cognite_client


class ExtractionPipelineConfig(ExtractionPipelineConfigRevision):
    """An extraction pipeline config

    Args:
        external_id (str): The external ID of the associated extraction pipeline.
        config (str): Contents of this configuration revision.
        revision (int): The revision number of this config as a positive integer.
        description (str): Short description of this configuration revision.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: Optional[str] = None,
        config: Optional[str] = None,
        revision: Optional[int] = None,
        description: Optional[str] = None,
        created_time: Optional[int] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
        super().__init__(
            external_id=external_id,
            revision=revision,
            description=description,
            created_time=created_time,
            cognite_client=cognite_client,
        )
        self.config = config


class ExtractionPipelineConfigRevisionList(CogniteResourceList[ExtractionPipelineConfigRevision]):
    _RESOURCE = ExtractionPipelineConfigRevision
