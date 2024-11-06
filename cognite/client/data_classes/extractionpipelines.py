from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteListUpdate,
    CogniteObject,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class ExtractionPipelineContact(CogniteObject):
    """A contact for an extraction pipeline

    Args:
        name (str): Name of contact
        email (str): Email address of contact
        role (str): Role of contact, such as Owner, Maintainer, etc.
        send_notification (bool): Whether to send notifications to this contact or not
    """

    def __init__(self, name: str, email: str, role: str, send_notification: bool) -> None:
        self.name = name
        self.email = email
        self.role = role
        self.send_notification = send_notification

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ExtractionPipelineContact:
        return cls(
            name=resource["name"],
            email=resource["email"],
            role=resource["role"],
            send_notification=resource["sendNotification"],
        )


@dataclass
class ExtractionPipelineNotificationConfiguration(CogniteObject):
    """Extraction pipeline notification configuration

    Args:
        allowed_not_seen_range_in_minutes (int | None): Time in minutes to pass without any Run. Null if extraction pipeline is not checked.

    """

    allowed_not_seen_range_in_minutes: int | None = None


class ExtractionPipelineCore(WriteableCogniteResource["ExtractionPipelineWrite"], ABC):
    """An extraction pipeline is a representation of a process writing data to CDF, such as an extractor or an ETL tool.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the extraction pipeline.
        description (str | None): The description of the extraction pipeline.
        data_set_id (int | None): The id of the dataset this extraction pipeline related with.
        raw_tables (list[dict[str, str]] | None): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        schedule (str | None): One of None/On trigger/Continuous/cron regex.
        contacts (list[ExtractionPipelineContact] | None): list of contacts
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str | None): Source text value for extraction pipeline.
        documentation (str | None): Documentation text value for extraction pipeline.
        notification_config (ExtractionPipelineNotificationConfiguration | None): Notification configuration for the extraction pipeline.
        created_by (str | None): Extraction pipeline creator, usually an email.

    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
        raw_tables: list[dict[str, str]] | None = None,
        schedule: str | None = None,
        contacts: list[ExtractionPipelineContact] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        documentation: str | None = None,
        notification_config: ExtractionPipelineNotificationConfiguration | None = None,
        created_by: str | None = None,
    ) -> None:
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
        self.notification_config = notification_config
        self.created_by = created_by

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.contacts:
            result["contacts"] = [contact.dump(camel_case) for contact in self.contacts]
        if self.notification_config:
            result["notificationConfig"] = self.notification_config.dump(camel_case)
        return result


class ExtractionPipeline(ExtractionPipelineCore):
    """An extraction pipeline is a representation of a process writing data to CDF, such as an extractor or an ETL tool.
    This is the reading version of the ExtractionPipeline class, which is used when retrieving extraction pipelines.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the extraction pipeline.
        description (str | None): The description of the extraction pipeline.
        data_set_id (int | None): The id of the dataset this extraction pipeline related with.
        raw_tables (list[dict[str, str]] | None): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        last_success (int | None): Milliseconds value of last success status.
        last_failure (int | None): Milliseconds value of last failure status.
        last_message (str | None): Message of last failure.
        last_seen (int | None): Milliseconds value of last seen status.
        schedule (str | None): One of None/On trigger/Continuous/cron regex.
        contacts (list[ExtractionPipelineContact] | None): list of contacts
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str | None): Source text value for extraction pipeline.
        documentation (str | None): Documentation text value for extraction pipeline.
        notification_config (ExtractionPipelineNotificationConfiguration | None): Notification configuration for the extraction pipeline.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_by (str | None): Extraction pipeline creator, usually an email.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
        raw_tables: list[dict[str, str]] | None = None,
        last_success: int | None = None,
        last_failure: int | None = None,
        last_message: str | None = None,
        last_seen: int | None = None,
        schedule: str | None = None,
        contacts: list[ExtractionPipelineContact] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        documentation: str | None = None,
        notification_config: ExtractionPipelineNotificationConfiguration | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        created_by: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            data_set_id=data_set_id,
            raw_tables=raw_tables,
            schedule=schedule,
            contacts=contacts,
            metadata=metadata,
            source=source,
            documentation=documentation,
            notification_config=notification_config,
            created_by=created_by,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self.last_success = last_success
        self.last_failure = last_failure
        self.last_message = last_message
        self.last_seen = last_seen
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> ExtractionPipelineWrite:
        """Returns this ExtractionPipeline as a ExtractionPipelineWrite"""
        if self.external_id is None or self.name is None or self.data_set_id is None:
            raise ValueError("external_id, name and data_set_id are required to create a ExtractionPipeline")
        return ExtractionPipelineWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            data_set_id=self.data_set_id,
            raw_tables=self.raw_tables,
            schedule=self.schedule,
            contacts=self.contacts,
            metadata=self.metadata,
            source=self.source,
            documentation=self.documentation,
            notification_config=self.notification_config,
            created_by=self.created_by,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ExtractionPipeline:
        instance = super()._load(resource, cognite_client)
        if instance.contacts:
            instance.contacts = [
                ExtractionPipelineContact._load(contact) if isinstance(contact, dict) else contact
                for contact in instance.contacts
            ]
        if instance.notification_config and isinstance(instance.notification_config, dict):
            instance.notification_config = ExtractionPipelineNotificationConfiguration._load(
                instance.notification_config
            )
        return instance

    def __hash__(self) -> int:
        return hash(self.external_id)


class ExtractionPipelineWrite(ExtractionPipelineCore):
    """An extraction pipeline is a representation of a process writing data to CDF, such as an extractor or an ETL tool.
    This is the writing version of the ExtractionPipeline class, which is used when creating extraction pipelines.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the extraction pipeline.
        data_set_id (int): The id of the dataset this extraction pipeline related with.
        description (str | None): The description of the extraction pipeline.
        raw_tables (list[dict[str, str]] | None): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        schedule (str | None): One of None/On trigger/Continuous/cron regex.
        contacts (list[ExtractionPipelineContact] | None): list of contacts
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str | None): Source text value for extraction pipeline.
        documentation (str | None): Documentation text value for extraction pipeline.
        notification_config (ExtractionPipelineNotificationConfiguration | None): Notification configuration for the extraction pipeline.
        created_by (str | None): Extraction pipeline creator, usually an email.
    """

    def __init__(
        self,
        external_id: str,
        name: str,
        data_set_id: int,
        description: str | None = None,
        raw_tables: list[dict[str, str]] | None = None,
        schedule: str | None = None,
        contacts: list[ExtractionPipelineContact] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        documentation: str | None = None,
        notification_config: ExtractionPipelineNotificationConfiguration | None = None,
        created_by: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            data_set_id=data_set_id,
            raw_tables=raw_tables,
            schedule=schedule,
            contacts=contacts,
            metadata=metadata,
            source=source,
            documentation=documentation,
            notification_config=notification_config,
            created_by=created_by,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ExtractionPipelineWrite:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource.get("description"),
            data_set_id=resource["dataSetId"],
            raw_tables=resource.get("rawTables"),
            schedule=resource.get("schedule"),
            contacts=[ExtractionPipelineContact._load(contact) for contact in resource.get("contacts") or []] or None,
            metadata=resource.get("metadata"),
            source=resource.get("source"),
            documentation=resource.get("documentation"),
            notification_config=ExtractionPipelineNotificationConfiguration._load(resource["notificationConfig"])
            if "notificationConfig" in resource
            else None,
            created_by=resource.get("createdBy"),
        )

    def as_write(self) -> ExtractionPipelineWrite:
        """Returns this ExtractionPipelineWrite instance."""
        return self


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
        def set(self, value: dict) -> ExtractionPipelineUpdate:
            return self._set(value)

        def add(self, value: dict) -> ExtractionPipelineUpdate:
            return self._add(value)

        def remove(self, value: list) -> ExtractionPipelineUpdate:
            return self._remove(value)

    class _ListExtractionPipelineUpdate(CogniteListUpdate):
        def set(self, value: list) -> ExtractionPipelineUpdate:
            return self._set(value)

        def add(self, value: list) -> ExtractionPipelineUpdate:
            return self._add(value)

        def remove(self, value: list) -> ExtractionPipelineUpdate:
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
    def notification_config(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "notificationConfig")

    @property
    def schedule(self) -> _PrimitiveExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._PrimitiveExtractionPipelineUpdate(self, "schedule")

    @property
    def contacts(self) -> _ListExtractionPipelineUpdate:
        return ExtractionPipelineUpdate._ListExtractionPipelineUpdate(self, "contacts")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("data_set_id", is_nullable=False),
            PropertySpec("schedule", is_nullable=False),
            PropertySpec("raw_tables", is_list=True),
            PropertySpec("contacts", is_list=True),
            PropertySpec("metadata", is_object=True),
            PropertySpec("source", is_nullable=False),
            PropertySpec("documentation", is_nullable=False),
            PropertySpec("notification_config", is_nullable=False),
        ]


class ExtractionPipelineWriteList(CogniteResourceList[ExtractionPipelineWrite], ExternalIDTransformerMixin):
    _RESOURCE = ExtractionPipelineWrite


class ExtractionPipelineList(
    WriteableCogniteResourceList[ExtractionPipelineWrite, ExtractionPipeline], IdTransformerMixin
):
    _RESOURCE = ExtractionPipeline

    def as_write(self) -> ExtractionPipelineWriteList:
        return ExtractionPipelineWriteList([x.as_write() for x in self.data], cognite_client=self._get_cognite_client())


class ExtractionPipelineRunCore(WriteableCogniteResource["ExtractionPipelineRunWrite"], ABC):
    """A representation of an extraction pipeline run.

    Args:
        status (str | None): success/failure/seen.
        message (str | None): Optional status message.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        status: str | None = None,
        message: str | None = None,
        created_time: int | None = None,
    ) -> None:
        self.status = status
        self.message = message
        self.created_time = created_time


class ExtractionPipelineRun(ExtractionPipelineRunCore):
    """A representation of an extraction pipeline run.

    Args:
        extpipe_external_id (str | None): The external ID of the extraction pipeline.
        status (str | None): success/failure/seen.
        message (str | None): Optional status message.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
        id (int | None): A server-generated ID for the object.
    """

    def __init__(
        self,
        extpipe_external_id: str | None = None,
        status: str | None = None,
        message: str | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
        id: int | None = None,
    ) -> None:
        super().__init__(
            status=status,
            message=message,
            created_time=created_time,
        )
        self.id = id
        self.extpipe_external_id = extpipe_external_id
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> ExtractionPipelineRunWrite:
        """Returns this ExtractionPipelineRun as a ExtractionPipelineRunWrite"""
        if self.extpipe_external_id is None:
            raise ValueError("extpipe_external_id is required to create a ExtractionPipelineRun")
        return ExtractionPipelineRunWrite(
            extpipe_external_id=self.extpipe_external_id,
            status=cast(
                Literal["success", "failure", "seen"],
                self.status,
            ),
            message=self.message,
            created_time=self.created_time,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ExtractionPipelineRun:
        obj = super()._load(resource, cognite_client)
        # Note: The API ONLY returns IDs, but if they chose to change this, we're ready:
        if isinstance(resource, dict):
            obj.extpipe_external_id = resource.get("externalId")
        return obj

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dct = super().dump(camel_case=camel_case)
        # Note: No way to make this id/xid API mixup completely correct. Either:
        # 1. We use id / external_id for respectively "self id" / "ext.pipe external id"
        #   - Problem: Only dataclass in the SDK where id and external_id does not point to same object...
        # 2. We rename external_id to extpipe_external_id in the SDK only
        #   - Problem: This dump method might be surprising to the user - if used (its public)...
        # ...and 2 was chosen:
        if camel_case:
            dct["externalId"] = dct.pop("extpipeExternalId", None)
        else:
            dct["external_id"] = dct.pop("extpipe_external_id", None)
        return dct


class ExtractionPipelineRunWrite(ExtractionPipelineRunCore):
    """A representation of an extraction pipeline run.
    This is the writing version of the ExtractionPipelineRun class, which is used when creating extraction pipeline runs.

    Args:
        extpipe_external_id (str): The external ID of the extraction pipeline.
        status (Literal['success', 'failure', 'seen']): success/failure/seen.
        message (str | None): Optional status message.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        extpipe_external_id: str,
        status: Literal["success", "failure", "seen"],
        message: str | None = None,
        created_time: int | None = None,
    ) -> None:
        super().__init__(
            status=status,
            message=message,
            created_time=created_time,
        )
        self.extpipe_external_id = extpipe_external_id

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ExtractionPipelineRunWrite:
        return cls(
            extpipe_external_id=resource["externalId"],
            status=resource["status"],
            message=resource.get("message"),
            created_time=resource.get("createdTime"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dct = super().dump(camel_case=camel_case)
        # Note: No way to make this id/xid API mixup completely correct. Either:
        # 1. We use id / external_id for respectively "self id" / "ext.pipe external id"
        #   - Problem: Only dataclass in the SDK where id and external_id does not point to same object...
        # 2. We rename external_id to extpipe_external_id in the SDK only
        #   - Problem: This dump method might be surprising to the user - if used (its public)...
        # ...and 2 was chosen:
        if camel_case:
            dct["externalId"] = dct.pop("extpipeExternalId", None)
        else:
            dct["external_id"] = dct.pop("extpipe_external_id", None)
        return dct

    def as_write(self) -> ExtractionPipelineRunWrite:
        """Returns this ExtractionPipelineRunWrite instance."""
        return self


class ExtractionPipelineRunWriteList(CogniteResourceList[ExtractionPipelineRunWrite]):
    _RESOURCE = ExtractionPipelineRunWrite


class ExtractionPipelineRunList(WriteableCogniteResourceList[ExtractionPipelineRunWrite, ExtractionPipelineRun]):
    _RESOURCE = ExtractionPipelineRun

    def as_write(self) -> ExtractionPipelineRunWriteList:
        return ExtractionPipelineRunWriteList(
            [x.as_write() for x in self.data], cognite_client=self._get_cognite_client()
        )


class StringFilter(CogniteFilter):
    """Filter runs on substrings of the message

    Args:
        substring (str | None): Part of message
    """

    def __init__(self, substring: str | None = None) -> None:
        self.substring = substring


class ExtractionPipelineRunFilter(CogniteFilter):
    """Filter runs with exact matching

    Args:
        external_id (str | None): The external ID of related ExtractionPipeline provided by the client. Must be unique for the resource type.
        statuses (SequenceNotStr[str] | None): success/failure/seen.
        message (StringFilter | None): message filter.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
    """

    def __init__(
        self,
        external_id: str | None = None,
        statuses: SequenceNotStr[str] | None = None,
        message: StringFilter | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
    ) -> None:
        self.external_id = external_id
        self.statuses = statuses
        self.message = message
        self.created_time = created_time


class ExtractionPipelineConfigRevision(CogniteResource):
    """An extraction pipeline config revision

    Args:
        external_id (str | None): The external ID of the associated extraction pipeline.
        revision (int | None): The revision number of this config as a positive integer.
        description (str | None): Short description of this configuration revision.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        revision: int | None = None,
        description: str | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.external_id = external_id
        self.revision = revision
        self.description = description
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)


class ExtractionPipelineConfigCore(WriteableCogniteResource["ExtractionPipelineConfigWrite"], ABC):
    """An extraction pipeline config

    Args:
        external_id (str | None): The external ID of the associated extraction pipeline.
        config (str | None): Contents of this configuration revision.
        description (str | None): Short description of this configuration revision.
    """

    def __init__(
        self,
        external_id: str | None = None,
        config: str | None = None,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.config = config
        self.description = description


class ExtractionPipelineConfig(ExtractionPipelineConfigCore):
    """An extraction pipeline config

    Args:
        external_id (str | None): The external ID of the associated extraction pipeline.
        config (str | None): Contents of this configuration revision.
        revision (int | None): The revision number of this config as a positive integer.
        description (str | None): Short description of this configuration revision.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        config: str | None = None,
        revision: int | None = None,
        description: str | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            config=config,
            description=description,
        )
        self.revision = revision
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> ExtractionPipelineConfigWrite:
        """Returns this ExtractionPipelineConfig as a ExtractionPipelineConfigWrite"""
        if self.external_id is None:
            raise ValueError("external_id is required to create a ExtractionPipelineConfig")
        return ExtractionPipelineConfigWrite(
            external_id=self.external_id,
            config=self.config,
            description=self.description,
        )


class ExtractionPipelineConfigWrite(ExtractionPipelineConfigCore):
    """An extraction pipeline config

    Args:
        external_id (str): The external ID of the associated extraction pipeline.
        config (str | None): Contents of this configuration revision.
        description (str | None): Short description of this configuration revision.
    """

    def __init__(
        self,
        external_id: str,
        config: str | None = None,
        description: str | None = None,
    ) -> None:
        super().__init__(external_id, config, description)

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> ExtractionPipelineConfigWrite:
        return cls(
            external_id=resource["externalId"],
            config=resource.get("config"),
            description=resource.get("description"),
        )

    def as_write(self) -> ExtractionPipelineConfigWrite:
        """Returns this ExtractionPipelineConfigWrite instance."""
        return self


class ExtractionPipelineConfigRevisionList(CogniteResourceList[ExtractionPipelineConfigRevision]):
    _RESOURCE = ExtractionPipelineConfigRevision


class ExtractionPipelineConfigWriteList(CogniteResourceList[ExtractionPipelineConfigWrite]):
    _RESOURCE = ExtractionPipelineConfigWrite


class ExtractionPipelineConfigList(
    WriteableCogniteResourceList[ExtractionPipelineConfigWrite, ExtractionPipelineConfig]
):
    _RESOURCE = ExtractionPipelineConfig

    def as_write(self) -> ExtractionPipelineConfigWriteList:
        return ExtractionPipelineConfigWriteList(
            [x.as_write() for x in self.data], cognite_client=self._get_cognite_client()
        )
