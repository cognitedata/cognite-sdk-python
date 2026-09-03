from __future__ import annotations

from abc import ABC
from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

ActiveConfigRevision: TypeAlias = int | Literal["local"]


class Extractor(CogniteResource):
    """The extractor (or other process) that reports as this integration.

    Args:
        external_id (str): External id of the extractor, e.g. "cognite-simple-influxdb-extractor" for a Cognite-built extractor.
        version (str | None): The version of the extractor.
    """

    def __init__(self, external_id: str, version: str | None = None) -> None:
        self.external_id = external_id
        self.version = version

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(external_id=resource["externalId"], version=resource.get("version"))


class Task(CogniteResource):
    """A named unit of work in an integration, reported by the extractor.

    Args:
        type (Literal['continuous', 'batch']): Whether the task runs for the lifetime of the extractor (continuous) or runs to completion and exits (batch).
        name (str): Name of the task, unique within the integration.
        action (bool): Whether this task can be triggered through an Action. Defaults to False.
        description (str | None): Description of the task.
        sources (list[str] | None): Lineage: URIs of the systems/resources this task reads from.
        targets (list[str] | None): Lineage: URIs of the CDF (or other) resources this task writes to.
    """

    def __init__(
        self,
        type: Literal["continuous", "batch"],
        name: str,
        action: bool = False,
        description: str | None = None,
        sources: list[str] | None = None,
        targets: list[str] | None = None,
    ) -> None:
        self.type = type
        self.name = name
        self.action = action
        self.description = description
        self.sources = sources
        self.targets = targets

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            type=resource["type"],
            name=resource["name"],
            action=resource.get("action", False),
            description=resource.get("description"),
            sources=resource.get("sources"),
            targets=resource.get("targets"),
        )


class IntegrationCore(WriteableCogniteResource["IntegrationWrite"], ABC):
    """An integration is a record of an extractor or other process that sends data to CDF.

    It identifies the extractor type and holds the configuration, task history, and error history for that data
    pipeline. Note that an integration isn't an extraction pipeline: don't use both for the same external ID.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        extractor (Extractor): The extractor (or other process) that reports as this integration.
        name (str | None): Name of the integration.
        description (str | None): Description of the integration.
        documentation (str | None): Documentation for the integration, formatted as markdown.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
        allowed_not_seen_minutes (int | None): Number of minutes the integration is allowed to not report in before it's flagged as inactive. Defaults to 1440 (1 day) server-side.
    """

    def __init__(
        self,
        external_id: str,
        extractor: Extractor,
        name: str | None = None,
        description: str | None = None,
        documentation: str | None = None,
        metadata: dict[str, str] | None = None,
        allowed_not_seen_minutes: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.extractor = extractor
        self.name = name
        self.description = description
        self.documentation = documentation
        self.metadata = metadata
        self.allowed_not_seen_minutes = allowed_not_seen_minutes

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        result["extractor"] = self.extractor.dump(camel_case)
        return result


class IntegrationWrite(IntegrationCore):
    """An integration is a record of an extractor or other process that sends data to CDF.
    This is the write/create format of the integration.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        extractor (Extractor): The extractor (or other process) that reports as this integration.
        name (str | None): Name of the integration.
        description (str | None): Description of the integration.
        documentation (str | None): Documentation for the integration, formatted as markdown.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
        allowed_not_seen_minutes (int | None): Number of minutes the integration is allowed to not report in before it's flagged as inactive. Defaults to 1440 (1 day) server-side.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            extractor=Extractor._load(resource["extractor"]),
            name=resource.get("name"),
            description=resource.get("description"),
            documentation=resource.get("documentation"),
            metadata=resource.get("metadata"),
            allowed_not_seen_minutes=resource.get("allowedNotSeenMinutes"),
        )

    def as_write(self) -> IntegrationWrite:
        return self


class Integration(IntegrationCore):
    """An integration is a record of an extractor or other process that sends data to CDF.
    This is the read/response format of the integration.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        extractor (Extractor): The extractor (or other process) that reports as this integration.
        created_time (int): The time when this integration was created, in milliseconds since epoch.
        last_updated_time (int): The time when this integration was last updated, in milliseconds since epoch.
        name (str | None): Name of the integration.
        description (str | None): Description of the integration.
        documentation (str | None): Documentation for the integration, formatted as markdown.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
        allowed_not_seen_minutes (int | None): Number of minutes the integration is allowed to not report in before it's flagged as inactive.
        last_seen (int | None): The time this integration was last seen (checked in), in milliseconds since epoch.
        last_config_revision (int | None): The revision number of the last config revision created for this integration.
        active_config_revision (ActiveConfigRevision | None): The config revision currently reported active by the extractor, or "local" if it's using a local config file instead of a revision managed through CDF.
        tasks (list[Task] | None): The tasks the extractor has reported as part of this integration.
    """

    def __init__(
        self,
        external_id: str,
        extractor: Extractor,
        created_time: int,
        last_updated_time: int,
        name: str | None = None,
        description: str | None = None,
        documentation: str | None = None,
        metadata: dict[str, str] | None = None,
        allowed_not_seen_minutes: int | None = None,
        last_seen: int | None = None,
        last_config_revision: int | None = None,
        active_config_revision: ActiveConfigRevision | None = None,
        tasks: list[Task] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            extractor=extractor,
            name=name,
            description=description,
            documentation=documentation,
            metadata=metadata,
            allowed_not_seen_minutes=allowed_not_seen_minutes,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.last_seen = last_seen
        self.last_config_revision = last_config_revision
        self.active_config_revision = active_config_revision
        self.tasks = tasks or []

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        result["tasks"] = [task.dump(camel_case) for task in self.tasks]
        return result

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            extractor=Extractor._load(resource["extractor"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            name=resource.get("name"),
            description=resource.get("description"),
            documentation=resource.get("documentation"),
            metadata=resource.get("metadata"),
            allowed_not_seen_minutes=resource.get("allowedNotSeenMinutes"),
            last_seen=resource.get("lastSeen"),
            last_config_revision=resource.get("lastConfigRevision"),
            active_config_revision=resource.get("activeConfigRevision"),
            tasks=[Task._load(task) for task in resource.get("tasks", [])],
        )

    def as_write(self) -> IntegrationWrite:
        """Returns this Integration as an IntegrationWrite"""
        return IntegrationWrite(
            external_id=self.external_id,
            extractor=self.extractor,
            name=self.name,
            description=self.description,
            documentation=self.documentation,
            metadata=self.metadata,
            allowed_not_seen_minutes=self.allowed_not_seen_minutes,
        )

    def __hash__(self) -> int:
        return hash(self.external_id)


class IntegrationWriteList(CogniteResourceList[IntegrationWrite], ExternalIDTransformerMixin):
    _RESOURCE = IntegrationWrite


class IntegrationList(WriteableCogniteResourceList[IntegrationWrite, Integration], ExternalIDTransformerMixin):
    _RESOURCE = Integration

    def as_write(self) -> IntegrationWriteList:
        return IntegrationWriteList([item.as_write() for item in self.data])


class IntegrationUpdate(CogniteUpdate):
    """Changes applied to an integration

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    class _PrimitiveIntegrationUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> IntegrationUpdate:
            return self._set(value)

    class _ObjectIntegrationUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> IntegrationUpdate:
            return self._set(value)

        def add(self, value: dict) -> IntegrationUpdate:
            return self._add(value)

        def remove(self, value: list) -> IntegrationUpdate:
            return self._remove(value)

    @property
    def name(self) -> _PrimitiveIntegrationUpdate:
        return IntegrationUpdate._PrimitiveIntegrationUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveIntegrationUpdate:
        return IntegrationUpdate._PrimitiveIntegrationUpdate(self, "description")

    @property
    def documentation(self) -> _PrimitiveIntegrationUpdate:
        return IntegrationUpdate._PrimitiveIntegrationUpdate(self, "documentation")

    @property
    def allowed_not_seen_minutes(self) -> _PrimitiveIntegrationUpdate:
        return IntegrationUpdate._PrimitiveIntegrationUpdate(self, "allowedNotSeenMinutes")

    @property
    def metadata(self) -> _ObjectIntegrationUpdate:
        return IntegrationUpdate._ObjectIntegrationUpdate(self, "metadata")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name"),
            PropertySpec("description"),
            PropertySpec("documentation"),
            PropertySpec("allowed_not_seen_minutes"),
            PropertySpec("metadata", is_object=True),
        ]
