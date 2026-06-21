from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference, UnitSystemReference
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId

__all__ = [
    "RecordContainerId",
    "RecordId",
    "RecordIdSequence",
    "RecordSource",
    "RecordSourceSelector",
    "RecordTargetUnit",
    "RecordTargetUnits",
    "RecordWrite",
    "RecordWriteList",
    "SyncRecord",
    "SyncRecordList",
]


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)


@dataclass(frozen=True)
class RecordContainerId(ContainerId):
    """Container reference used as a source in a record write.

    Args:
        space (str): Space that contains the container.
        external_id (str): External ID of the container.
    """


class RecordSource(CogniteResource):
    """Container source with property values for a record write.

    Args:
        source (RecordContainerId): Reference to the container.
        properties (Mapping[str, Any]): The data to write to the source container.
    """

    def __init__(self, source: RecordContainerId, properties: Mapping[str, Any]) -> None:
        self.source = source
        self.properties = dict(properties)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordContainerId.load(resource["source"]),
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case),
            "properties": self.properties,
        }


class RecordWrite(WriteableCogniteResource["RecordWrite"]):
    """Write representation of a record, used for ingest and upsert.

    This is the write version of :class:`Record`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record (1-256 chars, no null bytes).
        sources (Sequence[RecordSource]): Container property values to write (1-100 sources).
    """

    def __init__(self, space: str, external_id: str, sources: Sequence[RecordSource]) -> None:
        self.space = space
        self.external_id = external_id
        self.sources = list(sources)

    def as_id(self) -> RecordId:
        return RecordId(space=self.space, external_id=self.external_id)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            sources=[RecordSource._load(s) for s in resource.get("sources", [])],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "sources": [s.dump(camel_case=camel_case) for s in self.sources],
        }

    def as_write(self) -> RecordWrite:
        return self


class RecordWriteList(CogniteResourceList[RecordWrite]):
    """A list of :class:`RecordWrite` objects."""

    _RESOURCE = RecordWrite

    def as_ids(self) -> list[RecordId]:
        return [v.as_id() for v in self]


class RecordSourceSelector(CogniteResource):
    """Selects which container properties to return for a record.

    Args:
        source (RecordContainerId): The container to select properties from.
        properties (Sequence[str]): Property identifiers to return; use ``["*"]`` to return all.
    """

    def __init__(self, source: RecordContainerId, properties: Sequence[str]) -> None:
        self.source = source
        self.properties = list(properties)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(source=RecordContainerId.load(resource["source"]), properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"source": self.source.dump(camel_case=camel_case), "properties": self.properties}


class RecordTargetUnit(CogniteResource):
    """A target unit conversion for one Records container property.

    Args:
        property (Sequence[str]): Fully qualified container property path:
            ``[space, container_external_id, property_id]``.
        unit (UnitReference | UnitSystemReference): Target unit or target unit system.
    """

    def __init__(self, property: Sequence[str], unit: UnitReference | UnitSystemReference) -> None:
        self.property = list(property)
        self.unit = unit

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            property=resource["property"],
            unit=UnitReference.load(resource["unit"])
            if "externalId" in resource["unit"]
            else UnitSystemReference.load(resource["unit"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"property": self.property, "unit": self.unit.dump(camel_case=camel_case)}


class RecordTargetUnits(CogniteResource):
    """Target unit conversions for a Records filter, sync, or aggregate request.

    Args:
        properties (Sequence[RecordTargetUnit] | None): Property-specific target unit conversions.
        unit_system_name (str | None): Convert all convertible properties to a target unit system.
    """

    def __init__(
        self, properties: Sequence[RecordTargetUnit] | None = None, unit_system_name: str | None = None
    ) -> None:
        self.properties = list(properties) if properties is not None else None
        self.unit_system_name = unit_system_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        if "properties" in resource:
            return cls(properties=[RecordTargetUnit._load(item) for item in resource["properties"]])
        if "unitSystemName" in resource:
            return cls(unit_system_name=resource["unitSystemName"])
        return cls()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.unit_system_name is not None:
            return {"unitSystemName" if camel_case else "unit_system_name": self.unit_system_name}
        if self.properties is not None:
            return {
                "properties": [target_unit.dump(camel_case=camel_case) for target_unit in self.properties],
            }
        return {}


class SyncRecord(WriteableCogniteResource["RecordWrite"]):
    """A record returned by the sync endpoint, annotated with a change status.

    For ``status="deleted"`` tombstones (mutable streams), :attr:`properties` is ``None``.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        status (Literal['created', 'updated', 'deleted']): The record's change status.
        properties (Mapping[str, Mapping[str, Mapping[str, Any]]] | None): Property values (absent for
            deleted tombstones).
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        status: Literal["created", "updated", "deleted"],
        properties: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.status = status
        self.properties = (
            {
                space: {container: dict(values) for container, values in containers.items()}
                for space, containers in properties.items()
            }
            if properties is not None
            else None
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            status=resource["status"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "createdTime" if camel_case else "created_time": self.created_time,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
            "status": self.status,
        }
        if self.properties is not None:
            output["properties"] = self.properties
        return output

    def as_id(self) -> RecordId:
        return RecordId(space=self.space, external_id=self.external_id)

    def as_write(self) -> RecordWrite:
        """Reconstruct the :class:`RecordWrite` by grouping read properties back into sources."""
        sources = [
            RecordSource(
                source=RecordContainerId(space=space, external_id=container),
                properties=dict(props),
            )
            for space, containers in (self.properties or {}).items()
            for container, props in containers.items()
        ]
        return RecordWrite(space=self.space, external_id=self.external_id, sources=sources)


class SyncRecordList(CogniteResourceList[SyncRecord]):
    """A page of :class:`SyncRecord` objects from the sync endpoint.

    Args:
        resources (Sequence[SyncRecord]): The records in this page.
        cursor (str | None): Cursor to pass as ``cursor`` to the next ``sync_resume`` call to resume
            from this position.
        has_next (bool): Whether more changes are available beyond this page.
        typing (TypeInformation | None): Property type information, present when the request was
            made with ``include_typing=True``.
    """

    _RESOURCE = SyncRecord

    def __init__(
        self,
        resources: Sequence[SyncRecord],
        cursor: str | None = None,
        has_next: bool = False,
        typing: TypeInformation | None = None,
    ) -> None:
        super().__init__(resources)
        self.cursor = cursor
        self.has_next = has_next
        self.typing = typing

    @classmethod
    def _load_response(cls, response: dict[str, Any]) -> Self:
        return cls._load_raw_api_response([response])

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]]) -> Self:
        last_response = responses[-1]
        typing = next(
            (TypeInformation._load(response["typing"]) for response in responses if "typing" in response), None
        )
        return cls(
            [SyncRecord._load(item) for response in responses for item in response["items"]],
            cursor=last_response["nextCursor"],
            has_next=last_response["hasNext"],
            typing=typing,
        )
