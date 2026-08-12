from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import aggregates as aggs
from cognite.client.data_classes.data_modeling.data_types import UnitReference, UnitSystemReference
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.utils._identifier import IdentifierSequenceCore
from cognite.client.utils._identifier import RecordId as RecordId  # explicit re-export


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
        properties (dict[str, Any]): The data to write to the source container.
    """

    def __init__(self, source: RecordContainerId, properties: dict[str, Any]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordContainerId.load(resource["source"]),
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case),
            "properties": deepcopy(self.properties),
        }


class RecordWrite(WriteableCogniteResource["RecordWrite"]):
    """Write representation of a record, used for ingest and upsert.

    This is the write version of :class:`Record`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record (1-256 chars, no null bytes).
        sources (list[RecordSource]): Container property values to write (1-100 sources).
    """

    def __init__(self, space: str, external_id: str, sources: list[RecordSource]) -> None:
        self.space = space
        self.external_id = external_id
        self.sources = sources

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


class RecordsAggregation(CogniteResource):
    """Aggregate results returned from the Records aggregate endpoint.

    Args:
        aggregates (dict[str, aggs.Result]): Aggregate results keyed by the client-defined
            aggregate IDs.
        typing (TypeInformation | None): Optional property typing metadata.
    """

    def __init__(self, aggregates: dict[str, aggs.Result], typing: TypeInformation | None = None) -> None:
        self.aggregates = aggregates
        self.typing = typing

    def __getitem__(self, aggregate_id: str) -> aggs.Result:
        return self.aggregates[aggregate_id]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            aggregates=aggs._load_results(resource["aggregates"]),
            typing=TypeInformation._load(resource["typing"]) if "typing" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"aggregates": aggs._dump_results(self.aggregates, camel_case)}
        if self.typing is not None:
            output["typing"] = self.typing.dump(camel_case=camel_case)
        return output


class Record(WriteableCogniteResource["RecordWrite"]):
    """A record returned from the stream records API.

    This is the read version of :class:`RecordWrite`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values keyed by
            ``{space: {container_external_id: {property_id: value}}}``.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "createdTime" if camel_case else "created_time": self.created_time,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
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


class RecordList(WriteableCogniteResourceList[RecordWrite, Record]):
    """A list of :class:`Record` objects.

    Args:
        resources (Sequence[Record]): The records.
        typing (TypeInformation | None): Property type information, present when the request
            was made with ``include_typing=True``.
    """

    _RESOURCE = Record

    def __init__(self, resources: Sequence[Record], typing: TypeInformation | None = None) -> None:
        super().__init__(resources)
        self.typing = typing

    def as_ids(self) -> list[RecordId]:
        return [record.as_id() for record in self]

    def as_write(self) -> RecordWriteList:
        return RecordWriteList([record.as_write() for record in self])

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]]) -> Self:
        typing = next((TypeInformation._load(resp["typing"]) for resp in responses if "typing" in resp), None)
        resources = [cls._RESOURCE._load(item) for response in responses for item in response.get("items", [])]
        return cls(resources, typing)


class TimeRange(CogniteResource):
    """A time range filter on ``lastUpdatedTime``.

    Bounds are either milliseconds since the Unix epoch (int) or an ISO-8601 string. At least a
    lower bound (``gte`` or ``gt``) is required for immutable streams; specifying two lower or two
    upper bounds is not allowed.

    Args:
        gte (int | str | None): Greater than or equal to.
        gt (int | str | None): Greater than.
        lte (int | str | None): Less than or equal to.
        lt (int | str | None): Less than.
    """

    def __init__(
        self,
        gte: int | str | None = None,
        gt: int | str | None = None,
        lte: int | str | None = None,
        lt: int | str | None = None,
    ) -> None:
        self.gte = gte
        self.gt = gt
        self.lte = lte
        self.lt = lt

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(gte=resource.get("gte"), gt=resource.get("gt"), lte=resource.get("lte"), lt=resource.get("lt"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            key: value
            for key, value in {"gte": self.gte, "gt": self.gt, "lte": self.lte, "lt": self.lt}.items()
            if value is not None
        }


class RecordSourceSelector(CogniteResource):
    """Selects which container properties to return for a record.

    Args:
        source (RecordContainerId): The container to select properties from.
        properties (list[str]): Property identifiers to return; use ``["*"]`` to return all.
    """

    def __init__(self, source: RecordContainerId, properties: list[str]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(source=RecordContainerId.load(resource["source"]), properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"source": self.source.dump(camel_case=camel_case), "properties": self.properties}


class RecordTargetUnit(CogniteResource):
    """A target unit conversion for one Records container property.

    Args:
        property (list[str]): Fully qualified container property path:
            ``[space, container_external_id, property_id]``.
        unit (UnitReference | UnitSystemReference): Target unit or target unit system.
    """

    def __init__(self, property: list[str], unit: UnitReference | UnitSystemReference) -> None:
        self.property = property
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
        properties (list[RecordTargetUnit] | None): Property-specific target unit conversions.
        unit_system_name (str | None): Convert all convertible properties to a target unit system.
    """

    def __init__(self, properties: list[RecordTargetUnit] | None = None, unit_system_name: str | None = None) -> None:
        self.properties = properties
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


class SyncRecord(Record):
    """A record returned by the sync endpoint, annotated with a change status.

    For ``status="deleted"`` tombstones (mutable streams), :attr:`properties` is ``None``.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        status (Literal['created', 'updated', 'deleted']): The record's change status.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values (absent for
            deleted tombstones).
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        status: Literal["created", "updated", "deleted"],
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(space, external_id, created_time, last_updated_time, properties)
        self.status = status

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
        output = super().dump(camel_case=camel_case)
        output["status"] = self.status
        if self.properties is not None:
            output["properties"] = deepcopy(self.properties)
        return output


class SyncRecordList(CogniteResourceList[SyncRecord]):
    """A page of :class:`SyncRecord` objects from the sync endpoint.

    Args:
        resources (Sequence[SyncRecord]): The records in this page.
        cursor (str | None): Cursor to pass as ``cursor`` to a later ``sync`` call to resume
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
