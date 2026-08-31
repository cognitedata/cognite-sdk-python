from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import aggregates as aggs
from cognite.client.data_classes.data_modeling._validation import validate_property_path
from cognite.client.data_classes.data_modeling.data_types import UnitReference, UnitSystemReference
from cognite.client.data_classes.data_modeling.ids import ContainerId, PropertyPath, ViewId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.utils._identifier import IdentifierSequenceCore
from cognite.client.utils._identifier import RecordId as RecordId  # explicit re-export
from cognite.client.utils.useful_types import SequenceNotStr


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)


@dataclass(frozen=True)
class RecordContainerId(ContainerId):
    """Container reference used as a source in a record write or read.

    Args:
        space (str): Space that contains the container.
        external_id (str): External ID of the container.
    """


@dataclass(frozen=True)
class RecordViewId(ViewId):
    """View reference used as a source in a record write or read.

    Args:
        space (str): Space that contains the view.
        external_id (str): External ID of the view.
        version (str): Version of the view.
    """

    version: str

    def __post_init__(self) -> None:
        if self.version is None:
            raise TypeError("RecordViewId requires an explicit 'version'.")


RecordSourceIdentifier: TypeAlias = ContainerId | ViewId | tuple[str, str] | tuple[str, str, str]


def _load_record_source_id(data: RecordSourceIdentifier | dict[str, Any]) -> RecordContainerId | RecordViewId:
    if isinstance(data, RecordViewId):
        return data
    if isinstance(data, RecordContainerId):
        return data
    if isinstance(data, ViewId):
        if data.version is None:
            raise ValueError("A view used as a record source requires an explicit version.")
        return RecordViewId(space=data.space, external_id=data.external_id, version=data.version)
    if isinstance(data, ContainerId):
        return RecordContainerId(space=data.space, external_id=data.external_id)
    if isinstance(data, tuple):
        if len(data) == 3:
            return RecordViewId(space=data[0], external_id=data[1], version=data[2])
        if len(data) == 2:
            return RecordContainerId(space=data[0], external_id=data[1])
        raise ValueError(f"Invalid tuple length for record source identifier: {len(data)}, expected 2 or 3.")
    if isinstance(data, dict):
        source_type = data.get("type")
        if source_type == "view" or (source_type is None and "version" in data):
            return RecordViewId.load(data)
        if source_type in ("container", None):
            return RecordContainerId.load(data)
        raise ValueError(f"Record source 'type' must be 'container' or 'view', but was {source_type!r}")
    raise TypeError(f"Cannot load record source from {type(data).__name__}")


class RecordSource(CogniteResource):
    """Container or view source with property values for a record write.

    Args:
        source (RecordSourceIdentifier): Reference to the container or view.
        properties (dict[str, Any]): The data to write to the source container or view.
    """

    def __init__(
        self,
        source: RecordSourceIdentifier,
        properties: dict[str, Any],
    ) -> None:
        self.source = _load_record_source_id(source)
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=_load_record_source_id(resource["source"]),
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
        sources (list[RecordSource]): Container or view property values to write (1-100 sources).
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
            ``{space: {source_identifier: {property_id: value}}}``, where the source identifier
            is ``container_external_id`` for a container and ``view_external_id/version`` for
            a view.
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
        sources: list[RecordSource] = []
        for space, containers in (self.properties or {}).items():
            for container_or_view, props in containers.items():
                source: RecordContainerId | RecordViewId
                if "/" in container_or_view:
                    view_xid, version = container_or_view.split("/", 1)
                    source = RecordViewId(space=space, external_id=view_xid, version=version)
                else:
                    source = RecordContainerId(space=space, external_id=container_or_view)
                sources.append(RecordSource(source=source, properties=dict(props)))
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
    """Selects which container or view properties to return for a record.

    Args:
        source (RecordSourceIdentifier): The container or view to select properties from.
        properties (SequenceNotStr[str]): Property identifiers to return; use ``["*"]`` to return all.
    """

    def __init__(
        self,
        source: RecordSourceIdentifier,
        properties: SequenceNotStr[str],
    ) -> None:
        self.source = _load_record_source_id(source)
        self.properties = validate_property_path(
            properties,
            "properties",
            'Properties are the container or view property identifiers to return, e.g. ["temperature"], or ["*"] for all.',
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(source=_load_record_source_id(resource["source"]), properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"source": self.source.dump(camel_case=camel_case), "properties": self.properties}


class RecordTargetUnit(CogniteResource):
    """A target unit conversion for one Records container or view property.

    Args:
        property (PropertyPath): Fully qualified container or view property path:
            ``[space, container_external_id, property_id]`` or
            ``[space, "view_external_id/version", property_id]``.
        unit (UnitReference | UnitSystemReference): Target unit or target unit system.
    """

    def __init__(self, property: PropertyPath, unit: UnitReference | UnitSystemReference) -> None:
        self.property = validate_property_path(property)
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
