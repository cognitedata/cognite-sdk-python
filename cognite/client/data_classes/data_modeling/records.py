from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId

__all__ = [
    "RecordContainerId",
    "RecordId",
    "RecordIdSequence",
    "RecordSource",
    "RecordWrite",
    "RecordWriteList",
    "RecordsAggregation",
]


def _dump_aggregate_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _dump_aggregate_value(val) for key, val in value.items()}
    if isinstance(value, list | tuple):
        return [_dump_aggregate_value(item) for item in value]
    dump = getattr(value, "dump", None)
    if callable(dump):
        return _dump_aggregate_value(dump())
    return value


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
            "properties": self.properties,
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
        aggregates (dict[str, Any]): Aggregate results keyed by the client-defined aggregate IDs.
        typing (TypeInformation | None): Optional property typing metadata.
    """

    def __init__(self, aggregates: dict[str, Any], typing: TypeInformation | None = None) -> None:
        self.aggregates = aggregates
        self.typing = typing

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            aggregates=resource["aggregates"],
            typing=TypeInformation._load(resource["typing"]) if "typing" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"aggregates": _dump_aggregate_value(self.aggregates)}
        if self.typing is not None:
            output["typing"] = self.typing.dump(camel_case=camel_case)
        return output
