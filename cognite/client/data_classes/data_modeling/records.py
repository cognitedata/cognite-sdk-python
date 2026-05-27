from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
)
from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId

__all__ = ["RecordId", "RecordIdSequence", "RecordSource", "RecordSourceReference", "RecordWrite", "RecordWriteList"]


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)

    def are_unique(self) -> bool:
        return len(self) == len({(r.space, r.external_id) for r in self._identifiers})


class RecordSourceReference(CogniteResource):
    """Container reference used as a source in a record write.

    Args:
        space (str): Space that contains the container.
        external_id (str): External ID of the container.
        type (str): Must be ``"container"`` (default).
    """

    def __init__(self, space: str, external_id: str, type: str = "container") -> None:
        self.space = space
        self.external_id = external_id
        self.type = type

    def to_identifier(self) -> RecordId:
        return RecordId(space=self.space, external_id=self.external_id)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            type=resource.get("type", "container"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self.type,
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
        }


class RecordSource(CogniteResource):
    """Container source with property values for a record write.

    Args:
        source (RecordSourceReference): Reference to the container.
        properties (dict[str, Any]): Map of ``{property_id: value}``.
    """

    def __init__(self, source: RecordSourceReference, properties: dict[str, Any]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordSourceReference._load(resource["source"]),
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

    def to_identifier(self) -> RecordId:
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
