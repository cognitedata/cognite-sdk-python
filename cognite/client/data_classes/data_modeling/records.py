from __future__ import annotations

from collections.abc import Sequence

from cognite.client.utils._identifier import IdentifierSequenceCore


class RecordId:
    def __init__(self, space: str, external_id: str) -> None:
        self.space = space
        self.external_id = external_id

    @classmethod
    def from_tuple(cls, v: tuple[str, str]) -> RecordId:
        return cls(space=v[0], external_id=v[1])

    def as_dict(self, camel_case: bool = True) -> dict[str, str]:
        return {"space": self.space, ("externalId" if camel_case else "external_id"): self.external_id}

    def as_primitive(self) -> str:
        return self.external_id


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)

    def are_unique(self) -> bool:
        return len(self) == len({(r.space, r.external_id) for r in self._identifiers})
