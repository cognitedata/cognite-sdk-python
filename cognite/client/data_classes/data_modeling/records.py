from __future__ import annotations

from collections.abc import Sequence

from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId

__all__ = ["RecordId", "RecordIdSequence"]


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)
