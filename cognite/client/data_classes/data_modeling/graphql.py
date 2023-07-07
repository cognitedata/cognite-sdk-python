from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class DMLApplyResult:
    space: str
    external_id: str
    version: str
    name: Optional[str]
    description: Optional[str]
    created_time: int
    last_updated_time: int

    @classmethod
    def load(cls, data: dict[str, Any]) -> DMLApplyResult:
        return cls(
            space=data["space"],
            external_id=data["externalId"],
            version=data["version"],
            name=data["name"],
            description=data["description"],
            created_time=data["createdTime"],
            last_updated_time=data["lastUpdatedTime"],
        )
