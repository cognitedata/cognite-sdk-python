from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cognite.client.data_classes.data_modeling.ids import DataModelId


@dataclass
class DMLApplyResult:
    space: str
    external_id: str
    version: str
    name: str | None
    description: str | None
    created_time: int
    last_updated_time: int

    def as_id(self) -> DataModelId:
        return DataModelId(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
        )

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
