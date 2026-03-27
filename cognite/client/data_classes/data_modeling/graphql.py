from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.utils import _json_extended as _json


@dataclass
class DMLApplyResult(CogniteResource):
    space: str
    external_id: str
    version: str
    name: str | None
    description: str | None
    created_time: str
    last_updated_time: str

    def __str__(self) -> str:
        return _json.dumps(self.dump(camel_case=False), indent=4)

    def as_id(self) -> DataModelId:
        return DataModelId(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            name=resource["name"],
            description=resource["description"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )


@dataclass
class GraphQlQueryResult(CogniteResource):
    items: list[dict[str, Any]]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(resource["items"])
