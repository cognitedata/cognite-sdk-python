from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.utils import _json

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class DMLApplyResult(CogniteObject):
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
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
class GraphQlQueryResult(CogniteObject):
    items: list[dict[str, Any]]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(resource["items"])
