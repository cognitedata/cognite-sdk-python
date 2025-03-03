from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    IdTransformerMixin,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

Severity = Literal["Debug", "Information", "Warning", "Error"]


@dataclass
class SimulatorLogData(CogniteObject):
    timestamp: int
    message: str
    severity: Severity

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            timestamp=resource["timestamp"],
            message=resource["message"],
            severity=resource["severity"],
        )


class SimulatorLog(CogniteResource):
    def __init__(
        self,
        id: int,
        data: Sequence[SimulatorLogData],
        created_time: int,
        last_updated_time: int,
        data_set_id: int,
        severity: Severity | None = None,
    ) -> None:
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_id = data_set_id
        self.data = data
        self.severity = severity

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            data_set_id=resource["dataSetId"],
            data=[SimulatorLogData._load(entry, cognite_client) for entry in resource["data"]],
            severity=resource.get("severity"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.data, list) and all(isinstance(item, SimulatorLogData) for item in self.data):
            output["data"] = [item.dump(camel_case=camel_case) for item in self.data]

        return output


class SimulatorLogList(CogniteResourceList[SimulatorLog], IdTransformerMixin):
    _RESOURCE = SimulatorLog
