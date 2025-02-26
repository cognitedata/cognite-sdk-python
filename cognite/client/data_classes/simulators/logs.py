from __future__ import annotations

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


class SimulatorLog(CogniteResource):
    def __init__(
        self,
        id: int,
        data: list[SimulatorLogData],
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
            data=resource["data"],
            severity=resource.get("severity"),
        )


class SimulatorLogList(CogniteResourceList[SimulatorLog], IdTransformerMixin):
    _RESOURCE = SimulatorLog
