from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from cognite.client.data_classes._base import CogniteFilter, CogniteSort


class SimulatorIntegrationFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: str | Sequence[str] | None = None,
        active: bool | None = None,
    ) -> None:
        self.simulator_external_ids = (
            [simulator_external_ids] if isinstance(simulator_external_ids, str) else simulator_external_ids
        )
        self.active = active


class SimulatorModelsFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: str | Sequence[str] | None = None,
    ) -> None:
        self.simulator_external_ids = (
            [simulator_external_ids] if isinstance(simulator_external_ids, str) else simulator_external_ids
        )


class SimulatorModelRevisionsFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: str | Sequence[str] | None = None,
        all_versions: bool | None = None,
    ) -> None:
        self.model_external_ids = [model_external_ids] if isinstance(model_external_ids, str) else model_external_ids


class SimulatorRoutinesFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.model_external_ids = model_external_ids
        self.simulator_integration_external_ids = simulator_integration_external_ids


class PropertySort(CogniteSort):
    def __init__(
        self,
        property: Literal["createdTime"] = "createdTime",
        order: Literal["asc", "desc"] = "asc",
    ):
        super().__init__(property, order)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        dumped["property"] = self.property
        return dumped


class SimulatorRoutineRevisionsFilter(CogniteFilter):
    def __init__(
        self,
        routine_external_ids: Sequence[str] | None = None,
        all_versions: bool | None = None,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        simulator_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.model_external_ids = model_external_ids
        self.all_versions = all_versions
        self.routine_external_ids = routine_external_ids
        self.simulator_integration_external_ids = simulator_integration_external_ids
        self.simulator_external_ids = simulator_external_ids
