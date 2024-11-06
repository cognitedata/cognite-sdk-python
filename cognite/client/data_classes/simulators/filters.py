from __future__ import annotations

from typing import Any, Sequence

from cognite.client.data_classes._base import CogniteFilter


class SimulatorIntegrationFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: Sequence[str] | None = None,
        active: bool | None = None,
    ) -> None:
        self.simulator_external_ids = simulator_external_ids
        self.active = active

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorModelsFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.simulator_external_ids = simulator_external_ids

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorModelRevisionsFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: Sequence[str] | None = None,
        all_versions: bool | None = None,
    ) -> None:
        self.model_external_ids = model_external_ids
        self.all_versions = all_versions

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutinesFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.model_external_ids = model_external_ids
        self.simulator_integration_external_ids = simulator_integration_external_ids

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)
