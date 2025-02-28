from __future__ import annotations

from collections.abc import Sequence

from cognite.client.data_classes._base import CogniteFilter


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


class SimulatorRunsFilter(CogniteFilter):
    def __init__(
        self,
        status: str | None = None,
        run_type: str | None = None,
        model_external_ids: str | Sequence[str] | None = None,
        simulator_integration_external_ids: str | Sequence[str] | None = None,
        simulator_external_ids: str | Sequence[str] | None = None,
        routine_external_ids: str | Sequence[str] | None = None,
        routine_revision_external_ids: str | Sequence[str] | None = None,
        model_revision_external_ids: str | Sequence[str] | None = None,
    ) -> None:
        self.model_external_ids = [model_external_ids] if isinstance(model_external_ids, str) else model_external_ids
        self.simulator_integration_external_ids = (
            [simulator_integration_external_ids]
            if isinstance(simulator_integration_external_ids, str)
            else simulator_integration_external_ids
        )
        self.simulator_external_ids = (
            [simulator_external_ids] if isinstance(simulator_external_ids, str) else simulator_external_ids
        )
        self.routine_external_ids = (
            [routine_external_ids] if isinstance(routine_external_ids, str) else routine_external_ids
        )
        self.routine_revision_external_ids = (
            [routine_revision_external_ids]
            if isinstance(routine_revision_external_ids, str)
            else routine_revision_external_ids
        )
        self.model_revision_external_ids = (
            [model_revision_external_ids]
            if isinstance(model_revision_external_ids, str)
            else model_revision_external_ids
        )
        self.status = status
        self.run_type = run_type
