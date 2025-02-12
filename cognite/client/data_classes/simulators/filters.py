from __future__ import annotations

from collections.abc import Sequence

from cognite.client.data_classes._base import CogniteFilter
from cognite.client.utils.useful_types import SequenceNotStr


class SimulatorIntegrationFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        active: bool | None = None,
    ) -> None:
        self.simulator_external_ids = simulator_external_ids
        self.active = active


class SimulatorModelsFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.simulator_external_ids = (
            [simulator_external_ids] if isinstance(simulator_external_ids, str) else simulator_external_ids
        )


class SimulatorModelRevisionsFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: Sequence[str] | None = None,
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
