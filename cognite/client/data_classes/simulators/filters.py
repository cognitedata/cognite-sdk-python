from __future__ import annotations

from collections.abc import Sequence

from cognite.client.data_classes._base import CogniteFilter


class SimulatorModelsFilter(CogniteFilter):
    def __init__(
        self,
        simulator_external_ids: Sequence[str] | None = None,
    ) -> None:
        self.simulator_external_ids = simulator_external_ids


class SimulatorModelRevisionsFilter(CogniteFilter):
    def __init__(
        self,
        model_external_ids: Sequence[str] | None = None,
        all_versions: bool | None = None,
    ) -> None:
        self.model_external_ids = model_external_ids
        self.all_versions = all_versions
