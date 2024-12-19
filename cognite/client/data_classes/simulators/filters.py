from __future__ import annotations

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
