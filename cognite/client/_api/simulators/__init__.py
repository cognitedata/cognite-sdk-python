from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.simulators.simulator_integrations import SimulatorIntegrationsAPI
from cognite.client._api.simulators.simulator_models import SimulatorModelsAPI
from cognite.client._api.simulators.simulator_routines import SimulatorRoutinesAPI
from cognite.client._api.simulators.simulators import SimulatorsResourceAPI

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SimulatorsAPI(SimulatorsResourceAPI, SimulatorIntegrationsAPI, SimulatorModelsAPI, SimulatorRoutinesAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
