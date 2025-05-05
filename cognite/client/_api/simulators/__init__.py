from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api.simulators.integrations import SimulatorIntegrationsAPI
from cognite.client._api.simulators.logs import SimulatorLogsAPI
from cognite.client._api.simulators.models import SimulatorModelsAPI
from cognite.client._api.simulators.routines import SimulatorRoutinesAPI
from cognite.client._api.simulators.runs import SimulatorRunsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.simulators import Simulator, SimulatorList
from cognite.client.data_classes.simulators.runs import SimulationInputOverride, SimulationRun
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SimulatorsAPI(APIClient):
    _RESOURCE_PATH = "/simulators"
    _RESOURCE_PATH_RUN = "/simulators/run"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.integrations = SimulatorIntegrationsAPI(config, api_version, cognite_client)
        self.models = SimulatorModelsAPI(config, api_version, cognite_client)
        self.runs = SimulatorRunsAPI(config, api_version, cognite_client)
        self.routines = SimulatorRoutinesAPI(config, api_version, cognite_client)
        self.logs = SimulatorLogsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def __iter__(self) -> Iterator[Simulator]:
        """Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Returns:
            Iterator[Simulator]: yields Simulators one by one.
        """
        return self()

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Simulator]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[SimulatorList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Simulator] | Iterator[SimulatorList]:
        """Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Args:
            chunk_size (int | None): Number of simulators to return in each chunk. Defaults to yielding one simulator a time.
            limit (int | None): Maximum number of simulators to return. Defaults to return all items.

        Returns:
            Iterator[Simulator] | Iterator[SimulatorList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorList,
            resource_cls=Simulator,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> SimulatorList:
        """`List simulators <https://developer.cognite.com/api#tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Returns:
            SimulatorList: List of simulators

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list()

        """
        self._warning.warn()
        return self._list(method="POST", limit=limit, resource_cls=Simulator, list_cls=SimulatorList)

    def run(
        self,
        routine_external_id: str | None = None,
        routine_revision_external_id: str | None = None,
        model_revision_external_id: str | None = None,
        inputs: list[SimulationInputOverride] | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: Literal["Debug", "Information", "Warning", "Error"] | None = None,
        wait: bool = True,
    ) -> SimulationRun:
        """`Run a simulation <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>``
        Args:
            routine_external_id (str | None): External id of the simulator routine
            routine_revision_external_id (str | None): External id of the simulator routine revision
            model_revision_external_id (str | None): External id of the simulator model revision
            inputs (list[SimulationInputOverride] | None): List of input overrides
            run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
            queue (bool | None): Queue the simulation run when connector is down.
            log_severity (Literal["Debug", "Information", "Warning", "Error"] | None): Override the minimum severity level for the simulation run logs. If not provided, the minimum severity is read from the connector logger configuration.
            wait (bool): Wait until the simulation run is finished. Defaults to True.
        Returns:
            SimulationRun : Created simulation run
        Examples:
            Create new simulation run:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> run = client.simulators.run(routine_external_id="routine1", log_severity="Debug")
        """
        self._warning.warn()
        res = self._post(
            url_path=self._RESOURCE_PATH_RUN,
            json={
                "routineExternalId": routine_external_id,
                "routine_revision_external_id": routine_revision_external_id,
                "model_revision_external_id": model_revision_external_id,
                "inputs": inputs.dump() if inputs else None,
                "runTime": run_time,
                "queue": queue,
                "logSeverity": log_severity
            }
        )
        simulation_run = SimulationRun._load(res.json(), cognite_client=self._cognite_client)
        if wait:
            simulation_run.wait()
        return simulation_run
