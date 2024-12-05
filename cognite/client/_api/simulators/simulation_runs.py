from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulationRunsFilter
from cognite.client.data_classes.simulators.simulators import SimulationRun, SimulationRunCall, SimulationRunsList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRunsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/runs"
    _RESOURCE_PATH_RUN = "/simulators/run"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulationRunsFilter | dict[str, Any] | None = None
    ) -> SimulationRunsList:
        """`Filter simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>`_

        List simulation runs

        Args:
            limit (int): The maximum number of simulation runs to return. Defaults to 100.
            filter (SimulationRunsFilter | dict[str, Any] | None): The filter that helps narrow down the list of simulation runs.

        Returns:
            SimulationRunsList: List of simulation runs

        Examples:

            List simulation runs:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.runs.list()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/runs/list",
            resource_cls=SimulationRun,
            list_cls=SimulationRunsList,
            filter=filter.dump()
            if isinstance(filter, SimulationRunsFilter)
            else filter
            if isinstance(filter, dict)
            else None,
        )

    def retrieve(self, id: int | None = None) -> SimulationRun | None:
        """` Retrieve a simulation run

        Args:
            id (int | None): ID

        Returns:
            SimulationRun | None: The simulation run

        Examples:

        """
        identifier = IdentifierSequence.load(ids=id).as_singleton()
        return self._retrieve_multiple(list_cls=SimulationRunsList, resource_cls=SimulationRun, identifiers=identifier)

    def run(
        self,
        run_call: SimulationRunCall | None = None,
        wait: bool = False,
    ) -> SimulationRun:
        """`Run a simulation.

        Args:
            run_call (SimulationRunCall | None): No description.
            wait (bool): No description.

        Returns:
            SimulationRun: A simulation run object.
            
        """
        url = self._RESOURCE_PATH_RUN
        try:
            res = self._post(url, json={"items": [run_call.dump()]})
            response = res.json()
            run_response = response["items"][0]
        except (KeyError, IndexError, ValueError) as e:
            raise RuntimeError("Failed to parse simulation run response") from e
        run = SimulationRun._load(run_response, cognite_client=self._cognite_client)
        if wait:
            run.wait()
        return run
