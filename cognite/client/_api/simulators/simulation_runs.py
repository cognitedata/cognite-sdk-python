from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulationRunsFilter
from cognite.client.data_classes.simulators.simulators import SimulationRun, SimulationRunsList
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRunsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/runs"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators")

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulationRunsFilter | dict[str, Any] | None = None
    ) -> SimulationRunsList:
        """`Filter simulation runs <https://api-docs.cogheim.net/redoc/#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>`_

        List all simulation runs

        Args:
            limit (int): The maximum number of simulation runs to return. Defaults to 100.
            filter (SimulationRunsFilter | dict[str, Any] | None): The filter to narrow down simulator models.

        Returns:
            SimulationRunsList: List of simulator models

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/runs/list",
            resource_cls=SimulationRun,
            list_cls=SimulationRunsList,
            headers={"cdf-version": "beta"},
            filter=filter.dump()
            if isinstance(filter, SimulationRunsFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )
