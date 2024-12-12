from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulationRunsFilter
from cognite.client.data_classes.simulators.simulators import (
    CreatedTimeSort,
    SimulationRun,
    SimulationRunsList,
    SimulationTimeSort,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRunsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/runs"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter: SimulationRunsFilter | dict[str, Any] | None = None,
        sort: CreatedTimeSort | SimulationTimeSort | None = None,
    ) -> SimulationRunsList:
        """`Filter simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>`_

        Retrieves a list of simulation runs that match the given criteria

        Args:
            limit (int): The maximum number of simulation runs to return.
            filter (SimulationRunsFilter | dict[str, Any] | None): Filter to apply.

        Returns:
            SimulationRunsList: List of simulation runs

        Examples:

            List simulation runs:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.runs.list()

        """

        sort_by = None
        if isinstance(sort, CreatedTimeSort):
            sort_by = [CreatedTimeSort.load(sort).dump()]
        elif isinstance(sort, SimulationTimeSort):
            sort_by = [SimulationTimeSort.load(sort).dump()]

        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/runs/list",
            resource_cls=SimulationRun,
            list_cls=SimulationRunsList,
            sort=sort_by,
            filter=filter.dump()
            if isinstance(filter, SimulationRunsFilter)
            else filter
            if isinstance(filter, dict)
            else None,
        )
