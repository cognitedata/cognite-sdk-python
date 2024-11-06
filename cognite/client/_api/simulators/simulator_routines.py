from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulatorRoutineRevisionsFilter, SimulatorRoutinesFilter
from cognite.client.data_classes.simulators.simulators import (
    SimulatorRoutine,
    SimulatorRoutineList,
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionsList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRoutinesAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators")

    def list_routines(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorRoutinesFilter | dict[str, Any] | None = None
    ) -> SimulatorRoutineList:
        """`Filter Simulators <https://api-docs.cognite.com/20230101-alpha/tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): The maximum number of simulators to return. Defaults to 100.
            filter (SimulatorRoutinesFilter | dict[str, Any] | None): The filter to narrow down simulator routines.

        Returns:
            SimulatorRoutineList: List of simulator routines

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list_models()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/list",
            resource_cls=SimulatorRoutine,
            list_cls=SimulatorRoutineList,
            headers={"cdf-version": "beta"},
            filter=filter.dump()
            if isinstance(filter, SimulatorRoutinesFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )

    def list_routine_revisions(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorRoutineRevisionsFilter | dict[str, Any] | None = None
    ) -> SimulatorRoutineRevisionsList:
        """`Filter Simulators <https://api-docs.cognite.com/20230101-alpha/tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): The maximum number of simulators to return. Defaults to 25. Set to -1, float("inf") or None
            filter (SimulatorRoutineRevisionsFilter | dict[str, Any] | None): The filter to narrow down simulator routine revisions.

        Returns:
            SimulatorRoutineRevisionsList: List of simulator routines

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list_models()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/revisions/list",
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionsList,
            headers={"cdf-version": "beta"},
            filter=filter.dump()
            if isinstance(filter, SimulatorRoutineRevisionsFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )
