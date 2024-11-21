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
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRoutinesAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators")

    def list(
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
                    >>> res = client.simulators.list_routines()

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

    def list_revisions(
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
                    >>> res = client.simulators.list_revisions()

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

    def retrieve_revision(
        self, id: int | None = None, external_id: str | None = None
    ) -> SimulatorRoutineRevision | None:
        """`Retrieve Simulator Routine Revisions <https://api-docs.cogheim.net/redoc/#tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve Simulator Routine Revisions

        Args:
            id (int | None): The id of the simulator routine revision.
            external_id (str | None): The external id of the simulator routine revision.

        Returns:
            SimulatorRoutineRevision | None: Requested simulator routine revision

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.retrieve_revision()

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionsList,
            identifiers=identifiers,
            resource_path="/simulators/routines/revisions",
            headers={"cdf-version": "beta"},
        )
