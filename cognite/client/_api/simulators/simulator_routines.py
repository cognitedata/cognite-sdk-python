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


class SimulatorRoutineRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines/revisions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorRoutineRevisionsFilter | dict[str, Any] | None = None
    ) -> SimulatorRoutineRevisionsList:
        """`Filter simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routine_revisions_simulators_routines_revisions_list_post>`_

        List simulator routine revisions

        Args:
            limit (int): The maximum number of simulator routine revisions to return. Defaults to 10.
            filter (SimulatorRoutineRevisionsFilter | dict[str, Any] | None): The filter to narrow down simulator routine revisions.

        Returns:
            SimulatorRoutineRevisionsList: List of simulator routine revisions

        Examples:

            List simulator routine revisions:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.list()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/revisions/list",
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionsList,
            filter=filter.dump()
            if isinstance(filter, SimulatorRoutineRevisionsFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorRoutineRevision | None:
        """`Retrieve simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve multiple simulator routine revisions by IDs or external IDs

        Args:
            id (int | None): The id of the simulator routine revision.
            external_id (str | None): The external id of the simulator routine revision.

        Returns:
            SimulatorRoutineRevision | None: Requested simulator routine revision

        Examples:

            Get simulator routine revisions by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.retrieve(ids=[1, 2, 3])

            Get simulator routine revisions by external id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.retrieve(external_ids=["abc", "def"])

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionsList,
            identifiers=identifiers,
            resource_path="/simulators/routines/revisions",
        )


class SimulatorRoutinesAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorRoutineRevisionsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorRoutinesFilter | dict[str, Any] | None = None
    ) -> SimulatorRoutineList:
        """`Filter simulator routines <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routines_simulators_routines_list_post>`_

        List simulator routines

        Args:
            limit (int): The maximum number of simulator routines to return. Defaults to 100.
            filter (SimulatorRoutinesFilter | dict[str, Any] | None): The filter to narrow down simulator routines.

        Returns:
            SimulatorRoutineList: List of simulator routines

        Examples:

            List simulator routines:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.routines.list()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/list",
            resource_cls=SimulatorRoutine,
            list_cls=SimulatorRoutineList,
            filter=filter.dump()
            if isinstance(filter, SimulatorRoutinesFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )
