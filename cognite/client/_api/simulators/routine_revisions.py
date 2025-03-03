from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.simulators import PropertySort
from cognite.client.data_classes.simulators.filters import SimulatorRoutineRevisionsFilter
from cognite.client.data_classes.simulators.routine_revisions import (
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionsList,
    SimulatorRoutineRevisionWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRoutineRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines/revisions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 20

    def list(
        self,
        limit: int = 10,  # deviation from DEFAULT_LIMIT_READ to avoid paging by default (max 20)
        sort: PropertySort | None = None,
        filter: SimulatorRoutineRevisionsFilter | dict[str, Any] | None = None,
        include_all_fields: bool = False,
    ) -> SimulatorRoutineRevisionsList:
        """`Filter simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routine_revisions_simulators_routines_revisions_list_post>`_

        Retrieves a list of simulator routine revisions that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 10. Set to -1, float(“inf”) or None to return all items.
            sort (PropertySort | None): The criteria to sort by.
            filter (SimulatorRoutineRevisionsFilter | dict[str, Any] | None): Filter to apply.
            include_all_fields (bool): If all fields should be included in the response. Defaults to false which does not include script, configuration.inputs and configuration.outputs in the response.

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
            else None,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            other_params={"includeAllFields": include_all_fields},
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorRoutineRevision | None:
        """`Retrieve simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve a simulator routine revision by ID or external ID

        Args:
            id (int | None): The id of the simulator routine revision.
            external_id (str | None): The external id of the simulator routine revision.

        Returns:
            SimulatorRoutineRevision | None: Requested simulator routine revision

        Examples:

            Get simulator routine revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.retrieve(ids=123)

            Get simulator routine revision by external id:
                >>> res = client.simulators.routines.revisions.retrieve(external_ids="abcdef")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionsList,
            identifiers=identifiers,
            resource_path="/simulators/routines/revisions",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> SimulatorRoutineRevisionsList:
        """`Retrieve simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve one or more simulator routine revisions by IDs or external IDs

            Args:
                ids (Sequence[int] | None): IDs
                external_ids (SequenceNotStr[str] | None): External IDs
                ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

            Returns:
                SimulatorRoutineRevisionsList: Requested simulator routine revisions

            Examples:

                Get simulator routine revisions by id:
                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.routines.revisions.retrieve_multiple(ids=[1, 2, 3])

                Get simulator routine revisions by external id:
                    >>> res = client.simulators.routines.revisions.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SimulatorRoutineRevisionsList,
            resource_cls=SimulatorRoutineRevision,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    def create(self, routine_revision: Sequence[SimulatorRoutineRevisionWrite]) -> SimulatorRoutineRevisionsList: ...

    @overload
    def create(self, routine_revision: SimulatorRoutineRevisionWrite) -> SimulatorRoutineRevision: ...

    def create(
        self,
        routine_revision: SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite],
    ) -> SimulatorRoutineRevision | SimulatorRoutineRevisionsList:
        """`Create simulator routine revisions <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post>`_
        Args:
            routine_revision (SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite]): No description.
        Returns:
            SimulatorRoutineRevision | SimulatorRoutineRevisionsList: Created simulator routine revision(s)
        Examples:
            Create new simulator routine revisions:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.routines import SimulatorRoutineRevisionWrite
                >>> client = CogniteClient()
                >>> routine_revs = [
                ...     SimulatorRoutineRevisionWrite(
                ...         external_id="routine_rev_1",
                ...         routine_external_id="routine_1",
                ...         schedule={"enabled": True, "cronExpression": "*/10 * * * *"},
                ...         data_sampling={"enabled": True, "samplingWindow": 15, "granularity": 1},
                ...         logical_check=[],
                ...         steady_state_detection=[],
                ...         inputs=[
                ...             {
                ...                 "name": "Cold Water Temperature",
                ...                 "reference_id": "CWTC",
                ...                 "value": 10.0,
                ...                 "value_type": "DOUBLE",
                ...                 "unit": {"name": "C", "quantity": "temperature"},
                ...                 "save_timeseries_external_id": "TEST-ROUTINE-INPUT-CWTC",
                ...             },
                ...             {
                ...                 "name": "Cold Water Pressure",
                ...                 "reference_id": "CWPC",
                ...                 "value": 3.6,
                ...                 "value_type": "DOUBLE",
                ...                 "unit": {"name": "bar", "quantity": "pressure"},
                ...             },
                ...         ],
                ...         outputs=[
                ...             {
                ...                 "name": "Shower Temperature",
                ...                 "reference_id": "ST",
                ...                 "unit": {"name": "C", "quantity": "temperature"},
                ...                 "value_type": "DOUBLE",
                ...                 "save_timeseries_external_id": "TEST-ROUTINE-OUTPUT-ST",
                ...             },
                ...         ],
                ...         script=[
                ...             {
                ...                 "order": 1,
                ...                 "description": "Set Inputs",
                ...                 "steps": [
                ...                     {
                ...                         "order": 1,
                ...                         "step_type": "Set",
                ...                         "arguments": {"reference_id": "CWTC", "object_name": "Cold water", "object_property": "Temperature"},
                ...                     },
                ...                     {
                ...                         "order": 2,
                ...                         "step_type": "Set",
                ...                         "arguments": {"reference_id": "CWPC", "object_name": "Cold water", "object_property": "Pressure"},
                ...                     },
                ...                 ],
                ...             },
                ...             {
                ...                 "order": 2,
                ...                 "description": "Solve the flowsheet",
                ...                 "steps": [{"order": 1, "step_type": "Command", "arguments": {"command": "Solve"}}],
                ...             },
                ...             {
                ...                 "order": 3,
                ...                 "description": "Set simulation outputs",
                ...                 "steps": [
                ...                     {
                ...                         "order": 1,
                ...                         "step_type": "Get",
                ...                         "arguments": {"reference_id": "ST", "object_name": "Shower", "object_property": "Temperature"},
                ...                     },
                ...                 ],
                ...             },
                ...         ],
                ...     ),
                ... ]
                >>> res = client.simulators.routines.create(routines)
        """
        self._warning.warn()
        assert_type(
            routine_revision,
            "simulator_routine_revision",
            [SimulatorRoutineRevisionWrite, Sequence],
        )

        return self._create_multiple(
            list_cls=SimulatorRoutineRevisionsList,
            resource_cls=SimulatorRoutineRevision,
            items=routine_revision,
            input_resource_cls=SimulatorRoutineRevisionWrite,
            resource_path=self._RESOURCE_PATH,
        )
