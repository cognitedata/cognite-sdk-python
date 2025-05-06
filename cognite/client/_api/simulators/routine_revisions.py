from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators import PropertySort
from cognite.client.data_classes.simulators.filters import SimulatorRoutineRevisionsFilter
from cognite.client.data_classes.simulators.routine_revisions import (
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionList,
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
        self._LIST_LIMIT = 20
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 20

    def __iter__(self) -> Iterator[SimulatorRoutineRevision]:
        """Iterate over simulator routine revisions

        Fetches simulator routine revisions as they are iterated over, so you keep a limited number of simulator routine revisions in memory.

        Returns:
            Iterator[SimulatorRoutineRevision]: yields Simulator routine revisions one by one.
        """
        return self()

    @overload
    def __call__(
        self,
        chunk_size: int,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> Iterator[SimulatorRoutineRevisionList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> Iterator[SimulatorRoutineRevision]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> Iterator[SimulatorRoutineRevision] | Iterator[SimulatorRoutineRevisionList]:
        """Iterate over simulator routine revisions

        Fetches simulator routine revisions as they are iterated over, so you keep a limited number of simulator routine revisions in memory.

        Args:
            chunk_size (int | None): Number of simulator routine revisions to return in each chunk. Defaults to yielding one simulator routine revision a time.
            routine_external_ids (SequenceNotStr[str] | None): Filter on routine external ids.
            model_external_ids (SequenceNotStr[str] | None): Filter on model external ids.
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter on simulator integration external ids.
            simulator_external_ids (SequenceNotStr[str] | None): Filter on simulator external ids.
            created_time (TimestampRange | None): Filter on created time.
            all_versions (bool): If all versions of the routine should be returned. Defaults to false which only returns the latest version.
            include_all_fields (bool): If all fields should be included in the response. Defaults to false which does not include script, configuration.inputs and configuration.outputs in the response.
            limit (int | None): Maximum number of simulator routine revisions to return. Defaults to return all items.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            Iterator[SimulatorRoutineRevision] | Iterator[SimulatorRoutineRevisionList]: yields SimulatorRoutineRevision one by one if chunk is not specified, else SimulatorRoutineRevisionList objects.
        """
        self._warning.warn()
        filter = SimulatorRoutineRevisionsFilter(
            all_versions=all_versions,
            routine_external_ids=routine_external_ids,
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
            simulator_external_ids=simulator_external_ids,
            created_time=created_time,
        )
        return self._list_generator(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/revisions/list",
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionList,
            chunk_size=chunk_size,
            filter=filter.dump(),
            sort=[PropertySort.load(sort).dump()] if sort else None,
            other_params={"includeAllFields": include_all_fields},
        )

    @overload
    def retrieve(self, ids: None = None, external_ids: None = None) -> NoReturn: ...

    @overload
    def retrieve(self, ids: int, external_ids: None = None) -> SimulatorRoutineRevision | None: ...

    @overload
    def retrieve(
        self,
        ids: None,
        external_ids: str,
    ) -> SimulatorRoutineRevision | None: ...

    @overload
    def retrieve(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
    ) -> SimulatorRoutineRevisionList | None: ...

    def retrieve(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorRoutineRevision | SimulatorRoutineRevisionList | None:
        """`Retrieve simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve simulator routine revisions by ID or External Id.

        Args:
            ids (int | Sequence[int] | None): Simulator routine revision ID or list of IDs
            external_ids (str | SequenceNotStr[str] | None): Simulator routine revision External ID or list of external IDs

        Returns:
            SimulatorRoutineRevision | SimulatorRoutineRevisionList | None: Requested simulator routine revision

        Examples:

            Get simulator routine revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.retrieve(ids=123)

            Get simulator routine revision by external id:
                >>> res = client.simulators.routines.revisions.retrieve(external_ids="routine_v1")
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionList,
            identifiers=identifiers,
            resource_path="/simulators/routines/revisions",
        )

    @overload
    def create(self, routine_revision: Sequence[SimulatorRoutineRevisionWrite]) -> SimulatorRoutineRevisionList: ...

    @overload
    def create(self, routine_revision: SimulatorRoutineRevisionWrite) -> SimulatorRoutineRevision: ...

    def create(
        self,
        routine_revision: SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite],
    ) -> SimulatorRoutineRevision | SimulatorRoutineRevisionList:
        """`Create simulator routine revisions <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post>`_
        Args:
            routine_revision (SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite]): Simulator routine revisions to create.
        Returns:
            SimulatorRoutineRevision | SimulatorRoutineRevisionList: Created simulator routine revision(s)
        Examples:
            Create new simulator routine revisions:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.routine_revisions import SimulatorRoutineRevisionWrite, SimulatorRoutineConfiguration, SimulatorRoutineInputConstant, SimulatorRoutineOutput, SimulatorRoutineDataSampling, SimulatorRoutineStep, SimulatorRoutineStepArguments, SimulatorRoutineStage, SimulationValueUnitInput
                >>> client = CogniteClient()
                >>> routine_revs = [
                ...     SimulatorRoutineRevisionWrite(
                ...         external_id="routine_rev_1",
                ...         routine_external_id="routine_1",
                ...         configuration=SimulatorRoutineConfiguration(
                ...             data_sampling=SimulatorRoutineDataSampling(sampling_window=15, granularity="1m"),
                ...             inputs=[
                ...                 SimulatorRoutineInputConstant(
                ...                     name="Cold Water Temperature",
                ...                     reference_id="CWTC",
                ...                     value=10.0,
                ...                     value_type="DOUBLE",
                ...                     unit=SimulationValueUnitInput(name="C", quantity="temperature"),
                ...                     save_timeseries_external_id="TEST-ROUTINE-INPUT-CWTC",
                ...                 ),
                ...             ],
                ...             outputs=[
                ...                 SimulatorRoutineOutput(
                ...                     name="Shower Temperature",
                ...                     reference_id="ST",
                ...                     unit=SimulationValueUnitInput(name="C", quantity="temperature"),
                ...                     value_type="DOUBLE",
                ...                     save_timeseries_external_id="TEST-ROUTINE-OUTPUT-ST",
                ...                 ),
                ...             ],
                ...         ),
                ...         script=[
                ...             SimulatorRoutineStage(
                ...                 order=1,
                ...                 description="Set Inputs",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1,
                ...                         step_type="Set",
                ...                         arguments=SimulatorRoutineStepArguments(
                ...                             {
                ...                                 "referenceId": "CWTC",
                ...                                 "objectName": "Cold water",
                ...                                 "objectProperty": "Temperature",
                ...                             }
                ...                         ),
                ...                     ),
                ...                 ],
                ...             ),
                ...             SimulatorRoutineStage(
                ...                 order=2,
                ...                 description="Solve the flowsheet",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1, step_type="Command", arguments=SimulatorRoutineStepArguments({"command": "Solve"})
                ...                     ),
                ...                 ],
                ...             ),
                ...             SimulatorRoutineStage(
                ...                 order=3,
                ...                 description="Set simulation outputs",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1,
                ...                         step_type="Get",
                ...                         arguments=SimulatorRoutineStepArguments(
                ...                             {
                ...                                 "referenceId": "ST",
                ...                                 "objectName": "Shower",
                ...                                 "objectProperty": "Temperature",
                ...                             }
                ...                         ),
                ...                     ),
                ...                 ],
                ...             ),
                ...         ],
                ...     ),
                ... ]
                >>> res = client.simulators.routines.revisions.create(routine_revs)
        """
        self._warning.warn()
        assert_type(
            routine_revision,
            "simulator_routine_revision",
            [SimulatorRoutineRevisionWrite, Sequence],
        )

        return self._create_multiple(
            list_cls=SimulatorRoutineRevisionList,
            resource_cls=SimulatorRoutineRevision,
            items=routine_revision,
            input_resource_cls=SimulatorRoutineRevisionWrite,
            resource_path=self._RESOURCE_PATH,
        )

    def list(
        self,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorRoutineRevisionList:
        """`Filter simulator routine revisions <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routine_revisions_simulators_routines_revisions_list_post>`_

        Retrieves a list of simulator routine revisions that match the given criteria.

        Args:
            routine_external_ids (SequenceNotStr[str] | None): Filter on routine external ids.
            model_external_ids (SequenceNotStr[str] | None): Filter on model external ids.
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter on simulator integration external ids.
            simulator_external_ids (SequenceNotStr[str] | None): Filter on simulator external ids.
            created_time (TimestampRange | None): Filter on created time.
            all_versions (bool): If all versions of the routine should be returned. Defaults to false which only returns the latest version.
            include_all_fields (bool): If all fields should be included in the response. Defaults to false which does not include script, configuration.inputs and configuration.outputs in the response.
            limit (int | None): Maximum number of simulator routine revisions to return. Defaults to return all items.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorRoutineRevisionList: List of simulator routine revisions

        Examples:

            List simulator routine revisions:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.revisions.list()

            List simulator routine revisions with filter:
                >>> res = client.simulators.routines.revisions.list(
                ...     routine_external_ids=["routine_1"],
                ...     all_versions=True,
                ...     sort=PropertySort(order="asc", property="createdTime"),
                ...     include_all_fields=True
                ... )

        """
        self._warning.warn()
        filter = SimulatorRoutineRevisionsFilter(
            all_versions=all_versions,
            routine_external_ids=routine_external_ids,
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
            simulator_external_ids=simulator_external_ids,
            created_time=created_time,
        )
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/routines/revisions/list",
            resource_cls=SimulatorRoutineRevision,
            list_cls=SimulatorRoutineRevisionList,
            filter=filter.dump(),
            sort=[PropertySort.load(sort).dump()] if sort else None,
            other_params={"includeAllFields": include_all_fields},
        )
