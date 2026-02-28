"""
===============================================================================
cc8143e90a00dbc0d1a9bde49d575f7f
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators import PropertySort
from cognite.client.data_classes.simulators.routine_revisions import (
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionList,
    SimulatorRoutineRevisionWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorRoutineRevisionsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: int,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        kind: Literal["long"] | None = None,
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
        kind: Literal["long"] | None = None,
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
        kind: Literal["long"] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> Iterator[SimulatorRoutineRevision] | Iterator[SimulatorRoutineRevisionList]:
        """
        Iterate over simulator routine revisions

        Fetches simulator routine revisions as they are iterated over, so you keep a limited number of simulator routine revisions in memory.

        Args:
            chunk_size (int | None): Number of simulator routine revisions to return in each chunk. Defaults to yielding one simulator routine revision a time.
            routine_external_ids (SequenceNotStr[str] | None): Filter on routine external ids.
            model_external_ids (SequenceNotStr[str] | None): Filter on model external ids.
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter on simulator integration external ids.
            simulator_external_ids (SequenceNotStr[str] | None): Filter on simulator external ids.
            kind (Literal['long'] | None): Filter by routine kind. Note that this filter cannot be applied when 'include_all_fields' set to 'True' in the same query.
            created_time (TimestampRange | None): Filter on created time.
            all_versions (bool): If all versions of the routine should be returned. Defaults to false which only returns the latest version.
            include_all_fields (bool): If all fields should be included in the response. Defaults to false which does not include script, configuration.inputs and configuration.outputs in the response.
            limit (int | None): Maximum number of simulator routine revisions to return. Defaults to return all items.
            sort (PropertySort | None): The criteria to sort by.

        Yields:
            SimulatorRoutineRevision | SimulatorRoutineRevisionList: yields SimulatorRoutineRevision one by one if chunk is not specified, else SimulatorRoutineRevisionList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.simulators.routines.revisions(
                chunk_size=chunk_size,
                routine_external_ids=routine_external_ids,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                simulator_external_ids=simulator_external_ids,
                kind=kind,
                created_time=created_time,
                all_versions=all_versions,
                include_all_fields=include_all_fields,
                limit=limit,
                sort=sort,
            )
        )  # type: ignore [misc]

    @overload
    def retrieve(self, *, ids: int) -> SimulatorRoutineRevision | None: ...

    @overload
    def retrieve(self, *, external_ids: str) -> SimulatorRoutineRevision | None: ...

    @overload
    def retrieve(self, *, ids: Sequence[int]) -> SimulatorRoutineRevisionList: ...

    @overload
    def retrieve(self, *, external_ids: SequenceNotStr[str]) -> SimulatorRoutineRevisionList: ...

    def retrieve(
        self, *, ids: int | Sequence[int] | None = None, external_ids: str | SequenceNotStr[str] | None = None
    ) -> SimulatorRoutineRevision | SimulatorRoutineRevisionList | None:
        """
        `Retrieve simulator routine revisions <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/retrieve_simulator_routine_revisions_simulators_routines_revisions_byids_post>`_

        Retrieve simulator routine revisions by ID or External Id.

        Args:
            ids (int | Sequence[int] | None): Simulator routine revision ID or list of IDs
            external_ids (str | SequenceNotStr[str] | None): Simulator routine revision External ID or list of external IDs

        Returns:
            SimulatorRoutineRevision | SimulatorRoutineRevisionList | None: Requested simulator routine revision

        Examples:
            Get simulator routine revision by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.routines.revisions.retrieve(ids=123)

            Get simulator routine revision by external id:
                >>> res = client.simulators.routines.revisions.retrieve(external_ids="routine_v1")
        """
        return run_sync(
            self.__async_client.simulators.routines.revisions.retrieve(  # type: ignore [call-overload]
                ids=ids, external_ids=external_ids
            )
        )

    @overload
    def create(self, items: Sequence[SimulatorRoutineRevisionWrite]) -> SimulatorRoutineRevisionList: ...

    @overload
    def create(self, items: SimulatorRoutineRevisionWrite) -> SimulatorRoutineRevision: ...

    def create(
        self, items: SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite]
    ) -> SimulatorRoutineRevision | SimulatorRoutineRevisionList:
        """
        `Create simulator routine revisions <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post>`_

        Args:
            items (SimulatorRoutineRevisionWrite | Sequence[SimulatorRoutineRevisionWrite]): Simulator routine revisions to create.

        Returns:
            SimulatorRoutineRevision | SimulatorRoutineRevisionList: Created simulator routine revision(s)

        Examples:
            Create new simulator routine revisions:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.routine_revisions import (
                ...     SimulatorRoutineRevisionWrite,
                ...     SimulatorRoutineConfiguration,
                ...     SimulatorRoutineInputConstant,
                ...     SimulatorRoutineOutput,
                ...     SimulatorRoutineDataSampling,
                ...     SimulatorRoutineStep,
                ...     SimulatorRoutineStepArguments,
                ...     SimulatorRoutineStage,
                ...     SimulationValueUnitInput,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> routine_revisions = [
                ...     SimulatorRoutineRevisionWrite(
                ...         external_id="routine_rev_1",
                ...         routine_external_id="routine_1",
                ...         configuration=SimulatorRoutineConfiguration(
                ...             data_sampling=SimulatorRoutineDataSampling(
                ...                 sampling_window=15,
                ...                 granularity=1,
                ...             ),
                ...             inputs=[
                ...                 SimulatorRoutineInputConstant(
                ...                     name="Tubing Head Pressure",
                ...                     reference_id="THP",
                ...                     value=124.3,
                ...                     value_type="DOUBLE",
                ...                     unit=SimulationValueUnitInput(
                ...                         name="bar",
                ...                         quantity="pressure",
                ...                     ),
                ...                     save_timeseries_external_id="TEST-ROUTINE-INPUT-THP",
                ...                 ),
                ...             ],
                ...             outputs=[
                ...                 SimulatorRoutineOutput(
                ...                     name="Bottom Hole Pressure",
                ...                     reference_id="BHP",
                ...                     unit=SimulationValueUnitInput(
                ...                         name="bar",
                ...                         quantity="pressure",
                ...                     ),
                ...                     value_type="DOUBLE",
                ...                     save_timeseries_external_id="TEST-ROUTINE-OUTPUT-BHP",
                ...                 ),
                ...             ],
                ...         ),
                ...         script=[
                ...             SimulatorRoutineStage(
                ...                 order=1,
                ...                 description="Define simulation inputs",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1,
                ...                         step_type="Set",
                ...                         arguments=SimulatorRoutineStepArguments(
                ...                             {
                ...                                 "referenceId": "THP",
                ...                                 "objectName": "WELL",
                ...                                 "objectProperty": "WellHeadPressure",
                ...                             }
                ...                         ),
                ...                     ),
                ...                 ],
                ...             ),
                ...             SimulatorRoutineStage(
                ...                 order=2,
                ...                 description="Solve",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1,
                ...                         step_type="Command",
                ...                         arguments=SimulatorRoutineStepArguments(
                ...                             {"command": "Solve"}
                ...                         ),
                ...                     ),
                ...                 ],
                ...             ),
                ...             SimulatorRoutineStage(
                ...                 order=3,
                ...                 description="Define simulation outputs",
                ...                 steps=[
                ...                     SimulatorRoutineStep(
                ...                         order=1,
                ...                         step_type="Get",
                ...                         arguments=SimulatorRoutineStepArguments(
                ...                             {
                ...                                 "referenceId": "BHP",
                ...                                 "objectName": "WELL",
                ...                                 "objectProperty": "BottomHolePressure",
                ...                             }
                ...                         ),
                ...                     ),
                ...                 ],
                ...             ),
                ...         ],
                ...     ),
                ... ]
                >>> res = client.simulators.routines.revisions.create(routine_revisions)
        """
        return run_sync(self.__async_client.simulators.routines.revisions.create(items=items))

    def list(
        self,
        routine_external_ids: SequenceNotStr[str] | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        kind: Literal["long"] | None = None,
        created_time: TimestampRange | None = None,
        all_versions: bool = False,
        include_all_fields: bool = False,
        limit: int | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorRoutineRevisionList:
        """
        `Filter simulator routine revisions <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/filter_simulator_routine_revisions_simulators_routines_revisions_list_post>`_

        Retrieves a list of simulator routine revisions that match the given criteria.

        Args:
            routine_external_ids (SequenceNotStr[str] | None): Filter on routine external ids.
            model_external_ids (SequenceNotStr[str] | None): Filter on model external ids.
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter on simulator integration external ids.
            simulator_external_ids (SequenceNotStr[str] | None): Filter on simulator external ids.
            kind (Literal['long'] | None): Filter by routine kind. Note that this filter cannot be applied when 'include_all_fields' set to 'True' in the same query.
            created_time (TimestampRange | None): Filter on created time.
            all_versions (bool): If all versions of the routine should be returned. Defaults to false which only returns the latest version.
            include_all_fields (bool): If all fields should be included in the response. Defaults to false which does not include script, configuration.inputs and configuration.outputs in the response.
            limit (int | None): Maximum number of simulator routine revisions to return. Defaults to return all items.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorRoutineRevisionList: List of simulator routine revisions

        Examples:
            List simulator routine revisions:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.routines.revisions.list(limit=10)

            List simulator routine revisions with filter:
                >>> res = client.simulators.routines.revisions.list(
                ...     routine_external_ids=["routine_1"],
                ...     all_versions=True,
                ...     sort=PropertySort(order="asc", property="createdTime"),
                ...     include_all_fields=True
                ... )

            List simulator routine revisions by kind:
                >>> res = client.simulators.routines.revisions.list(
                ...     kind="long"
                ... )
        """
        return run_sync(
            self.__async_client.simulators.routines.revisions.list(
                routine_external_ids=routine_external_ids,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                simulator_external_ids=simulator_external_ids,
                kind=kind,
                created_time=created_time,
                all_versions=all_versions,
                include_all_fields=include_all_fields,
                limit=limit,
                sort=sort,
            )
        )
