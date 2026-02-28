"""
===============================================================================
ab338dba609a32a293cb2ec8821708c7
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.simulators.routine_revisions import SyncSimulatorRoutineRevisionsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.routines import (
    SimulatorRoutine,
    SimulatorRoutineList,
    SimulatorRoutineWrite,
)
from cognite.client.data_classes.simulators.runs import SimulationInputOverride, SimulationRun
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorRoutinesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.revisions = SyncSimulatorRoutineRevisionsAPI(async_client)

    @overload
    def __call__(
        self,
        chunk_size: int,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutineList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutine]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutine] | Iterator[SimulatorRoutineList]:
        """
        Iterate over simulator routines

        Fetches simulator routines as they are iterated over, so you keep a limited number of simulator routines in memory.

        Args:
            chunk_size (int | None): Number of simulator routines to return in each chunk. Defaults to yielding one simulator routine a time.
            model_external_ids (Sequence[str] | None): Filter on model external ids.
            simulator_integration_external_ids (Sequence[str] | None): Filter on simulator integration external ids.
            sort (PropertySort | None): The criteria to sort by.
            limit (int | None): Maximum number of simulator routines to return. Defaults to return all items.

        Yields:
            SimulatorRoutine | SimulatorRoutineList: yields SimulatorRoutine one by one if chunk is not specified, else SimulatorRoutineList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.simulators.routines(
                chunk_size=chunk_size,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                sort=sort,
                limit=limit,
            )
        )  # type: ignore [misc]

    @overload
    def create(self, routine: Sequence[SimulatorRoutineWrite]) -> SimulatorRoutineList: ...

    @overload
    def create(self, routine: SimulatorRoutineWrite) -> SimulatorRoutine: ...

    def create(
        self, routine: SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite]
    ) -> SimulatorRoutine | SimulatorRoutineList:
        """
        `Create simulator routines <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_simulators_routines_post>`_

        Args:
            routine (SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite]): Simulator routine(s) to create.

        Returns:
            SimulatorRoutine | SimulatorRoutineList: Created simulator routine(s)

        Examples:
            Create new simulator routines:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.routines import SimulatorRoutineWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> routines = [
                ...     SimulatorRoutineWrite(
                ...         name="routine1",
                ...         external_id="routine_ext_id",
                ...         simulator_integration_external_id="integration_ext_id",
                ...         model_external_id="model_ext_id",
                ...     ),
                ...     SimulatorRoutineWrite(
                ...         name="routine2",
                ...         external_id="routine_ext_id_2",
                ...         simulator_integration_external_id="integration_ext_id_2",
                ...         model_external_id="model_ext_id_2",
                ...         kind="long",
                ...     )
                ... ]
                >>> res = client.simulators.routines.create(routines)
        """
        return run_sync(self.__async_client.simulators.routines.create(routine=routine))

    def delete(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | SequenceNotStr[str] | None = None,
    ) -> None:
        """
        `Delete simulator routines <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/delete_simulator_routine_simulators_routines_delete_post>`_

        Args:
            ids (int | Sequence[int] | None): ids (or sequence of ids) for the routine(s) to delete.
            external_ids (str | SequenceNotStr[str] | SequenceNotStr[str] | None): external ids (or sequence of external ids) for the routine(s) to delete.

        Examples:
            Delete simulator routines by id or external id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.simulators.routines.delete(ids=[1,2,3], external_ids="foo")
        """
        return run_sync(self.__async_client.simulators.routines.delete(ids=ids, external_ids=external_ids))

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        kind: Literal["long"] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorRoutineList:
        """
        `Filter simulator routines <https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/filter_simulator_routines_simulators_routines_list_post>`_

        Retrieves a list of simulator routines that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            model_external_ids (Sequence[str] | None): Filter on model external ids.
            simulator_integration_external_ids (Sequence[str] | None): Filter on simulator integration external ids.
            kind (Literal['long'] | None): Filter on routine kind.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorRoutineList: List of simulator routines

        Examples:
            List simulator routines:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.routines.list(limit=10)

            Iterate over simulator routines, one-by-one:
                >>> for routine in client.simulators.routines():
                ...     routine  # do something with the simulator routine

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> res = client.simulators.routines.list(
                ...     simulator_integration_external_ids=["integration_ext_id"],
                ...     sort=PropertySort(
                ...         property="createdTime",
                ...         order="desc"
                ...     )
                ... )

            Filter on routine kind:
                >>> res = client.simulators.routines.list(
                ...     kind="long"
                ... )
        """
        return run_sync(
            self.__async_client.simulators.routines.list(
                limit=limit,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                kind=kind,
                sort=sort,
            )
        )

    @overload
    def run(
        self,
        *,
        routine_external_id: str,
        inputs: Sequence[SimulationInputOverride] | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: Literal["Debug", "Information", "Warning", "Error"] | None = None,
        wait: bool = True,
        timeout: float = 60,
    ) -> SimulationRun: ...

    @overload
    def run(
        self,
        *,
        routine_revision_external_id: str,
        model_revision_external_id: str,
        inputs: Sequence[SimulationInputOverride] | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: Literal["Debug", "Information", "Warning", "Error"] | None = None,
        wait: bool = True,
        timeout: float = 60,
    ) -> SimulationRun: ...

    def run(
        self,
        routine_external_id: str | None = None,
        routine_revision_external_id: str | None = None,
        model_revision_external_id: str | None = None,
        inputs: Sequence[SimulationInputOverride] | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: Literal["Debug", "Information", "Warning", "Error"] | None = None,
        wait: bool = True,
        timeout: float = 60,
    ) -> SimulationRun:
        """
        `Run a simulation <https://api-docs.cognite.com/20230101/tag/Simulation-Runs/operation/run_simulation_simulators_run_post>`_

        Run a simulation for a given simulator routine. Supports two modes:
        1. By routine external ID only
        2. By routine revision external ID + model revision external ID

        Args:
            routine_external_id (str | None): External id of the simulator routine to run.
                Cannot be specified together with routine_revision_external_id and model_revision_external_id.
            routine_revision_external_id (str | None): External id of the simulator routine revision to run.
                Must be specified together with model_revision_external_id.
            model_revision_external_id (str | None): External id of the simulator model revision.
                Must be specified together with routine_revision_external_id.
            inputs (Sequence[SimulationInputOverride] | None): List of input overrides
            run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
            queue (bool | None): Queue the simulation run when connector is down.
            log_severity (Literal['Debug', 'Information', 'Warning', 'Error'] | None): Override the minimum severity level for the simulation run logs. If not provided, the minimum severity is read from the connector logger configuration.
            wait (bool): Wait until the simulation run is finished. Defaults to True.
            timeout (float): Timeout in seconds for waiting for the simulation run to finish. Defaults to 60 seconds.

        Returns:
            SimulationRun: Created simulation run

        Examples:
            Create new simulation run using routine external ID:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> run = client.simulators.routines.run(
                ...     routine_external_id="routine1",
                ...     log_severity="Debug"
                ... )

            Create new simulation run using routine and model revision external IDs:
                >>> run = client.simulators.routines.run(
                ...     routine_revision_external_id="routine_revision1",
                ...     model_revision_external_id="model_revision1",
                ... )
        """
        return run_sync(
            self.__async_client.simulators.routines.run(  # type: ignore [call-overload, misc]
                routine_external_id=routine_external_id,
                routine_revision_external_id=routine_revision_external_id,
                model_revision_external_id=model_revision_external_id,
                inputs=inputs,
                run_time=run_time,
                queue=queue,
                log_severity=log_severity,
                wait=wait,
                timeout=timeout,
            )
        )
