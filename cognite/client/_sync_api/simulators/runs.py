"""
===============================================================================
21c47330969eedc273a7c18877879b7e
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators.filters import SimulationRunsSort
from cognite.client.data_classes.simulators.runs import (
    SimulationRun,
    SimulationRunDataList,
    SimulationRunList,
    SimulationRunWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorRunsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
        status: str | None = None,
        run_type: str | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        routine_revision_external_ids: SequenceNotStr[str] | None = None,
        model_revision_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        simulation_time: TimestampRange | None = None,
        sort: SimulationRunsSort | None = None,
    ) -> Iterator[SimulationRunList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
        status: str | None = None,
        run_type: str | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        routine_revision_external_ids: SequenceNotStr[str] | None = None,
        model_revision_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        simulation_time: TimestampRange | None = None,
        sort: SimulationRunsSort | None = None,
    ) -> Iterator[SimulationRun]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
        status: str | None = None,
        run_type: str | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        routine_revision_external_ids: SequenceNotStr[str] | None = None,
        model_revision_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        simulation_time: TimestampRange | None = None,
        sort: SimulationRunsSort | None = None,
    ) -> Iterator[SimulationRun | SimulationRunList]:
        """
        Iterate over simulation runs

        Fetches simulation runs as they are iterated over, so you keep a limited number of simulation runs in memory.

        Args:
            chunk_size (int | None): Number of simulation runs to return in each chunk. Defaults to yielding one simulation run a time.
            limit (int | None): The maximum number of simulation runs to return, pass None to return all.
            status (str | None): Filter by simulation run status
            run_type (str | None): Filter by simulation run type
            model_external_ids (SequenceNotStr[str] | None): Filter by simulator model external ids
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter by simulator integration external ids
            simulator_external_ids (SequenceNotStr[str] | None): Filter by simulator external ids
            routine_external_ids (SequenceNotStr[str] | None): Filter by routine external ids
            routine_revision_external_ids (SequenceNotStr[str] | None): Filter by routine revision external ids
            model_revision_external_ids (SequenceNotStr[str] | None): Filter by model revision external ids
            created_time (TimestampRange | None): Filter by created time
            simulation_time (TimestampRange | None): Filter by simulation time
            sort (SimulationRunsSort | None): The criteria to sort by.

        Yields:
            SimulationRun | SimulationRunList: yields Simulation Run one by one if chunk is not specified, else SimulatorRunsList objects.
        """
        yield from SyncIterator(
            self.__async_client.simulators.runs(
                chunk_size=chunk_size,
                limit=limit,
                status=status,
                run_type=run_type,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                simulator_external_ids=simulator_external_ids,
                routine_external_ids=routine_external_ids,
                routine_revision_external_ids=routine_revision_external_ids,
                model_revision_external_ids=model_revision_external_ids,
                created_time=created_time,
                simulation_time=simulation_time,
                sort=sort,
            )
        )  # type: ignore [misc]

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        status: str | None = None,
        run_type: str | None = None,
        model_external_ids: SequenceNotStr[str] | None = None,
        simulator_integration_external_ids: SequenceNotStr[str] | None = None,
        simulator_external_ids: SequenceNotStr[str] | None = None,
        routine_external_ids: SequenceNotStr[str] | None = None,
        routine_revision_external_ids: SequenceNotStr[str] | None = None,
        model_revision_external_ids: SequenceNotStr[str] | None = None,
        created_time: TimestampRange | None = None,
        simulation_time: TimestampRange | None = None,
        sort: SimulationRunsSort | None = None,
    ) -> SimulationRunList:
        """
        `Filter simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>`_

        Retrieves a list of simulation runs that match the given criteria.

        Args:
            limit (int | None): The maximum number of simulation runs to return, pass None to return all.
            status (str | None): Filter by simulation run status
            run_type (str | None): Filter by simulation run type
            model_external_ids (SequenceNotStr[str] | None): Filter by simulator model external ids
            simulator_integration_external_ids (SequenceNotStr[str] | None): Filter by simulator integration external ids
            simulator_external_ids (SequenceNotStr[str] | None): Filter by simulator external ids
            routine_external_ids (SequenceNotStr[str] | None): Filter by routine external ids
            routine_revision_external_ids (SequenceNotStr[str] | None): Filter by routine revision external ids
            model_revision_external_ids (SequenceNotStr[str] | None): Filter by model revision external ids
            created_time (TimestampRange | None): Filter by created time
            simulation_time (TimestampRange | None): Filter by simulation time
            sort (SimulationRunsSort | None): The criteria to sort by.

        Returns:
            SimulationRunList: List of simulation runs

        Examples:
            List simulation runs:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.runs.list()

            Iterate over simulation runs, one-by-one:
                >>> for run in client.simulators.runs():
                ...     run  # do something with the simulation run

            Filter runs by status and simulator external ids:
                >>> res = client.simulators.runs.list(
                ...     simulator_external_ids=["PROSPER", "DWSIM"],
                ...     status="success"
                ... )

            Filter runs by time ranges:
                >>> from cognite.client.data_classes.shared import TimestampRange
                >>> res = client.simulators.runs.list(
                ...     created_time=TimestampRange(min=0, max=1_700_000_000_000),
                ...     simulation_time=TimestampRange(min=0, max=1_700_000_000_000),
                ... )
        """
        return run_sync(
            self.__async_client.simulators.runs.list(
                limit=limit,
                status=status,
                run_type=run_type,
                model_external_ids=model_external_ids,
                simulator_integration_external_ids=simulator_integration_external_ids,
                simulator_external_ids=simulator_external_ids,
                routine_external_ids=routine_external_ids,
                routine_revision_external_ids=routine_revision_external_ids,
                model_revision_external_ids=model_revision_external_ids,
                created_time=created_time,
                simulation_time=simulation_time,
                sort=sort,
            )
        )

    @overload
    def retrieve(self, ids: int) -> SimulationRun | None: ...

    @overload
    def retrieve(self, ids: Sequence[int]) -> SimulationRunList | None: ...

    def retrieve(self, ids: int | Sequence[int]) -> SimulationRun | SimulationRunList | None:
        """
        `Retrieve simulation runs by ID <https://api-docs.cognite.com/20230101/tag/Simulation-Runs/operation/simulation_by_id_simulators_runs_byids_post>`_

        Args:
            ids (int | Sequence[int]): The ID(s) of the simulation run(s) to retrieve.

        Returns:
            SimulationRun | SimulationRunList | None: The simulation run(s) with the given ID(s)

        Examples:
            Retrieve a single simulation run by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> run = client.simulators.runs.retrieve(ids=2)
        """
        return run_sync(self.__async_client.simulators.runs.retrieve(ids=ids))

    @overload
    def create(self, items: SimulationRunWrite) -> SimulationRun: ...

    @overload
    def create(self, items: Sequence[SimulationRunWrite]) -> SimulationRunList: ...

    def create(self, items: SimulationRunWrite | Sequence[SimulationRunWrite]) -> SimulationRun | SimulationRunList:
        """
        `Create simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/run_simulation_simulators_run_post>`_

        Args:
            items (SimulationRunWrite | Sequence[SimulationRunWrite]): The simulation run(s) to execute.

        Returns:
            SimulationRun | SimulationRunList: Created simulation run(s)

        Examples:
            Create new simulation run:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.runs import SimulationRunWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> run = [
                ...     SimulationRunWrite(
                ...         routine_external_id="routine1",
                ...         log_severity="Debug",
                ...         run_type="external",
                ...     ),
                ... ]
                >>> res = client.simulators.runs.create(run)
        """
        return run_sync(self.__async_client.simulators.runs.create(items=items))

    def list_run_data(self, run_id: int) -> SimulationRunDataList:
        """
        `Get simulation run data <https://developer.cognite.com/api#tag/Simulation-Runs/operation/simulation_data_by_run_id_simulators_runs_data_list_post>`_

        Retrieve data associated with a simulation run by ID.

        Args:
            run_id (int): Simulation run id.

        Returns:
            SimulationRunDataList: List of simulation run data

        Examples:
            Get simulation run data by run id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.runs.list_run_data(run_id=12345)

            Get simulation run data directly on a simulation run object:
                >>> run = client.simulators.runs.retrieve(ids=2)
                >>> res = run.get_data()
        """
        return run_sync(self.__async_client.simulators.runs.list_run_data(run_id=run_id))
