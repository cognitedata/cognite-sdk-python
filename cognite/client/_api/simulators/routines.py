from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api.simulators.routine_revisions import SimulatorRoutineRevisionsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import PropertySort, SimulatorRoutinesFilter
from cognite.client.data_classes.simulators.routines import (
    SimulatorRoutine,
    SimulatorRoutineList,
    SimulatorRoutineWrite,
)
from cognite.client.data_classes.simulators.runs import (
    SimulationInputOverride,
    SimulationRun,
    SimulationRunWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRoutinesAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorRoutineRevisionsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    def __iter__(self) -> Iterator[SimulatorRoutine]:
        """Iterate over simulator routines

        Fetches simulator routines as they are iterated over, so you keep a limited number of simulator routines in memory.

        Returns:
            Iterator[SimulatorRoutine]: yields Simulator routines one by one.
        """
        return self()

    @overload
    def __call__(
        self,
        chunk_size: int,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutineList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutine]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorRoutine] | Iterator[SimulatorRoutineList]:
        """Iterate over simulator routines

        Fetches simulator routines as they are iterated over, so you keep a limited number of simulator routines in memory.

        Args:
            chunk_size (int | None): Number of simulator routines to return in each chunk. Defaults to yielding one simulator routine a time.
            model_external_ids (Sequence[str] | None): Filter on model external ids.
            simulator_integration_external_ids (Sequence[str] | None): Filter on simulator integration external ids.
            limit (int | None): Maximum number of simulator routines to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorRoutine] | Iterator[SimulatorRoutineList]: yields SimulatorRoutine one by one if chunk is not specified, else SimulatorRoutineList objects.
        """
        self._warning.warn()
        routines_filter = SimulatorRoutinesFilter(
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
        )
        return self._list_generator(
            list_cls=SimulatorRoutineList,
            resource_cls=SimulatorRoutine,
            method="POST",
            filter=routines_filter.dump(),
            chunk_size=chunk_size,
            limit=limit,
        )

    @overload
    def create(self, routine: Sequence[SimulatorRoutineWrite]) -> SimulatorRoutineList: ...

    @overload
    def create(self, routine: SimulatorRoutineWrite) -> SimulatorRoutine: ...

    def create(
        self,
        routine: SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite],
    ) -> SimulatorRoutine | SimulatorRoutineList:
        """`Create simulator routine <https://developer.cognite.com/api#tag/Simulator-Routines/operation/create_simulator_routine_simulators_routines_post>`_
        You can create an arbitrary number of simulator routines.
        Args:
            routine (SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite]): Simulator routines to create.
        Returns:
            SimulatorRoutine | SimulatorRoutineList: Created simulator routine(s)
        Examples:
            Create new simulator routines:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.routines import SimulatorRoutineWrite
                >>> client = CogniteClient()
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
                ...     )
                ... ]
                >>> res = client.simulators.routines.create(routines)
        """
        self._warning.warn()
        assert_type(routine, "simulator_routines", [SimulatorRoutineWrite, Sequence])

        return self._create_multiple(
            list_cls=SimulatorRoutineList,
            resource_cls=SimulatorRoutine,
            items=routine,
            input_resource_cls=SimulatorRoutineWrite,
            resource_path=self._RESOURCE_PATH,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete one or more routines <https://developer.cognite.com/api#tag/Simulator-Routines/operation/delete_simulator_routine_simulators_routines_delete_post>`_

        Args:
            id (int | Sequence[int] | None): ids (or sequence of ids) for the routine(s) to delete.
            external_ids (str | SequenceNotStr[str] | SequenceNotStr[str] | None): external ids (or sequence of external ids) for the routine(s) to delete.

        Examples:

            Delete routines by id or external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.routines.delete(id=[1,2,3], external_id="foo")
        """
        self._warning.warn()
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_ids),
            wrap_ids=True,
        )

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        model_external_ids: Sequence[str] | None = None,
        simulator_integration_external_ids: Sequence[str] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorRoutineList:
        """`Filter simulator routines <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routines_simulators_routines_list_post>`_

        Retrieves a list of simulator routines that match the given criteria

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            model_external_ids (Sequence[str] | None): Filter on model external ids.
            simulator_integration_external_ids (Sequence[str] | None): Filter on simulator integration external ids.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorRoutineList: List of simulator routines

        Examples:
            List simulator routines:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.list()

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> res = client.simulators.routines.list(
                ...     simulator_integration_external_ids=["integration_ext_id"],
                ...     sort=PropertySort(order="asc")
                ... )

        """
        self._warning.warn()
        routines_filter = SimulatorRoutinesFilter(
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
        )
        self._warning.warn()
        return self._list(
            limit=limit,
            method="POST",
            url_path="/simulators/routines/list",
            resource_cls=SimulatorRoutine,
            list_cls=SimulatorRoutineList,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            filter=routines_filter.dump(),
        )

    def run(
        self,
        routine_external_id: str,
        inputs: Sequence[SimulationInputOverride] | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: Literal["Debug", "Information", "Warning", "Error"] | None = None,
        wait: bool = True,
        timeout: float = 60,
    ) -> SimulationRun:
        """`Run a simulation <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>``
        Args:
            routine_external_id (str): External id of the simulator routine
            inputs (Sequence[SimulationInputOverride] | None): List of input overrides
            run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
            queue (bool | None): Queue the simulation run when connector is down.
            log_severity (Literal['Debug', 'Information', 'Warning', 'Error'] | None): Override the minimum severity level for the simulation run logs. If not provided, the minimum severity is read from the connector logger configuration.
            wait (bool): Wait until the simulation run is finished. Defaults to True.
            timeout (float): Timeout in seconds for waiting for the simulation run to finish. Defaults to 60 seconds.
        Returns:
            SimulationRun: Created simulation run
        Examples:
            Create new simulation run:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> run = client.simulators.routines.run(routine_external_id="routine1", log_severity="Debug")
        """
        self._warning.warn()
        run_object = SimulationRunWrite(
            routine_external_id=routine_external_id,
            inputs=list(inputs) if inputs is not None else None,
            run_time=run_time,
            queue=queue,
            log_severity=log_severity,
        )

        simulation_run = self._cognite_client.simulators.runs.create(run_object)

        if wait:
            simulation_run.wait(timeout)
        return simulation_run
