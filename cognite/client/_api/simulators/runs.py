from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulatorRunsFilter
from cognite.client.data_classes.simulators.runs import (
    SimulationRun,
    SimulationRunWrite,
    SimulatorRunDataList,
    SimulatorRunList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SimulatorRunsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/runs"
    _RESOURCE_PATH_RUN = "/simulators/run"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 1
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability",
            sdk_maturity="alpha",
            feature_name="Simulators",
        )

    def __iter__(self) -> Iterator[SimulationRun]:
        """Iterate over simulation runs

        Fetches simulation runs as they are iterated over, so you keep a limited number of simulation runs in memory.

        Returns:
            Iterator[SimulationRun]: yields simulation runs one by one.
        """
        return self()

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
    ) -> Iterator[SimulatorRunList]: ...

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
    ) -> Iterator[SimulationRun] | Iterator[SimulatorRunList]:
        """Iterate over simulation runs

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

        Returns:
            Iterator[SimulationRun] | Iterator[SimulatorRunList]: yields Simulation Run one by one if chunk is not specified, else SimulatorRunsList objects.
        """

        filter_runs = SimulatorRunsFilter(
            status=status,
            run_type=run_type,
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
            simulator_external_ids=simulator_external_ids,
            routine_external_ids=routine_external_ids,
            routine_revision_external_ids=routine_revision_external_ids,
            model_revision_external_ids=model_revision_external_ids,
        )

        return self._list_generator(
            list_cls=SimulatorRunList,
            resource_cls=SimulationRun,
            method="POST",
            filter=filter_runs.dump(),
            chunk_size=chunk_size,
            limit=limit,
        )

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
    ) -> SimulatorRunList:
        """`Filter simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/filter_simulation_runs_simulators_runs_list_post>`_

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

        Returns:
            SimulatorRunList: List of simulation runs

        Examples:
            List simulation runs:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.runs.list()

            Filter runs by status and simulator external ids:
                >>> res = client.simulators.runs.list(
                ...     simulator_external_ids=["PROSPER", "DWSIM"],
                ...     status="success"
                ... )
        """

        filter_runs = SimulatorRunsFilter(
            status=status,
            run_type=run_type,
            model_external_ids=model_external_ids,
            simulator_integration_external_ids=simulator_integration_external_ids,
            simulator_external_ids=simulator_external_ids,
            routine_external_ids=routine_external_ids,
            routine_revision_external_ids=routine_revision_external_ids,
            model_revision_external_ids=model_revision_external_ids,
        )
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulationRun,
            list_cls=SimulatorRunList,
            filter=filter_runs.dump(),
        )

    @overload
    def retrieve(self, ids: int) -> SimulationRun | None: ...

    @overload
    def retrieve(
        self,
        ids: Sequence[int],
    ) -> SimulatorRunList | None: ...

    def retrieve(
        self,
        ids: int | Sequence[int],
    ) -> SimulationRun | SimulatorRunList | None:
        """`Retrieve a simulation run by ID <https://api-docs.cognite.com/20230101/tag/Simulation-Runs/operation/simulation_by_id_simulators_runs_byids_post>`_

        Args:
            ids (int | Sequence[int]): The ID(s) of the simulation run(s) to retrieve.

        Returns:
            SimulationRun | SimulatorRunList | None: The simulation run(s) with the given ID(s)

        Examples:
            Retrieve a single simulation run by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> run = client.simulators.runs.retrieve(ids=2)
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=ids)
        return self._retrieve_multiple(
            resource_cls=SimulationRun,
            list_cls=SimulatorRunList,
            identifiers=identifiers,
            resource_path=self._RESOURCE_PATH,
        )

    @overload
    def create(self, run: SimulationRunWrite) -> SimulationRun: ...

    @overload
    def create(self, run: Sequence[SimulationRunWrite]) -> SimulatorRunList: ...

    def create(self, run: SimulationRunWrite | Sequence[SimulationRunWrite]) -> SimulationRun | SimulatorRunList:
        """`Create simulation runs <https://developer.cognite.com/api#tag/Simulation-Runs/operation/run_simulation_simulators_run_post>`_

        Args:
            run (SimulationRunWrite | Sequence[SimulationRunWrite]): The simulation run(s) to execute.

        Returns:
            SimulationRun | SimulatorRunList: Created simulation run(s)

        Examples:
            Create new simulation run:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.runs import SimulationRunWrite
                >>> client = CogniteClient()
                >>> run = [
                ...     SimulationRunWrite(
                ...         routine_external_id="routine1",
                ...         log_severity="Debug",
                ...         run_type="external",
                ...     ),
                ... ]
                >>> res = client.simulators.runs.create(run)
        """
        assert_type(run, "simulation_run", [SimulationRunWrite, Sequence])

        return self._create_multiple(
            list_cls=SimulatorRunList,
            resource_cls=SimulationRun,
            items=run,
            input_resource_cls=SimulationRunWrite,
            resource_path=self._RESOURCE_PATH_RUN,
        )

    def list_run_data(
        self,
        run_id: int,
    ) -> SimulatorRunDataList:
        """`Get simulation run data <https://developer.cognite.com/api#tag/Simulation-Runs/operation/simulation_data_by_run_id_simulators_runs_data_list_post>`_

        Retrieve data associated with a simulation run by ID.

        Args:
            run_id (int): Simulation run id.

        Returns:
            SimulatorRunDataList: List of simulation run data

        Examples:
            Get simulation run data by run id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.runs.list_run_data(run_id=12345)

            Get simulation run data directly on a simulation run object:
                >>> run = client.simulators.runs.retrieve(ids=2)
                >>> res = run.get_data()
        """
        self._warning.warn()

        req = self._post(
            url_path=f"{self._RESOURCE_PATH}/data/list",
            json={"items": [{"runId": run_id}]},
        )

        items = req.json().get("items", [])
        return SimulatorRunDataList._load(items, cognite_client=self._cognite_client)
