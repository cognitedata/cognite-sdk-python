from __future__ import annotations

import time
from itertools import pairwise
from random import randint

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import TimestampRange
from cognite.client.data_classes.simulators import (
    SimulationInput,
    SimulationOutput,
    SimulationRun,
    SimulationRunsSort,
    SimulationRunWrite,
    SimulationValueUnitName,
)
from cognite.client.utils._async_helpers import run_sync
from tests.tests_integration.test_api.test_simulators.seed.data import RESOURCES, ResourceNames


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_routine_revisions")
class TestSimulatorRuns:
    def test_list_filtering(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id
        runs_filtered_by_status = []
        for current_status in ["running", "success", "failure"]:
            created_runs = cognite_client.simulators.runs.create([SimulationRunWrite(routine_external_id)])

            run_sync(
                async_client.simulators._post(
                    "/simulators/run/callback", json={"items": [{"id": created_runs[0].id, "status": current_status}]}
                )
            )
            filter_by_status = cognite_client.simulators.runs.list(
                status=current_status, routine_external_ids=[routine_external_id]
            )
            runs_filtered_by_status.append(filter_by_status[0].dump())
        filter_by_routine = cognite_client.simulators.runs.list(routine_external_ids=[routine_external_id]).dump()
        assert len(filter_by_routine) >= len(runs_filtered_by_status)
        for run in runs_filtered_by_status:
            assert run in filter_by_routine

    def test_run_with_wait_and_retrieve(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id

        start_time = time.time()
        start_time_ts = int(start_time * 1000)
        run_time = randint(start_time_ts - 3600 * 24 * 1000, start_time_ts)
        # Start the run, but don't wait for it:
        created_run = cognite_client.simulators.routines.run(
            routine_external_id=routine_external_id, run_time=run_time, wait=False
        )
        assert created_run.status != "success"

        run_to_update: SimulationRun | None = None

        # Step 1: Wait for the run to be created
        while run_to_update is None and time.time() - start_time < 30:
            runs_to_update_res = cognite_client.simulators.runs.list(
                routine_external_ids=[routine_external_id],
                status="ready",
                limit=10,
            )
            runs_to_update = [
                run for run in runs_to_update_res if run.run_time == run_time
            ]  # prevents flaky tests if multiple runs are created in parallel
            if len(runs_to_update) == 1:
                run_to_update = runs_to_update[0]
            else:
                time.sleep(3)

        assert run_to_update is not None

        run_sync(
            async_client.simulators._post(
                "/simulators/run/callback", json={"items": [{"id": run_to_update.id, "status": "success"}]}
            )
        )
        created_run.wait(timeout=1)
        assert created_run.status == "success"

        retrieved_run = cognite_client.simulators.runs.retrieve(ids=created_run.id)
        assert retrieved_run is not None
        assert created_run.id == retrieved_run.id

        logs_res = retrieved_run.get_logs()
        logs_res2 = cognite_client.simulators.logs.retrieve(ids=created_run.log_id)

        assert logs_res is not None
        assert logs_res2 is not None
        assert logs_res.dump() == logs_res2.dump()

        data_res = retrieved_run.get_data()
        data_res2 = cognite_client.simulators.runs.list_run_data(run_id=created_run.id)[0]
        assert data_res is not None
        assert data_res.dump() == data_res2.dump()

    @pytest.mark.parametrize(
        "simulation_run_item",
        [
            (
                SimulationRunWrite(
                    routine_revision_external_id=f"{RESOURCES.simulator_routine_external_id}_v1",
                    model_revision_external_id=RESOURCES.simulator_model_revision_external_id,
                )
            ),
            (
                SimulationRunWrite(
                    routine_external_id=RESOURCES.simulator_routine_external_id,
                )
            ),
        ],
        ids=[
            "with_routine_revision_and_model_revision",
            "with_routine_only",
        ],
    )
    def test_create_run(
        self,
        cognite_client: CogniteClient,
        simulation_run_item: SimulationRunWrite,
    ) -> None:
        created_runs = cognite_client.simulators.runs.create([simulation_run_item])
        is_run_by_revision = (
            simulation_run_item.routine_revision_external_id is not None
            and simulation_run_item.model_revision_external_id is not None
        )
        if is_run_by_revision:
            assert created_runs[0].routine_revision_external_id == simulation_run_item.routine_revision_external_id
            assert created_runs[0].model_revision_external_id == simulation_run_item.model_revision_external_id
        else:
            assert created_runs[0].routine_external_id == simulation_run_item.routine_external_id
        assert len(created_runs) == 1
        assert created_runs[0].id is not None

    def test_list_filtering_timestamp_ranges(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id

        # Create a run to ensure we have something to filter
        created_run = cognite_client.simulators.runs.create(
            [
                SimulationRunWrite(
                    run_type="external",
                    routine_external_id=routine_external_id,
                )
            ]
        )
        created_run_time = created_run[0].created_time

        # Set a specific simulation time for our test run
        test_simulation_time = int(time.time() * 1000) - 5000  # 5 seconds ago
        run_sync(
            async_client.simulators._post(
                "/simulators/run/callback",
                json={
                    "items": [
                        {
                            "id": created_run[0].id,
                            "status": "success",
                            "simulationTime": test_simulation_time,
                        }
                    ]
                },
            )
        )

        # Test filtering by created_time and simulation_time - use a narrow range around the created run
        runs_filtered_both = cognite_client.simulators.runs.list(
            routine_external_ids=[routine_external_id],
            created_time=TimestampRange(min=created_run_time - 1000, max=created_run_time + 1000),
            simulation_time=TimestampRange(min=test_simulation_time - 1000, max=test_simulation_time + 1000),
            limit=100,
        )
        assert len(runs_filtered_both) > 0
        assert created_run[0].id in [run.id for run in runs_filtered_both]

        # Test with empty time range for created_time and simulation_time (should return no results)
        current_time_ms = int(time.time() * 1000)
        runs_empty_range = cognite_client.simulators.runs.list(
            routine_external_ids=[routine_external_id],
            created_time=TimestampRange(min=current_time_ms + 20000, max=current_time_ms + 30000),
            simulation_time=TimestampRange(min=current_time_ms + 20000, max=current_time_ms + 30000),
            limit=5,
        )
        assert len(runs_empty_range) == 0

    def test_list_sort(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id

        # Test list with sort by createdTime
        runs_asc = cognite_client.simulators.runs.list(
            routine_external_ids=[routine_external_id],
            sort=SimulationRunsSort(order="asc", property="created_time"),
            limit=5,
        )

        if len(runs_asc) > 1:
            for prev, curr in pairwise(runs_asc):
                assert curr.created_time >= prev.created_time

        # Test iterator with sort by createdTime descending
        runs_iter = list(
            cognite_client.simulators.runs(
                routine_external_ids=[routine_external_id],
                sort=SimulationRunsSort(order="desc", property="created_time"),
                limit=3,
            )
        )

        if len(runs_iter) > 1:
            for prev, curr in pairwise(runs_iter):
                assert curr.created_time <= prev.created_time

    def test_list_run_data(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id
        created_run = cognite_client.simulators.runs.create(
            [
                SimulationRunWrite(
                    run_type="external",
                    routine_external_id=routine_external_id,
                )
            ]
        )

        outputs = [
            SimulationOutput(
                reference_id="ST",
                simulator_object_reference={"address": "test_out"},
                value=18.5,
                value_type="DOUBLE",
                unit=SimulationValueUnitName(name="C"),
            ),
        ]

        inputs = [
            SimulationInput(
                reference_id="CWT",
                value=11.0,
                value_type="DOUBLE",
                overridden=True,
                unit=SimulationValueUnitName(name="C"),
            ),
            SimulationInput(
                reference_id="CWP",
                overridden=True,
                value=[5.0],
                value_type="DOUBLE_ARRAY",
                unit=SimulationValueUnitName(name="bar"),
            ),
        ]

        run_sync(
            async_client.simulators._post(
                "/simulators/run/callback",
                json={
                    "items": [
                        {
                            "id": created_run[0].id,
                            "status": "success",
                            "outputs": outputs,
                            "inputs": inputs,
                        }
                    ]
                },
            )
        )

        run_data_res = cognite_client.simulators.runs.list_run_data(
            run_id=created_run[0].id,
        )

        assert len(run_data_res) == 1
        assert run_data_res[0].run_id == created_run[0].id

        def sort_by_ref_id(x: SimulationInput | SimulationOutput) -> str:
            return x.reference_id

        assert sorted(run_data_res[0].inputs, key=sort_by_ref_id) == sorted(inputs, key=sort_by_ref_id)
        assert sorted(run_data_res[0].outputs, key=sort_by_ref_id) == sorted(outputs, key=sort_by_ref_id)
