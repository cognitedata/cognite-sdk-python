import asyncio
import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.runs import (
    SimulationInput,
    SimulationOutput,
    SimulationRun,
    SimulationRunWrite,
    SimulationValueUnitName,
)


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_routine_revisions")
class TestSimulatorRuns:
    def test_list_filtering(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        runs_filtered_by_status = []
        for current_status in ["running", "success", "failure"]:
            created_runs = cognite_client.simulators.runs.create(
                [
                    SimulationRunWrite(
                        run_type="external",
                        routine_external_id=routine_external_id,
                    )
                ]
            )

            cognite_client.simulators._post(
                "/simulators/run/callback",
                json={
                    "items": [
                        {
                            "id": created_runs[0].id,
                            "status": current_status,
                        }
                    ]
                },
            )

            filter_by_status = cognite_client.simulators.runs.list(
                status=current_status, routine_external_ids=[routine_external_id]
            )
            runs_filtered_by_status.append(filter_by_status[0].dump())
        filter_by_routine = cognite_client.simulators.runs.list(routine_external_ids=[routine_external_id]).dump()
        assert filter_by_routine == runs_filtered_by_status
        assert sorted(runs_filtered_by_status, key=lambda x: x["id"]) == sorted(
            filter_by_routine, key=lambda x: x["id"]
        )

    @pytest.mark.asyncio
    async def test_run_with_wait_and_retrieve(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]

        run_task = asyncio.create_task(
            asyncio.to_thread(lambda: cognite_client.simulators.routines.run(routine_external_id=routine_external_id))
        )

        run_to_update: SimulationRun | None = None
        start_time = time.time()

        # 1. Wait for the run to be created
        # 2. Emulate it being finished by the simulator
        while run_to_update is None and time.time() - start_time < 30:
            runs_to_update = cognite_client.simulators.runs.list(
                routine_external_ids=[routine_external_id],
                status="ready",
                limit=5,
            )
            if len(runs_to_update) == 1:
                run_to_update = runs_to_update[0]
            else:
                await asyncio.sleep(1)

        assert run_to_update is not None

        cognite_client.simulators._post(
            "/simulators/run/callback",
            json={
                "items": [
                    {
                        "id": run_to_update.id,
                        "status": "success",
                    }
                ]
            },
        )

        created_run = await run_task

        retrieved_run = cognite_client.simulators.runs.retrieve(ids=created_run.id)
        assert created_run.id == retrieved_run.id

        logs_res = retrieved_run.get_logs()
        logs_res2 = cognite_client.simulators.logs.retrieve(ids=created_run.log_id)

        assert logs_res is not None
        assert logs_res.dump() == logs_res2.dump()

        data_res = retrieved_run.get_data()
        data_res2 = cognite_client.simulators.runs.list_run_data(run_id=created_run.id)[0]
        assert data_res is not None
        assert data_res.dump() == data_res2.dump()

    def test_create_run(
        self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names
    ) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        created_runs = cognite_client.simulators.runs.create(
            [
                SimulationRunWrite(
                    run_type="external",
                    routine_external_id=routine_external_id,
                )
            ]
        )
        assert len(created_runs) == 1
        assert created_runs[0].routine_external_id == routine_external_id
        assert created_runs[0].id is not None

    def test_list_run_data(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
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
            ).dump(),
        ]

        inputs = [
            SimulationInput(
                reference_id="CWT",
                value=11.0,
                value_type="DOUBLE",
                overridden=True,
                unit=SimulationValueUnitName(name="C"),
            ).dump(),
            SimulationInput(
                reference_id="CWP",
                overridden=True,
                value=[5.0],
                value_type="DOUBLE_ARRAY",
                unit=SimulationValueUnitName(name="bar"),
            ).dump(),
        ]

        cognite_client.simulators._post(
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

        get_run_data = cognite_client.simulators.runs.list_run_data(
            run_id=created_run[0].id,
        )

        assert len(get_run_data) == 1
        assert get_run_data[0].run_id == created_run[0].id
        assert get_run_data[0].inputs[0].dump() == inputs[0]
        assert get_run_data[0].outputs[0].dump() == outputs[0]
