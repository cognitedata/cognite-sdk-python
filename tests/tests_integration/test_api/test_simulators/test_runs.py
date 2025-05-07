import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.runs import (
    SimulationInput,
    SimulationOutput,
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
