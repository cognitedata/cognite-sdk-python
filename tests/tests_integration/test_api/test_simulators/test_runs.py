import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.runs import (
    SimulationRunWrite,
)


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_routine_revisions")
class TestSimulatorRuns:
    def test_create_run(
        self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names
    ) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        runs = [
            SimulationRunWrite(
                run_type="external",
                routine_external_id=routine_external_id,
            )
        ]
        created_runs = cognite_client.simulators.runs.create(runs)
        assert len(created_runs) == 1
        assert created_runs[0].run_type == "external"
        assert created_runs[0].routine_external_id == routine_external_id
        assert created_runs[0].status == "ready"

    def test_list_runs(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        runs = cognite_client.simulators.runs.list()
        assert len(runs) > 0

        list_by_routine = cognite_client.simulators.runs.list(routine_external_ids=[routine_external_id])
        assert all(run.routine_external_id == routine_external_id for run in list_by_routine)

    def test_list_runs_by_status(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        status = ["running", "success", "failure"]
        for _ in range(3):
            created_runs = cognite_client.simulators.runs.create(
                [
                    SimulationRunWrite(
                        run_type="external",
                        routine_external_id=routine_external_id,
                    )
                ]
            )
            assert created_runs[0].routine_external_id == routine_external_id
            run_id = created_runs[0].id
            status_to_be_set = status.pop(0)

            cognite_client.simulators._post(
                "/simulators/run/callback",
                json={
                    "items": [
                        {
                            "id": run_id,
                            "status": status_to_be_set,
                        }
                    ]
                },
            )

            list_runs_status_filter = cognite_client.simulators.runs.list(
                status=status_to_be_set, routine_external_ids=[routine_external_id]
            )
            assert list_runs_status_filter[0].status == status_to_be_set
            assert list_runs_status_filter[0].id == run_id
