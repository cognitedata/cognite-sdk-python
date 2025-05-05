import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.runs import SimulationRunWrite


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_routine_revisions")
class TestSimulatorRuns:
    def test_list_filtering(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        runs_filtered_by_status = []
        for current_status in ["running", "success", "failure"]:
            created_run = cognite_client.simulators.run(routine_external_id=routine_external_id)

            cognite_client.simulators._post(
                "/simulators/run/callback",
                json={
                    "items": [
                        {
                            "id": created_run.id,
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

    def test_retrieve_run(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        created_run = cognite_client.simulators.run(routine_external_id=routine_external_id)
        retrieved_run = cognite_client.simulators.runs.retrieve(id=created_run.id)
        assert created_run.id == retrieved_run.id

    def test_create_run(
        self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names
    ) -> None:
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        created_run = cognite_client.simulators.run(routine_external_id=routine_external_id)
        assert created_run.routine_external_id == routine_external_id
        assert created_run.id is not None
