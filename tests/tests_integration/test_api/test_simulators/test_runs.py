import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.runs import (
    SimulationRunWrite,
)


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_routine_revision")
class TestSimulatorRuns:
    # todo: base conftest on top of routine revisions PR
    def test_create_run(
        self, cognite_client: CogniteClient, seed_simulator_routine_revision, seed_resource_names
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

    def test_list_runs(self, cognite_client: CogniteClient) -> None:
        runs = cognite_client.simulators.runs.list()
        assert len(runs) > 0
