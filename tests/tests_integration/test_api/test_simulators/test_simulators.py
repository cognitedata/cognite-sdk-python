import pytest

from cognite.client._cognite_client import CogniteClient
from tests.tests_integration.test_api.test_simulators.seed.data import resource_names, simulator


@pytest.fixture(scope="class")
def seed_resource_names() -> dict[str, str]:
    return resource_names


@pytest.fixture
def seed_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    simulators = cognite_client.simulators.list()
    simulator_exists = len(list(filter(lambda x: x.external_id == simulator_external_id, simulators))) > 0
    if not simulator_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators",
            json={"items": [simulator]},
        )


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator")
class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0
