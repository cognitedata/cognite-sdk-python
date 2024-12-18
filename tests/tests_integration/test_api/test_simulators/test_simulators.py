import pytest

from cognite.client._cognite_client import CogniteClient


@pytest.mark.usefixtures("seed_simulator")
class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0
