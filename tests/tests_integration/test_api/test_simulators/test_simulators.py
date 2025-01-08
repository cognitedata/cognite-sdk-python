import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.simulators import Simulator


@pytest.mark.usefixtures("seed_simulator")
class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        for simulator in cognite_client.simulators:
            assert isinstance(simulator, Simulator)

        simulators = cognite_client.simulators.list(limit=5)
        for simulator in simulators:
            assert simulator.id is not None

        assert len(simulators) > 0
