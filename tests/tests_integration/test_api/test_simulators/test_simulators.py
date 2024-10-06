from cognite.client import CogniteClient


class TestSimulators:
    def test_list(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0
