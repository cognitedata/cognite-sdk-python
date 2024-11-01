from cognite.client import CogniteClient


class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0


class TestSimulatorIntegrations:
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.list_integrations(limit=5)

        assert len(integrations) > 0


class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient) -> None:
        models = cognite_client.simulators.list_models(limit=5)

        assert len(models) > 0
