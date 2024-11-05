from cognite.client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter


class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0


class TestSimulatorIntegrations:
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.list_integrations(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient) -> None:
        all_integrations = cognite_client.simulators.list_integrations()
        active_integrations = cognite_client.simulators.list_integrations(
            filter=SimulatorIntegrationFilter(active=True)
        )
        dwsim_integrations = cognite_client.simulators.list_integrations(
            filter=SimulatorIntegrationFilter(simulator_external_ids=["DWSIM"])
        )

        assert len(active_integrations) > 0
        assert len(all_integrations) != len(active_integrations)
        assert len(dwsim_integrations) > 0
        assert len(all_integrations) != len(dwsim_integrations)


class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient) -> None:
        models = cognite_client.simulators.list_models(limit=5)

        assert len(models) > 0

    def test_list_model_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.list_model_revisions(limit=5)

        assert len(revisions) > 0


class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.list_routines(limit=5)
        assert len(routines) > 0

    def test_list_routine_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.list_routine_revisions(limit=5)
        assert revisions[0].configuration is not None
        assert revisions[0].script is not None
        assert len(revisions) > 0
