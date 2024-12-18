import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter


class TestSimulatorIntegrations:
    @pytest.mark.usefixtures("seed_resource_names", "seed_simulator_integration")
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(active=True)
        )

        filtered_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(simulator_external_ids=[seed_resource_names["simulator_external_id"]])
        )

        assert len(all_integrations) > 0
        assert filtered_integrations[0].external_id == seed_resource_names["simulator_integration_external_id"]
        # check time difference
        assert filtered_integrations[0].active is True

        assert len(active_integrations) > 0
        assert len(filtered_integrations) > 0
