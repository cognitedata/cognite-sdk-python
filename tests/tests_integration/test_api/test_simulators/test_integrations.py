import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import simulator_integration


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

    def test_delete_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        simulator_integration["externalId"] = random_string(50)

        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations",
            json={"items": [simulator_integration]},
        )

        all_integrations = cognite_client.simulators.integrations.list()
        assert len(all_integrations) > 0
        assert any(
            map(
                lambda x: x.external_id == simulator_integration["externalId"],
                all_integrations,
            )
        )

        cognite_client.simulators.integrations.delete(external_id=simulator_integration["externalId"])

        all_integrations = cognite_client.simulators.integrations.list()
        assert not any(
            map(
                lambda x: x.external_id == simulator_integration["externalId"],
                all_integrations,
            )
        )
