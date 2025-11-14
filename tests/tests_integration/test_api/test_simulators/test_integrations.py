from __future__ import annotations

import time

import pytest

from cognite.client import CogniteClient
from tests.tests_integration.test_api.test_simulators.seed.data import ResourceNames


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_integration")
class TestSimulatorIntegrations:
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        for integration in cognite_client.simulators.integrations(active=True):
            assert integration.active is True

        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(active=True)
        filtered_integrations = cognite_client.simulators.integrations.list(
            simulator_external_ids=[seed_resource_names.simulator_external_id],
        )
        assert len(all_integrations) > 0
        assert len(active_integrations) > 0
        assert len(filtered_integrations) > 0

        item = filtered_integrations.get(external_id=seed_resource_names.simulator_integration_external_id)
        assert item is not None
        assert item.active is True
        assert item.created_time is not None
        assert item.last_updated_time is not None
        assert item.log_id is not None
        log = cognite_client.simulators.logs.retrieve(ids=item.log_id)
        assert log is not None
        assert log.data is not None
        assert log.data[0].timestamp is not None
        assert log.data[0].message == "Testing logs update for simulator integration"
        assert log.data[0].timestamp > int(time.time() * 1000) - 30000  # updated less than 30 seconds ago
        assert log.data[0].severity == "Debug"
