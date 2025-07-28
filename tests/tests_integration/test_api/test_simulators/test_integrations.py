import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import simulator_integration


def cleanup_inactive_integrations(cognite_client: CogniteClient, external_id_to_keep: str) -> None:
    """Cleanup simulator integrations except the one with the given external_id."""
    try:
        all_integrations = cognite_client.simulators.integrations.list(limit=None)
        integrations_to_delete = [
            item for item in all_integrations if not item.active and item.external_id != external_id_to_keep
        ]
        if len(integrations_to_delete) > 0:
            cognite_client.simulators.integrations.delete(ids=[item.id for item in integrations_to_delete])
    except CogniteAPIError:
        pass


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_integration")
class TestSimulatorIntegrations:
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        for integration in cognite_client.simulators.integrations(filter=SimulatorIntegrationFilter(active=True)):
            assert integration.active is True

        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(active=True)
        filtered_integrations = cognite_client.simulators.integrations.list(
            simulator_external_ids=[seed_resource_names["simulator_external_id"]],
        )
        assert len(all_integrations) > 0
        assert len(active_integrations) > 0
        assert len(filtered_integrations) > 0

        item = filtered_integrations.get(external_id=seed_resource_names["simulator_integration_external_id"])
        assert item is not None
        # assert item.data_set_id == seed_resource_names["simulator_test_data_set_id"]
        assert item.active is True
        assert item.created_time is not None
        assert item.last_updated_time is not None
        assert item.log_id is not None
        log = cognite_client.simulators.logs.retrieve(id=item.log_id)
        assert log is not None
        assert log.data is not None
        assert log.data[0].timestamp is not None
        assert log.data[0].timestamp > int(time.time() * 1000) - 10000  # updated less than 10 seconds ago
        assert log.data[0].message == "Testing logs update for simulator integration"
        assert log.data[0].severity == "Debug"

    def test_delete_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        test_integration = simulator_integration.copy()
        test_integration["heartbeat"] = int(time.time() * 1000)
        test_integration["externalId"] = random_string(50)
        test_integration["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]

        try:
            cognite_client.simulators._post("/simulators/integrations", json={"items": [test_integration]})

            all_integrations = cognite_client.simulators.integrations.list(limit=None)
            assert all_integrations.get(external_id=test_integration["externalId"]) is not None

            cognite_client.simulators.integrations.delete(external_ids=test_integration["externalId"])

            all_integrations = cognite_client.simulators.integrations.list(limit=None)
            assert all_integrations.get(external_id=test_integration["externalId"]) is None
        finally:
            cleanup_inactive_integrations(cognite_client, seed_resource_names["simulator_integration_external_id"])
