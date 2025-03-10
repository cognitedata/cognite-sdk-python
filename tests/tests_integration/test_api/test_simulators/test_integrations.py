import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import simulator_integration
from tests.tests_integration.test_api.test_simulators.utils import update_logs


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator_integration")
class TestSimulatorIntegrations:
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations_asdfdsa(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        for integration in cognite_client.simulators.integrations(filter=SimulatorIntegrationFilter(active=True)):
            assert integration.active is True

        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(active=True)
        )
        filtered_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(simulator_external_ids=[seed_resource_names["simulator_external_id"]])
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
        timestamp = int(time.time() * 1000)
        update_logs(
            cognite_client,
            item.log_id,
            [{"timestamp": timestamp, "message": "Testing logs update for simulator integration", "severity": "Debug"}],
        )
        log = cognite_client.simulators.logs.retrieve(id=item.log_id)
        assert log is not None
        assert log.data is not None
        assert log.data[0].timestamp == timestamp
        assert log.data[0].message == "Testing logs update for simulator integration"
        assert log.data[0].severity == "Debug"

    def test_delete_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        simulator_integration["externalId"] = random_string(50)
        simulator_integration["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]

        cognite_client.simulators._post("/simulators/integrations", json={"items": [simulator_integration]})

        all_integrations = cognite_client.simulators.integrations.list(limit=None)
        assert all_integrations.get(external_id=simulator_integration["externalId"]) is not None

        cognite_client.simulators.integrations.delete(external_id=simulator_integration["externalId"])

        all_integrations = cognite_client.simulators.integrations.list(limit=None)
        assert all_integrations.get(external_id=simulator_integration["externalId"]) is None
