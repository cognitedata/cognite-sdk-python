from __future__ import annotations

import time

import pytest

from cognite.client._cognite_client import CogniteClient
from tests.tests_integration.test_api.test_simulators.seed.data import resource_names, simulator, simulator_integration


@pytest.fixture(scope="session")
def seed_resource_names(cognite_client: CogniteClient) -> dict[str, str]:
    seed_data_set_external_id = resource_names["simulator_test_data_set_external_id"]
    data_set = cognite_client.data_sets.retrieve(external_id=seed_data_set_external_id)
    if not data_set:
        data_set = cognite_client.data_sets.create(
            external_id=seed_data_set_external_id, name=seed_data_set_external_id
        )

    resource_names["simulator_test_data_set_id"] = data_set.id
    return resource_names.copy()


@pytest.fixture(scope="session")
def seed_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    simulators = cognite_client.simulators.list(limit=None)
    if not simulators.get(external_id=simulator_external_id):
        cognite_client.simulators._post("/simulators", json={"items": [simulator]})


@pytest.fixture(scope="session")
def seed_simulator_integration(cognite_client: CogniteClient, seed_simulator) -> None:
    # try:
    simulator_integrations = cognite_client.simulators.integrations.list(limit=None)
    if not simulator_integrations.get(external_id=simulator_integration["externalId"]):
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        cognite_client.simulators._post(
            "/simulators/integrations",
            json={"items": [simulator_integration]},
        )
    else:
        integration = simulator_integrations.get(external_id=simulator_integration["externalId"])
        # update hearbeat instead
        cognite_client.simulators.integrations._post(
            "/simulators/integrations/update",
            json={"items": [{"id": integration.id, "update": {"heartbeat": {"set": int(time.time() * 1000)}}}]},
        )
