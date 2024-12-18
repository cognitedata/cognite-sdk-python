from __future__ import annotations

import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import (
    resource_names,
    simulator,
    simulator_integration,
    simulator_model,
    simulator_model_revision,
)


@pytest.fixture(scope="class")
def seed_resource_names() -> dict[str, str]:
    resource_names["simulator_model_external_id"] += random_string()
    resource_names["simulator_model_revision_external_id"] += random_string()
    return resource_names


@pytest.fixture
def seed_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    simulators = cognite_client.simulators.list()
    simulator_exists = len(list(filter(lambda x: x.external_id == simulator_external_id, simulators))) > 0
    if not simulator_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators",
            json={"items": [simulator]},
        )


@pytest.fixture
def seed_simulator_integration(cognite_client: CogniteClient, seed_simulator, seed_resource_names) -> None:
    try:
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations",
            json={"items": [simulator_integration]},
        )
    except CogniteAPIError:
        simulator_integrations = cognite_client.simulators.integrations.list()
        integration_id = next(
            filter(
                lambda x: x.external_id == simulator_integration["externalId"],
                simulator_integrations,
            )
        ).id
        # update hearbeat instead
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations/update",
            json={"items": [{"id": integration_id, "update": {"heartbeat": {"set": int(time.time() * 1000)}}}]},
        )


@pytest.fixture
def seed_simulator_models(cognite_client: CogniteClient, seed_simulator_integration, seed_resource_names) -> None:
    model_unique_external_id = seed_resource_names["simulator_model_external_id"]
    models = cognite_client.simulators.models.list()
    model_exists = len(list(filter(lambda x: x.external_id == model_unique_external_id, models))) > 0

    if not model_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/models",
            json={
                "items": [{**simulator_model, "externalId": model_unique_external_id}]
            },  # Post actual simulator models here
        )


@pytest.fixture
def seed_simulator_model_revisions(cognite_client: CogniteClient, seed_simulator_models, seed_file) -> None:
    model_unique_external_id = resource_names["simulator_model_external_id"]
    model_revision_unique_external_id = resource_names["simulator_model_revision_external_id"]
    model_revisions = cognite_client.simulators.models.revisions.list(
        filter=SimulatorModelRevisionsFilter(model_external_ids=[model_unique_external_id])
    )
    model_revision_not_exists = (
        len(list(filter(lambda x: x.external_id == model_revision_unique_external_id, model_revisions))) == 0
    )

    if model_revision_not_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/models/revisions",
            json={
                "items": [
                    {
                        **simulator_model_revision,
                        "fileId": seed_file.id,
                        "modelExternalId": model_unique_external_id,
                        "externalId": model_revision_unique_external_id,
                    }
                ]
            },
        )
