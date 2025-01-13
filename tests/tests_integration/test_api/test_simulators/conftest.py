from __future__ import annotations

import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.data_sets import DataSetWrite
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorRoutinesFilter
from tests.tests_integration.test_api.test_simulators.seed.data import (
    resource_names,
    simulator,
    simulator_integration,
    simulator_model,
    simulator_model_revision,
    simulator_routine,
)


@pytest.fixture(scope="session")
def seed_resource_names(cognite_client: CogniteClient) -> dict[str, str]:
    seed_data_set_external_id = resource_names["simulator_test_data_set_external_id"]
    data_set = cognite_client.data_sets.retrieve(external_id=seed_data_set_external_id)
    if not data_set:
        data_sets = cognite_client.data_sets.create(
            [DataSetWrite(external_id=seed_data_set_external_id, name=seed_data_set_external_id)]
        )
        data_set = data_sets[0]
    resource_names["simulator_test_data_set_id"] = data_set.id
    return resource_names.copy()


@pytest.fixture
def seed_file(cognite_client: CogniteClient, seed_resource_names) -> FileMetadata | None:
    # check if file already exists
    data_set_id = seed_resource_names["simulator_test_data_set_id"]
    file = cognite_client.files.retrieve(external_id=seed_resource_names["simulator_model_file_external_id"])
    if (file is None) or (file is False):
        file = cognite_client.files.upload(
            path="tests/tests_integration/test_api/test_simulators/seed/ShowerMixer.dwxmz",
            external_id=seed_resource_names["simulator_model_file_external_id"],
            name="ShowerMixer.dwxmz",
            data_set_id=data_set_id,
        )
    yield file


@pytest.fixture(scope="session")
def seed_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    simulators = cognite_client.simulators.list(limit=None)
    if not simulators.get(external_id=simulator_external_id):
        cognite_client.simulators._post("/simulators", json={"items": [simulator]})


@pytest.fixture(scope="session")
def seed_simulator_integration(cognite_client: CogniteClient, seed_simulator, seed_resource_names) -> None:
    simulator_integrations = cognite_client.simulators.integrations.list(limit=None)
    if not simulator_integrations.get(external_id=simulator_integration["externalId"]):
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        simulator_integration["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]
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


@pytest.fixture
def seed_simulator_models(cognite_client: CogniteClient, seed_simulator_integration, seed_resource_names) -> None:
    model_unique_external_id = seed_resource_names["simulator_model_external_id"]
    models = cognite_client.simulators.models.list()
    model_exists = len(list(filter(lambda x: x.external_id == model_unique_external_id, models))) > 0

    if not model_exists:
        simulator_model["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]
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


@pytest.fixture
def seed_simulator_routines(cognite_client: CogniteClient, seed_simulator_model_revisions) -> None:
    model_unique_external_id = resource_names["simulator_model_external_id"]
    simulator_routine_unique_external_id = resource_names["simulator_routine_external_id"]
    routines = cognite_client.simulators.routines.list(
        filter=SimulatorRoutinesFilter(model_external_ids=[model_unique_external_id])
    )
    routine_not_exists = (
        len(list(filter(lambda x: x.external_id == simulator_routine_unique_external_id, routines))) == 0
    )

    if routine_not_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/routines",
            json={
                "items": [
                    {
                        **simulator_routine,
                        "modelExternalId": model_unique_external_id,
                        "externalId": simulator_routine_unique_external_id,
                    }
                ]
            },
        )
