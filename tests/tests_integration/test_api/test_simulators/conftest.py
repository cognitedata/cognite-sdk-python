from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.data_sets import DataSetWrite
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.models import SimulatorModelWrite
from cognite.client.data_classes.simulators.routine_revisions import SimulatorRoutineRevisionWrite
from cognite.client.data_classes.simulators.routines import SimulatorRoutineWrite
from tests.tests_integration.test_api.test_simulators.seed.data import (
    resource_names,
    simulator,
    simulator_integration,
    simulator_model,
    simulator_routine,
    simulator_routine_revision_obj,
)
from tests.tests_integration.test_api.test_simulators.utils import update_logs

SEED_DIR = Path(__file__).resolve(strict=True).parent / "seed"


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


@pytest.fixture(scope="session")
def seed_file(cognite_client: CogniteClient, seed_resource_names: dict[str, str]) -> Iterator[FileMetadata | None]:
    data_set_id = seed_resource_names["simulator_test_data_set_id"]
    file = cognite_client.files.retrieve(external_id=seed_resource_names["simulator_model_file_external_id"])
    if not file:
        file = cognite_client.files.upload(
            path=SEED_DIR / "ShowerMixer.txt",
            external_id=seed_resource_names["simulator_model_file_external_id"],
            name="ShowerMixer.txt",
            data_set_id=data_set_id,
        )
    yield file


@pytest.fixture(scope="session")
def seed_simulator(cognite_client: CogniteClient, seed_resource_names: dict[str, str]) -> Iterator[None]:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    simulators = cognite_client.simulators.list(limit=None)
    if not simulators.get(external_id=simulator_external_id):
        cognite_client.simulators._post("/simulators", json={"items": [simulator]})


@pytest.fixture(scope="session")
def seed_simulator_integration(
    cognite_client: CogniteClient, seed_simulator: None, seed_resource_names: dict[str, str]
) -> Iterator[None]:
    log_id = None
    timestamp = int(time.time() * 1000)
    simulator_integrations = cognite_client.simulators.integrations.list(limit=None)
    if not simulator_integrations.get(external_id=simulator_integration["externalId"]):
        simulator_integration["heartbeat"] = timestamp
        simulator_integration["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]
        res = cognite_client.simulators._post(
            "/simulators/integrations",
            json={"items": [simulator_integration]},
        )
        log_id = res.json()["items"][0]["logId"]
    else:
        integration = simulator_integrations.get(external_id=simulator_integration["externalId"])
        if integration is not None:
            log_id = integration.log_id
        cognite_client.simulators.integrations._post(
            "/simulators/integrations/update",
            json={"items": [{"id": integration.id, "update": {"heartbeat": {"set": timestamp}}}]},
        )

    if log_id:
        update_logs(
            cognite_client,
            log_id,
            [{"timestamp": timestamp, "message": "Testing logs update for simulator integration", "severity": "Debug"}],
        )


@pytest.fixture(scope="session")
def seed_simulator_models(
    cognite_client: CogniteClient, seed_simulator_integration: None, seed_resource_names: dict[str, str]
) -> Iterator[dict[str, Any]]:
    model_unique_external_id = seed_resource_names["simulator_model_external_id"]
    models = cognite_client.simulators.models.list(limit=None)
    model_exists = models.get(external_id=model_unique_external_id)

    if not model_exists:
        simulator_model["dataSetId"] = seed_resource_names["simulator_test_data_set_id"]
        simulator_model["externalId"] = model_unique_external_id

        model = SimulatorModelWrite._load(
            {
                "externalId": simulator_model["externalId"],
                "simulatorExternalId": simulator_model["simulatorExternalId"],
                "dataSetId": simulator_model["dataSetId"],
                "name": simulator_model["name"],
                "type": simulator_model["type"],
                "description": simulator_model["description"],
            }
        )
        cognite_client.simulators.models.create(model)

    yield simulator_model

    cognite_client.simulators.models.delete(external_id=model_unique_external_id)


@pytest.fixture(scope="session")
def seed_simulator_model_revisions(cognite_client: CogniteClient, seed_simulator_models, seed_file) -> None:
    model_unique_external_id = resource_names["simulator_model_external_id"]
    model_revision_unique_external_id = resource_names["simulator_model_revision_external_id"]
    model_revisions = cognite_client.simulators.models.revisions.list(
        model_external_ids=[model_unique_external_id],
    )

    cognite_client.simulators._post(
        "/simulators/models/delete",
        json={
            "items": [
                {
                    "externalId": model_unique_external_id,
                }
            ]
        },
    )

    second_version_ext_id = f"{model_revision_unique_external_id}_1"

    if not model_revisions.get(external_id=second_version_ext_id):
        cognite_client.simulators._post(
            "/simulators/models/revisions",
            json={
                "items": [
                    {
                        "description": "test sim model revision description",
                        "fileId": seed_file.id,
                        "modelExternalId": model_unique_external_id,
                        "externalId": second_version_ext_id,
                    }
                ]
            },
        )

    if not model_revisions.get(external_id=model_revision_unique_external_id):
        cognite_client.simulators._post(
            "/simulators/models/revisions",
            json={
                "items": [
                    {
                        "description": "test sim model revision description",
                        "fileId": seed_file.id,
                        "modelExternalId": model_unique_external_id,
                        "externalId": model_revision_unique_external_id,
                    }
                ]
            },
        )


@pytest.fixture(scope="session")
def seed_simulator_routines(cognite_client: CogniteClient, seed_simulator_model_revisions):
    model_unique_external_id = resource_names["simulator_model_external_id"]
    simulator_routine_unique_external_id = resource_names["simulator_routine_external_id"]

    routines = cognite_client.simulators.routines.create(
        [
            SimulatorRoutineWrite.load(
                {
                    **simulator_routine,
                    "modelExternalId": model_unique_external_id,
                    "externalId": simulator_routine_unique_external_id,
                }
            ),
            SimulatorRoutineWrite.load(
                {
                    **simulator_routine,
                    "modelExternalId": model_unique_external_id,
                    "externalId": simulator_routine_unique_external_id + "_1",
                }
            ),
        ]
    )

    yield routines

    cognite_client.simulators.routines.delete(
        external_ids=[simulator_routine_unique_external_id, simulator_routine_unique_external_id + "_1"]
    )


def seed_simulator_routine_revision(
    cognite_client: CogniteClient, routine_external_id: str, version: str
) -> dict[str, Any]:
    routine_revs = cognite_client.simulators.routines.revisions.list(routine_external_ids=[routine_external_id])
    rev_external_id = f"{routine_external_id}_{version}"
    routine_rev_exists = routine_revs.get(external_id=rev_external_id)

    revision = {**simulator_routine_revision_obj, "externalId": rev_external_id}

    if not routine_rev_exists:
        cognite_client.simulators.routines.revisions.create(SimulatorRoutineRevisionWrite.load(revision))

    return revision


@pytest.fixture(scope="session")
def seed_simulator_routine_revisions(cognite_client: CogniteClient, seed_simulator_routines):
    simulator_routine_external_id = resource_names["simulator_routine_external_id"]

    rev1 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v1")
    rev2 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v2")

    yield rev1, rev2
