from __future__ import annotations

import copy
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
    SIMULATOR,
    SIMULATOR_INTEGRATION,
    SIMULATOR_MODEL,
    SIMULATOR_ROUTINE,
    SIMULATOR_ROUTINE_REVISION_OBJ,
    ResourceNames,
    resources,
)
from tests.tests_integration.test_api.test_simulators.utils import update_logs

SEED_DIR = Path(__file__).resolve(strict=True).parent / "seed"


@pytest.fixture(scope="session")
def seed_resource_names(cognite_client: CogniteClient) -> ResourceNames:
    seed_data_set_external_id = resources.simulator_test_data_set_external_id
    data_set = cognite_client.data_sets.retrieve(external_id=seed_data_set_external_id)
    if not data_set:
        data_sets = cognite_client.data_sets.create(
            [DataSetWrite(external_id=seed_data_set_external_id, name=seed_data_set_external_id)]
        )
        data_set = data_sets[0]
    resources.simulator_test_data_set_id = data_set.id
    return copy.copy(resources)


def upload_file(cognite_client: CogniteClient, filename: str, external_id: str, data_set_id: int) -> FileMetadata:
    file = cognite_client.files.retrieve(external_id=external_id)
    if not file:
        return cognite_client.files.upload(
            path=SEED_DIR / filename,
            external_id=external_id,
            name=filename,
            data_set_id=data_set_id,
        )

    return file


@pytest.fixture(scope="session")
def seed_model_revision_file(
    cognite_client: CogniteClient, seed_resource_names: ResourceNames
) -> Iterator[FileMetadata | None]:
    data_set_id = seed_resource_names.simulator_test_data_set_id
    file = upload_file(
        cognite_client,
        filename="ShowerMixer.txt",
        external_id=seed_resource_names.simulator_model_file_external_id,
        data_set_id=data_set_id,
    )

    yield file


@pytest.fixture(scope="session")
def seed_simulator(cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> Iterator[None]:
    simulator_external_id = seed_resource_names.simulator_external_id
    simulators = cognite_client.simulators.list(limit=None)
    seeded_simulator = simulators.get(external_id=simulator_external_id)
    fields_to_compare = ["fileExtensionTypes", "modelTypes", "modelDependencies", "stepFields", "unitQuantities"]
    seeded_simulator_dump = seeded_simulator.dump() if seeded_simulator else None

    if not seeded_simulator:
        cognite_client.simulators._post("/simulators", json={"items": [SIMULATOR]})
    # if any field in simulator is different from the current seeded simulator, update it
    elif any(seeded_simulator_dump.get(field) != SIMULATOR[field] for field in fields_to_compare if field in SIMULATOR):
        simulator_update = {
            "id": seeded_simulator.id,
            "update": {field: {"set": SIMULATOR[field]} for field in fields_to_compare},
        }
        cognite_client.simulators._post("/simulators/update", json={"items": [simulator_update]})


@pytest.fixture(scope="session")
def seed_simulator_integration(
    cognite_client: CogniteClient, seed_simulator: None, seed_resource_names: ResourceNames
) -> Iterator[None]:
    log_id = None
    timestamp = int(time.time() * 1000)
    simulator_integrations = cognite_client.simulators.integrations.list(limit=None)
    if not simulator_integrations.get(external_id=SIMULATOR_INTEGRATION["externalId"]):
        SIMULATOR_INTEGRATION["heartbeat"] = timestamp
        SIMULATOR_INTEGRATION["dataSetId"] = seed_resource_names.simulator_test_data_set_id
        res = cognite_client.simulators._post(
            "/simulators/integrations",
            json={"items": [SIMULATOR_INTEGRATION]},
        )
        log_id = res.json()["items"][0]["logId"]
    else:
        integration = simulator_integrations.get(external_id=SIMULATOR_INTEGRATION["externalId"])
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
    cognite_client: CogniteClient, seed_simulator_integration: None, seed_resource_names: ResourceNames
) -> Iterator[dict[str, Any]]:
    model_unique_external_id = seed_resource_names.simulator_model_external_id
    models = cognite_client.simulators.models.list(limit=None)
    model_exists = models.get(external_id=model_unique_external_id)

    if not model_exists:
        SIMULATOR_MODEL["dataSetId"] = seed_resource_names.simulator_test_data_set_id
        SIMULATOR_MODEL["externalId"] = model_unique_external_id

        model = SimulatorModelWrite._load(
            {
                "externalId": SIMULATOR_MODEL["externalId"],
                "simulatorExternalId": SIMULATOR_MODEL["simulatorExternalId"],
                "dataSetId": SIMULATOR_MODEL["dataSetId"],
                "name": SIMULATOR_MODEL["name"],
                "type": SIMULATOR_MODEL["type"],
                "description": SIMULATOR_MODEL["description"],
            }
        )
        cognite_client.simulators.models.create(model)

    yield SIMULATOR_MODEL

    cognite_client.simulators.models.delete(external_ids=model_unique_external_id)


@pytest.fixture(scope="session")
def seed_simulator_model_revisions(
    cognite_client: CogniteClient, seed_simulator_models, seed_model_revision_file, seed_resource_names: ResourceNames
) -> None:
    model_unique_external_id = seed_resource_names.simulator_model_external_id
    model_revision_unique_external_id = seed_resource_names.simulator_model_revision_external_id
    model_revisions = cognite_client.simulators.models.revisions.list(
        model_external_ids=[model_unique_external_id],
    )

    revisions = [f"{model_revision_unique_external_id}_1", model_revision_unique_external_id]

    for revision in revisions:
        if not model_revisions.get(external_id=revision):
            cognite_client.simulators._post(
                "/simulators/models/revisions",
                json={
                    "items": [
                        {
                            "description": "test sim model revision description",
                            "fileId": seed_model_revision_file.id,
                            "modelExternalId": model_unique_external_id,
                            "externalId": revision,
                        }
                    ]
                },
            )


@pytest.fixture(scope="session")
def seed_simulator_routines(cognite_client: CogniteClient, seed_simulator_model_revisions):
    model_unique_external_id = resources.simulator_model_external_id
    simulator_routine_unique_external_id = resources.simulator_routine_external_id

    routines = cognite_client.simulators.routines.create(
        [
            SimulatorRoutineWrite.load(
                {
                    **SIMULATOR_ROUTINE,
                    "modelExternalId": model_unique_external_id,
                    "externalId": simulator_routine_unique_external_id,
                }
            ),
            SimulatorRoutineWrite.load(
                {
                    **SIMULATOR_ROUTINE,
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

    revision = {**SIMULATOR_ROUTINE_REVISION_OBJ, "externalId": rev_external_id}

    if not routine_rev_exists:
        cognite_client.simulators.routines.revisions.create(SimulatorRoutineRevisionWrite.load(revision))

    return revision


@pytest.fixture(scope="session")
def seed_simulator_routine_revisions(cognite_client: CogniteClient, seed_simulator_routines):
    simulator_routine_external_id = resources.simulator_routine_external_id

    rev1 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v1")
    rev2 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v2")

    yield rev1, rev2
