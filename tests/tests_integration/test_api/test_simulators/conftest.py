from __future__ import annotations

import copy
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_sets import DataSetWrite
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators import (
    SimulatorModel,
    SimulatorModelRevisionWrite,
    SimulatorModelWrite,
    SimulatorRoutine,
    SimulatorRoutineList,
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionWrite,
    SimulatorRoutineWrite,
)
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._text import to_snake_case
from tests.tests_integration.test_api.test_simulators.seed.data import (
    RESOURCES,
    SIMULATOR,
    SIMULATOR_INTEGRATION,
    SIMULATOR_MODEL,
    SIMULATOR_ROUTINE,
    SIMULATOR_ROUTINE_REVISION,
    ResourceNames,
)
from tests.tests_integration.test_api.test_simulators.utils import update_logs

SEED_DIR = Path(__file__).resolve(strict=True).parent / "seed"


@pytest.fixture(scope="session")
def seed_resource_names(cognite_client: CogniteClient) -> ResourceNames:
    seed_data_set_external_id = RESOURCES.simulator_test_data_set_external_id
    data_set = cognite_client.data_sets.retrieve(external_id=seed_data_set_external_id)
    if not data_set:
        data_sets = cognite_client.data_sets.create(
            [DataSetWrite(external_id=seed_data_set_external_id, name=seed_data_set_external_id)]
        )
        data_set = data_sets[0]
    RESOURCES.simulator_test_data_set_id = data_set.id
    return copy.copy(RESOURCES)


def upload_file(cognite_client: CogniteClient, filename: str, external_id: str, data_set_id: int) -> FileMetadata:
    file = cognite_client.files.retrieve(external_id=external_id)
    if not file:
        uploaded_file = cognite_client.files.upload(
            path=SEED_DIR / filename,
            external_id=external_id,
            name=filename,
            data_set_id=data_set_id,
        )
        assert isinstance(uploaded_file, FileMetadata)
        return uploaded_file

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
def seed_simulator(
    cognite_client: CogniteClient, async_client: AsyncCogniteClient, seed_resource_names: ResourceNames
) -> None:
    simulator_external_id = seed_resource_names.simulator_external_id
    simulators = cognite_client.simulators.list(limit=None)
    seeded_simulator = simulators.get(external_id=simulator_external_id)
    fields_to_compare = ["fileExtensionTypes", "modelTypes", "modelDependencies", "stepFields", "unitQuantities"]

    if not seeded_simulator:
        run_sync(async_client.simulators._post("/simulators", json={"items": [SIMULATOR]}, semaphore=None))
    # if any field in simulator is different from the current seeded simulator, update it
    elif any(getattr(seeded_simulator, to_snake_case(field)) != SIMULATOR.get(field) for field in fields_to_compare):
        simulator_update = {
            "id": seeded_simulator.id,
            "update": {field: {"set": SIMULATOR.get(field)} for field in fields_to_compare},
        }
        run_sync(
            async_client.simulators._post("/simulators/update", json={"items": [simulator_update]}, semaphore=None)
        )


@pytest.fixture(scope="session")
def seed_simulator_integration(
    cognite_client: CogniteClient,
    async_client: AsyncCogniteClient,
    seed_simulator: None,
    seed_resource_names: ResourceNames,
) -> None:
    log_id = None
    timestamp = int(time.time() * 1000)
    simulator_integrations = cognite_client.simulators.integrations.list(limit=None)
    existing_integration = simulator_integrations.get(external_id=seed_resource_names.simulator_integration_external_id)
    if not existing_integration:
        new_integration = {
            **SIMULATOR_INTEGRATION,
            "heartbeat": timestamp,
            "dataSetId": seed_resource_names.simulator_test_data_set_id,
        }
        res = run_sync(
            async_client.simulators._post(
                "/simulators/integrations",
                json={"items": [new_integration]},
                semaphore=None,
            )
        )
        log_id = res.json()["items"][0]["logId"]
    else:
        log_id = existing_integration.log_id
        run_sync(
            async_client.simulators.integrations._post(
                "/simulators/integrations/update",
                json={"items": [{"id": existing_integration.id, "update": {"heartbeat": {"set": timestamp}}}]},
                semaphore=None,
            )
        )

    if log_id:
        run_sync(
            update_logs(
                async_client,
                log_id,
                [
                    {
                        "timestamp": timestamp,
                        "message": "Testing logs update for simulator integration",
                        "severity": "Debug",
                    }
                ],
            )
        )


@pytest.fixture(scope="session")
def seed_simulator_models(
    cognite_client: CogniteClient, seed_simulator_integration: None, seed_resource_names: ResourceNames
) -> Iterator[SimulatorModel]:
    model_unique_external_id = seed_resource_names.simulator_model_external_id
    models = cognite_client.simulators.models.list(limit=None)
    model = models.get(external_id=model_unique_external_id)

    if not model:
        new_model = SimulatorModelWrite._load(
            {
                **SIMULATOR_MODEL.dump(),
                "dataSetId": seed_resource_names.simulator_test_data_set_id,
                "externalId": model_unique_external_id,
            }
        )
        res = cognite_client.simulators.models.create(new_model)
        yield res
    else:
        yield model

    cognite_client.simulators.models.delete(external_ids=model_unique_external_id)


@pytest.fixture(scope="session")
def seed_simulator_model_revisions(
    cognite_client: CogniteClient,
    async_client: AsyncCogniteClient,
    seed_simulator_models: SimulatorModel,
    seed_model_revision_file: FileMetadata,
    seed_resource_names: ResourceNames,
) -> None:
    model_unique_external_id = seed_resource_names.simulator_model_external_id
    model_revision_unique_external_id = seed_resource_names.simulator_model_revision_external_id
    model_revisions = cognite_client.simulators.models.revisions.list(
        model_external_ids=[model_unique_external_id],
    )

    revisions = [f"{model_revision_unique_external_id}_1", model_revision_unique_external_id]

    for revision in revisions:
        if not model_revisions.get(external_id=revision):
            cognite_client.simulators.models.revisions.create(
                SimulatorModelRevisionWrite(
                    external_id=revision,
                    model_external_id=model_unique_external_id,
                    file_id=seed_model_revision_file.id,
                    description="test sim model revision description",
                )
            )


@pytest.fixture(scope="session")
def seed_simulator_routines(
    cognite_client: CogniteClient, seed_simulator_model_revisions: None
) -> Iterator[SimulatorRoutineList]:
    model_unique_external_id = RESOURCES.simulator_model_external_id
    simulator_routine_unique_external_id = RESOURCES.simulator_routine_external_id

    routines = cognite_client.simulators.routines.create(
        [
            SimulatorRoutineWrite.load(
                {
                    **SIMULATOR_ROUTINE.dump(),
                    "modelExternalId": model_unique_external_id,
                    "externalId": simulator_routine_unique_external_id,
                }
            ),
            SimulatorRoutineWrite.load(
                {
                    **SIMULATOR_ROUTINE.dump(),
                    "modelExternalId": model_unique_external_id,
                    "externalId": simulator_routine_unique_external_id + "_1",
                }
            ),
        ]
    )

    yield routines

    cognite_client.simulators.routines.delete(external_ids=[routine.external_id for routine in routines])


def seed_simulator_routine_revision(
    cognite_client: CogniteClient, routine_external_id: str, version: str
) -> SimulatorRoutineRevision:
    routine_revs = cognite_client.simulators.routines.revisions.list(routine_external_ids=[routine_external_id])
    rev_external_id = f"{routine_external_id}_{version}"
    revision = routine_revs.get(external_id=rev_external_id)

    if not revision:
        revision_write = SimulatorRoutineRevisionWrite.load(
            {**SIMULATOR_ROUTINE_REVISION.dump(), "externalId": rev_external_id}
        )
        return cognite_client.simulators.routines.revisions.create(revision_write)

    return revision


@pytest.fixture(scope="session")
def seed_simulator_routine_revisions(
    cognite_client: CogniteClient, seed_simulator_routines: list[SimulatorRoutine]
) -> Iterator[tuple[SimulatorRoutineRevision, SimulatorRoutineRevision]]:
    simulator_routine_external_id = RESOURCES.simulator_routine_external_id

    rev1 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v1")
    rev2 = seed_simulator_routine_revision(cognite_client, simulator_routine_external_id, "v2")

    yield rev1, rev2
