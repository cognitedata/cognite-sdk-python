import asyncio
import datetime
import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.filters import (
    SimulationRunsFilter,
    SimulatorIntegrationFilter,
    SimulatorModelRevisionsFilter,
    SimulatorModelsFilter,
)
from cognite.client.data_classes.simulators.simulators import (
    SimulationRun,
    SimulationRunCall,
    SimulatorModel,
    SimulatorModelRevision,
)
from tests.tests_integration.test_api.test_simulators.seed.data import (
    resource_names,
    simulator,
    simulator_integration,
    simulator_model,
    simulator_model_revision,
    simulator_routine,
    simulator_routine_revision,
)


@pytest.fixture(scope="class")
def seed_resource_names() -> dict[str, str]:
    return resource_names


@pytest.fixture
def seed_file(cognite_client: CogniteClient, seed_resource_names) -> FileMetadata | None:
    # check if file already exists
    file = cognite_client.files.retrieve(external_id=seed_resource_names["simulator_model_file_external_id"])
    if (file is None) or (file is False):
        file = cognite_client.files.upload(
            path="tests/tests_integration/test_api/test_simulators/seed/ShowerMixer.dwxmz",
            external_id=seed_resource_names["simulator_model_file_external_id"],
            name="ShowerMixer.dwxmz",
            data_set_id=97552494921583,
        )
    yield file


@pytest.fixture
def seed_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    simulator_external_id = seed_resource_names["simulator_external_id"]
    print(f"\nSeeding simulator with external id = {simulator_external_id} \n")
    simulators = cognite_client.simulators.list()
    for sim in simulators:
        if sim.external_id == simulator_external_id:
            return

    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators",
        json={"items": [simulator]},
    )


@pytest.fixture
def seed_simulator_integration(cognite_client: CogniteClient, seed_simulator) -> None:
    print("\nSeeding simulator integration \n")

    def create_integration():
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations",
            json={"items": [simulator_integration]},
        )

    try:
        create_integration()
    except Exception:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations/delete",
            json={"items": [{"externalId": simulator_integration["externalId"]}]},
        )
        create_integration()
        pass


@pytest.fixture
def seed_simulator_models(cognite_client: CogniteClient, seed_simulator_integration) -> None:
    print("\nSeeding simulator models \n")
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/models",
        json={"items": [simulator_model]},  # Post actual simulator models here
    )


@pytest.fixture
def seed_simulator_model_revisions(cognite_client: CogniteClient, seed_simulator_models, seed_file) -> None:
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/models/revisions",
        json={"items": [{**simulator_model_revision, "fileId": seed_file.id}]},  # Post actual simulator models here
    )


@pytest.fixture
def seed_simulator_routines(cognite_client: CogniteClient, seed_simulator_model_revisions) -> None:
    print("\nSeeding simulator routines \n")
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/routines",
        json={"items": [simulator_routine]},
    )


@pytest.fixture
def seed_simulator_routine_revisions(cognite_client: CogniteClient, seed_simulator_routines) -> None:
    print("\nSeeding simulator routine revisions \n")
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/routines/revisions",
        json={"items": [simulator_routine_revision]},
    )


@pytest.fixture
def seed_simulator_resources(cognite_client: CogniteClient) -> FileMetadata | None:
    simulator_external_id = "integration_tests_workflow"
    simulator_model_file_external_id = "ShowerMixer_simulator_model_file_3"

    # check if file already exists
    file = cognite_client.files.retrieve(external_id=simulator_model_file_external_id)
    if (file is None) or (file is False):
        file = cognite_client.files.upload(
            path="tests/tests_integration/test_api/test_simulators/seed/ShowerMixer.dwxmz",
            external_id=simulator_model_file_external_id,
            name="ShowerMixer.dwxmz",
            data_set_id=97552494921583,
        )

    resources = [
        {
            "url": f"/api/v1/projects/{cognite_client.config.project}/simulators/delete",
            "seed": {"externalId": simulator_external_id},
        },
        {"url": f"/api/v1/projects/{cognite_client.config.project}/simulators", "seed": simulator},
        {
            "url": f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations",
            "seed": simulator_integration,
        },
        {"url": f"/api/v1/projects/{cognite_client.config.project}/simulators/models", "seed": simulator_model},
        {
            "url": f"/api/v1/projects/{cognite_client.config.project}/simulators/models/revisions",
            "seed": {**simulator_model_revision, "fileId": file.id},
        },
        {"url": f"/api/v1/projects/{cognite_client.config.project}/simulators/routines", "seed": simulator_routine},
        {
            "url": f"/api/v1/projects/{cognite_client.config.project}/simulators/routines/revisions",
            "seed": simulator_routine_revision,
        },
    ]

    for resource in resources:
        try:
            cognite_client.post(
                resource["url"],
                json={"items": [resource["seed"]]},
            )
        except Exception as e:
            print(e)
            pass

    if isinstance(file, FileMetadata):
        yield file

    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/delete",
        json={"items": [{"externalId": simulator_external_id}]},
    )

    cognite_client.files.delete(external_id=simulator_model_file_external_id)


@pytest.fixture(scope="class")
def delete_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    yield
    print("Deleting simulator")
    response = cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/delete",
        json={"items": [{"externalId": seed_resource_names["simulator_external_id"]}]},
    )
    print("Deleted simulator with external id = ", seed_resource_names["simulator_external_id"])


class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0


class TestSimulatorIntegrations:
    # test list
    # test filter
    # test retrieve
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient) -> None:
        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(active=True)
        )
        dwsim_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(simulator_external_ids=["DWSIM"])
        )

        assert len(active_integrations) > 0
        assert len(all_integrations) != len(active_integrations)
        assert len(dwsim_integrations) > 0
        assert len(all_integrations) != len(dwsim_integrations)


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator", "delete_simulator")
class TestSimulatorModels:
    TEST_DATA_SET_ID = 97552494921583
    TEST_FILE_ID = 1951667411909355

    @pytest.mark.usefixtures("seed_simulator_models", "seed_simulator_model_revisions")
    def test_list_models(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        models = cognite_client.simulators.models.list(
            limit=5, filter=SimulatorModelsFilter(simulator_external_ids=[seed_resource_names["simulator_external_id"]])
        )
        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model = cognite_client.simulators.models.retrieve(external_id="TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL")
        assert model is not None
        assert model.external_id == "TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL"

    def test_list_model_revisions(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        revisions = cognite_client.simulators.models.list(
            limit=5,
            filter=SimulatorModelRevisionsFilter(
                model_external_ids=[seed_resource_names["simulator_model_external_id"]]
            ),
        )
        assert len(revisions) > 0

    def test_retrieve_model_revision(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        # TODO : this test is incorrect, it should retreive model revisions instead of model
        model = cognite_client.simulators.models.retrieve(
            external_id=seed_resource_names["simulator_model_external_id"]
        )
        assert model is not None
        assert model.external_id == seed_resource_names["simulator_model_external_id"]

    @pytest.mark.usefixtures("seed_file", "seed_resource_names")
    def test_create_model(self, cognite_client: CogniteClient, seed_file: FileMetadata, seed_resource_names) -> None:
        model_external_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        models_to_create = SimulatorModel(
            name="sdk-test-model1",
            simulator_external_id=seed_resource_names["simulator_external_id"],
            external_id=model_external_id,
            data_set_id=self.TEST_DATA_SET_ID,
            type="SteadyState",
        )

        models_created = cognite_client.simulators.models.create(models_to_create)
        assert models_created is not None
        assert models_created.external_id == model_external_id

        model_revision_to_create = SimulatorModelRevision(
            external_id=model_external_id + "-revision-1",
            model_external_id=model_external_id,
            file_id=seed_file.id,
            description="Test revision",
        )

        model_revision_created = cognite_client.simulators.models.create_revisions(model_revision_to_create)
        assert model_revision_created is not None
        assert model_revision_created.external_id == model_external_id + "-revision-1"
        cognite_client.simulators.models.delete(external_ids=model_external_id)

    def test_update_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        models_to_create = SimulatorModel(
            name="sdk-test-model1",
            simulator_external_id=seed_resource_names["simulator_external_id"],
            external_id=model_external_id,
            data_set_id=self.TEST_DATA_SET_ID,
            type="SteadyState",
        )

        models_created = cognite_client.simulators.models.create(models_to_create)
        assert models_created is not None
        assert models_created.external_id == model_external_id  # Validate external ID
        models_created.description = "updated description"  # Update the description
        models_created.name = "updated name"  # Update the name
        model_updated = cognite_client.simulators.models.update(models_created)
        assert model_updated is not None
        assert model_updated.description == "updated description"
        assert model_updated.name == "updated name"


class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.routines.list(limit=5)
        assert len(routines) > 0

    def test_list_routine_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.routines.revisions.list(limit=5)
        assert revisions[0].configuration is not None
        assert revisions[0].script is not None
        assert len(revisions) > 0

    def test_retrieve_routine_revision(self, cognite_client: CogniteClient) -> None:
        revision = cognite_client.simulators.routines.revisions.retrieve(external_id="ShowerMixerForTests-1")
        assert revision is not None
        assert revision.external_id == "ShowerMixerForTests-1"


class TestSimulationRuns:
    @pytest.mark.usefixtures("seed_simulator_routine_revisions", "delete_simulator", "seed_resource_names")
    def test_create_runs(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        integrations = cognite_client.simulators.integrations.list()
        assert len(integrations) > 0
        # Check if the seeded simulator integration is alive
        integration_to_check = seed_resource_names["simulator_integration_external_id"]
        integration = next((x for x in integrations if x.external_id == integration_to_check), None)
        assert integration is not None
        assert integration.heartbeat >= time.time() - 60

        run_to_create = SimulationRun(
            routine_external_id=seed_resource_names["simulator_routine_external_id"],
        )

        run = cognite_client.simulators.runs.create(run_to_create)
        assert run is not None
        assert run.status == "ready"

    @pytest.mark.usefixtures("seed_resource_names")
    def test_list_runs(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]
        routines = cognite_client.simulators.runs.list(
            limit=5, filter=SimulationRunsFilter(model_external_ids=[model_external_id])
        )
        assert len(routines) > 0

    @pytest.mark.usefixtures("seed_resource_names")
    async def test_run_async(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        async def trigger_callback_after_delay(run_id: int, delay: int):
            await asyncio.sleep(delay)  # Wait for the specified delay
            cognite_client.simulators._do_request(
                "POST", "/simulators/run/callback", json={"items": [{"id": run_id, "status": "success"}]}
            )

        run_to_create = SimulationRunCall(
            routine_external_id=seed_resource_names["simulator_routine_external_id"],
        )

        run = cognite_client.simulators.runs.run(run_to_create)

        assert run is not None
