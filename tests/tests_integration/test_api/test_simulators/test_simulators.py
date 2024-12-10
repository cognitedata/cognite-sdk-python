import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.filters import (
    SimulatorIntegrationFilter,
)
from cognite.client.exceptions import CogniteAPIError
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
    simulators = cognite_client.simulators.list()
    simulator_exists = len(list(filter(lambda x: x.external_id == simulator_external_id, simulators))) > 0
    if not simulator_exists:
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators",
            json={"items": [simulator]},
        )


@pytest.fixture
def seed_simulator_integration(cognite_client: CogniteClient, seed_simulator) -> None:
    try:
        simulator_integration["heartbeat"] = int(time.time() * 1000)
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations",
            json={"items": [simulator_integration]},
        )
    except CogniteAPIError:
        simulator_integrations = cognite_client.simulators.integrations.list()
        integration_id = list(
            filter(
                lambda x: x.external_id == simulator_integration["externalId"],
                simulator_integrations,
            )
        )[0].id
        # update hearbeat instead
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations/update",
            json={"items": [{"id": integration_id, "update": {"heartbeat": {"set": int(time.time() * 1000)}}}]},
        )


@pytest.fixture
def seed_simulator_models(cognite_client: CogniteClient, seed_simulator_integration) -> None:
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
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/routines",
        json={"items": [simulator_routine]},
    )


@pytest.fixture
def seed_simulator_routine_revisions(cognite_client: CogniteClient, seed_simulator_routines) -> None:
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/routines/revisions",
        json={"items": [simulator_routine_revision]},
    )


@pytest.fixture
def delete_simulator(cognite_client: CogniteClient, seed_resource_names) -> None:
    yield
    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/delete",
        json={"items": [{"externalId": seed_resource_names["simulator_external_id"]}]},
    )


@pytest.mark.usefixtures("seed_resource_names", "seed_simulator")
class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0


class TestSimulatorIntegrations:
    @pytest.mark.usefixtures("seed_resource_names", "seed_simulator_integration")
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.integrations.list(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        all_integrations = cognite_client.simulators.integrations.list()
        active_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(active=True)
        )

        filtered_integrations = cognite_client.simulators.integrations.list(
            filter=SimulatorIntegrationFilter(
                simulator_external_ids=[seed_resource_names["simulator_external_id"]]
            )
        )

        assert len(all_integrations) > 0
        assert filtered_integrations[0].external_id == seed_resource_names["simulator_integration_external_id"]
        # check time difference
        assert filtered_integrations[0].active is True

        assert len(active_integrations) > 0
        assert len(filtered_integrations) > 0


"""@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator",
    "seed_simulator_models",
    "seed_simulator_model_revisions",
    "delete_simulator",
)
class TestSimulatorModels:
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
        # TODO : this test is incorrect, it should retrieve model revisions instead of model
        model = cognite_client.simulators.models.retrieve(
            external_id=seed_resource_names["simulator_model_external_id"]
        )
        assert model is not None
        assert model.external_id == seed_resource_names["simulator_model_external_id"]


@pytest.mark.usefixtures(
    "seed_resource_names", "seed_simulator", "seed_simulator_routine_revisions", "delete_simulator"
)
class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.routines.list(limit=5)
        assert len(routines) > 0

    def test_list_routine_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.routines.revisions.list(limit=5)
        assert revisions[0].configuration is not None
        assert revisions[0].script is not None
        assert len(revisions) > 0
"""
