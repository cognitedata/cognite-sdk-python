import datetime

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.data_classes.simulators.simulators import SimulatorModel
from tests.tests_integration.test_api.test_simulators.seed.data import (
    simulator,
    simulator_integration,
    simulator_model,
    simulator_model_revision,
    simulator_routine,
    simulator_routine_revision,
)


@pytest.fixture
def add_simulator_resoures(cognite_client: CogniteClient) -> None:
    simulator_external_id = "integration_tests_workflow"
    simulator_model_file_external_id = "ShowerMixer_simulator_model_file"

    file = cognite_client.files.upload(
        path="tests/tests_integration/test_api/test_simulators/seed/data/ShowerMixer.dwxmz",
        external_id=simulator_model_file_external_id,
        name="ShowerMixer.dwxmz",
        data_set_id=97552494921583,
    )

    resources = [
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
        cognite_client.post(
            resource["url"],
            json={"items": [resource["seed"]]},
            headers={"cdf-version": "alpha"},
        )

    yield None

    cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/delete",
        json={"items": [{"externalId": simulator_external_id}]},
        headers={"cdf-version": "alpha"},
    )

    cognite_client.files.delete(external_id=simulator_model_file_external_id)


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


class TestSimulatorModels:
    TEST_DATA_SET_ID = 97552494921583

    def test_list_models(self, cognite_client: CogniteClient) -> None:
        models = cognite_client.simulators.models.list(limit=5)
        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient) -> None:
        model = cognite_client.simulators.models.retrieve(external_id="TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL")
        assert model is not None
        assert model.external_id == "TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL"

    def test_list_model_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.models.list_revisions(limit=5)
        assert len(revisions) > 0

    def test_retrieve_model_revision(self, cognite_client: CogniteClient) -> None:
        model = cognite_client.simulators.models.retrieve_revision(external_id="Shower_mixer-1")
        assert model is not None
        assert model.external_id == "Shower_mixer-1"

    @pytest.fixture
    def test_create_model(self, cognite_client: CogniteClient) -> None:
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        model_external_id = current_time
        models_to_create = SimulatorModel(
            name="model1",
            simulator_external_id="DWSIM",
            external_id=model_external_id,
            data_set_id=self.TEST_DATA_SET_ID,
            type="SteadyState",
        )

        models_created = cognite_client.simulators.create_model(models_to_create)
        assert models_created is not None
        assert models_created.external_id == model_external_id
        # delete created model
        cognite_client.simulators.delete_models(id=models_created.id)

        # assert model.external_id == "TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL"


class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.routines.list(limit=5)
        assert len(routines) > 0

    def test_list_routine_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.routines.list_revisions(limit=5)
        assert revisions[0].configuration is not None
        assert revisions[0].script is not None
        assert len(revisions) > 0

    def test_retrieve_routine_revision(self, cognite_client: CogniteClient) -> None:
        revision = cognite_client.simulators.routines.retrieve_revision(external_id="ShowerMixerForTests-1")
        assert revision is not None
        assert revision.external_id == "ShowerMixerForTests-1"


class TestSimulationRuns:
    def test_list_runs(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.runs.list(limit=5)
        assert len(routines) > 0
