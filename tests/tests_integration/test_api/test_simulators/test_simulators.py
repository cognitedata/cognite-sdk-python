import datetime

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.data_classes.simulators.simulators import SimulatorModel


class TestSimulators:
    def test_list_simulators(self, cognite_client: CogniteClient) -> None:
        simulators = cognite_client.simulators.list(limit=5)

        assert len(simulators) > 0


class TestSimulatorIntegrations:
    # test list
    # test filter
    # test retrieve
    def test_list_integrations(self, cognite_client: CogniteClient) -> None:
        integrations = cognite_client.simulators.list_integrations(limit=5)

        assert len(integrations) > 0

    def test_filter_integrations(self, cognite_client: CogniteClient) -> None:
        all_integrations = cognite_client.simulators.list_integrations()
        active_integrations = cognite_client.simulators.list_integrations(
            filter=SimulatorIntegrationFilter(active=True)
        )
        dwsim_integrations = cognite_client.simulators.list_integrations(
            filter=SimulatorIntegrationFilter(simulator_external_ids=["DWSIM"])
        )

        assert len(active_integrations) > 0
        assert len(all_integrations) != len(active_integrations)
        assert len(dwsim_integrations) > 0
        assert len(all_integrations) != len(dwsim_integrations)


class TestSimulatorModels:
    TEST_DATA_SET_ID = 97552494921583

    def test_list_models(self, cognite_client: CogniteClient) -> None:
        models = cognite_client.simulators.list_models(limit=5)
        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient) -> None:
        model = cognite_client.simulators.retrieve_model(external_id="TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL")
        assert model is not None
        assert model.external_id == "TEST_WORKFLOWS_SIMINT_INTEGRATION_MODEL"

    def test_list_model_revisions(self, cognite_client: CogniteClient) -> None:
        revisions = cognite_client.simulators.list_model_revisions(limit=5)
        assert len(revisions) > 0

    def test_retrieve_model_revision(self, cognite_client: CogniteClient) -> None:
        model = cognite_client.simulators.retrieve_model_revision(external_id="Shower_mixer-1")
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
        routines = cognite_client.simulators.list_routines(limit=5)
        assert len(routines) > 0

    #     def test_list_routine_revisions(self, cognite_client: CogniteClient) -> None:
    #         revisions = cognite_client.simulators.list_routine_revisions(limit=5)
    #         assert revisions[0].configuration is not None
    #         assert revisions[0].script is not None
    #         assert len(revisions) > 0

    def test_retrieve_routine_revision(self, cognite_client: CogniteClient) -> None:
        revision = cognite_client.simulators.retrieve_routine_revision(external_id="ShowerMixerForTests-1")
        assert revision is not None
        assert revision.external_id == "ShowerMixerForTests-1"


class TestSimulationRuns:
    def test_list_runs(self, cognite_client: CogniteClient) -> None:
        routines = cognite_client.simulators.list_runs(limit=5)
        assert len(routines) > 0
