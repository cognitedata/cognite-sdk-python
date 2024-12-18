import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorModelsFilter


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_model_revisions",
)
class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        models = cognite_client.simulators.models.list(
            limit=5, filter=SimulatorModelsFilter(simulator_external_ids=[seed_resource_names["simulator_external_id"]])
        )
        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]
        model = cognite_client.simulators.models.retrieve(external_id=model_external_id)
        assert model is not None
        assert model.external_id == model_external_id

    def test_list_model_revisions(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]

        revisions = cognite_client.simulators.models.revisions.list(
            limit=5,
            filter=SimulatorModelRevisionsFilter(model_external_ids=[model_external_id]),
        )
        assert len(revisions) > 0

    def test_retrieve_model_revision(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_revision_external_id = seed_resource_names["simulator_model_revision_external_id"]
        model_revision = cognite_client.simulators.models.revisions.retrieve(external_id=model_revision_external_id)
        assert model_revision is not None
        assert model_revision.model_external_id == model_revision_external_id
