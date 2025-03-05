import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorModelsFilter
from cognite.client.data_classes.simulators.models import (
    SimulatorModelRevisionWrite,
    SimulatorModelWrite,
)
from cognite.client.utils._text import random_string


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_model_revisions",
)
class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        models = cognite_client.simulators.models.list(
            limit=5, filter=SimulatorModelsFilter(simulator_external_ids=[seed_resource_names["simulator_external_id"]])
        )

        model_ids = []
        for model in cognite_client.simulators.models(limit=2):
            assert model.created_time is not None
            model_ids.append(model.id)

        found_models = cognite_client.simulators.models.retrieve(id=model_ids)

        assert len(found_models) == len(model_ids)

        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]
        model = cognite_client.simulators.models.retrieve(external_id=model_external_id)
        assert model is not None
        assert model.external_id == model_external_id
        assert model.created_time > 0
        assert model.last_updated_time >= model.created_time
        assert model.type == "SteadyState"
        assert model.data_set_id == seed_resource_names["simulator_test_data_set_id"]
        assert model.name == "Test Simulator Model"

    def test_list_model_revisions(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]

        revisions = cognite_client.simulators.models.revisions.list(
            limit=5,
            filter=SimulatorModelRevisionsFilter(model_external_ids=[model_external_id]),
        )

        model_revision_ids = []
        for revision in cognite_client.simulators.models.revisions(limit=2):
            assert revision.created_time is not None
            model_revision_ids.append(revision.id)

        found_revisions = cognite_client.simulators.models.revisions.retrieve(id=model_revision_ids)
        assert len(found_revisions) == len(model_revision_ids)

        assert len(revisions) > 0

    def test_retrieve_model_revision(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_revision_external_id = seed_resource_names["simulator_model_revision_external_id"]
        model_revision = cognite_client.simulators.models.revisions.retrieve(external_id=model_revision_external_id)
        assert model_revision is not None
        assert model_revision.model_external_id == seed_resource_names["simulator_model_external_id"]

    @pytest.mark.usefixtures("seed_file", "seed_resource_names")
    def test_create_model_and_revisions(
        self, cognite_client: CogniteClient, seed_file: FileMetadata, seed_resource_names
    ) -> None:
        model_external_id_1 = random_string(10)
        model_external_id_2 = random_string(10)
        models_to_create = [
            SimulatorModelWrite(
                name="sdk-test-model1",
                simulator_external_id=seed_resource_names["simulator_external_id"],
                external_id=model_external_id_1,
                data_set_id=seed_resource_names["simulator_test_data_set_id"],
                type="SteadyState",
            ),
            SimulatorModelWrite(
                name="sdk-test-model2",
                simulator_external_id=seed_resource_names["simulator_external_id"],
                external_id=model_external_id_2,
                data_set_id=seed_resource_names["simulator_test_data_set_id"],
                type="SteadyState",
            ),
        ]

        models_created = cognite_client.simulators.models.create(models_to_create)

        assert models_created is not None
        assert len(models_created) == 2
        model_revision_external_id = model_external_id_1 + "revision"
        model_revision_to_create = SimulatorModelRevisionWrite(
            external_id=model_revision_external_id,
            model_external_id=model_external_id_1,
            file_id=seed_file.id,
            description="Test revision",
        )
        multiple_model_revisions_to_create = [
            SimulatorModelRevisionWrite(
                external_id=model_revision_external_id + "1",
                model_external_id=model_external_id_1,
                file_id=seed_file.id,
                description="Test revision",
            ),
            SimulatorModelRevisionWrite(
                external_id=model_revision_external_id + "2",
                model_external_id=model_external_id_2,
                file_id=seed_file.id,
                description="Test revision",
            ),
        ]

        multiple_model_revisions_created = cognite_client.simulators.models.revisions.create(
            multiple_model_revisions_to_create
        )
        model_revision_created = cognite_client.simulators.models.revisions.create(model_revision_to_create)

        assert model_revision_created is not None
        assert model_revision_created.external_id == model_revision_external_id
        assert len(multiple_model_revisions_created) == 2
        cognite_client.simulators.models.delete(external_id=[model_external_id_1, model_external_id_2])

    def test_update_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = random_string(10)
        models_to_create = SimulatorModelWrite(
            name="sdk-test-model1",
            simulator_external_id=seed_resource_names["simulator_external_id"],
            external_id=model_external_id,
            data_set_id=seed_resource_names["simulator_test_data_set_id"],
            type="SteadyState",
        )

        models_created = cognite_client.simulators.models.create(models_to_create)
        assert models_created is not None
        assert models_created.external_id == model_external_id
        models_created.description = "updated description"
        models_created.name = "updated name"
        model_updated = cognite_client.simulators.models.update(models_created)
        assert model_updated is not None
        assert model_updated.description == "updated description"
        assert model_updated.name == "updated name"
        cognite_client.simulators.models.delete(external_id=[models_created.external_id])
