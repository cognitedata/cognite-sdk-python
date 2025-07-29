import time

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes import TimestampRange
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.models import (
    SimulatorModelExternalDependencyFileField,
    SimulatorModelRevisionExternalDependency,
    SimulatorModelRevisionWrite,
    SimulatorModelWrite,
)
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.utils import update_logs


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_model_revisions",
)
class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        models = cognite_client.simulators.models.list(
            limit=5,
            simulator_external_ids=[seed_resource_names["simulator_external_id"]],
        )

        model_ids = []
        for model in cognite_client.simulators.models(limit=2):
            assert model.created_time is not None
            model_ids.append(model.id)

        found_models = cognite_client.simulators.models.retrieve(ids=model_ids)

        assert len(found_models) == len(model_ids)

        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]
        model = cognite_client.simulators.models.retrieve(external_ids=model_external_id)
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
            model_external_ids=[model_external_id],
        )

        model_revision_ids = []
        for revision in cognite_client.simulators.models.revisions(limit=2):
            assert revision.created_time is not None
            model_revision_ids.append(revision.id)

        found_revisions = cognite_client.simulators.models.revisions.retrieve(ids=model_revision_ids)
        assert len(found_revisions) == len(model_revision_ids)

        assert len(revisions) > 0

    def test_list_model_revisions_filtering_all_versions(
        self, cognite_client: CogniteClient, seed_resource_names
    ) -> None:
        model_external_id = seed_resource_names["simulator_model_external_id"]
        revisions_all_versions = cognite_client.simulators.models.revisions.list(
            all_versions=True,
            model_external_ids=[model_external_id],
            created_time=TimestampRange(start=0, end=int(time.time() * 1000)),
            last_updated_time=TimestampRange(start=0, end=int(time.time() * 1000)),
        )
        revisions_all_versions_external_ids = [revision.external_id for revision in revisions_all_versions]
        revisions_default = cognite_client.simulators.models.revisions.list(
            model_external_ids=seed_resource_names["simulator_model_external_id"]
        )

        assert len(revisions_default) == 1
        assert revisions_default[0].external_id in revisions_all_versions_external_ids
        assert len(revisions_all_versions) != len(revisions_default)

    def test_list_model_revisions_filtering_sort(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        revisions_asc = cognite_client.simulators.models.revisions.list(
            sort=PropertySort(order="asc", property="createdTime"),
            model_external_ids=[seed_resource_names["simulator_model_external_id"]],
            all_versions=True,
        )

        revisions_desc = cognite_client.simulators.models.revisions.list(
            sort=PropertySort(order="desc", property="createdTime"),
            model_external_ids=[seed_resource_names["simulator_model_external_id"]],
            all_versions=True,
        )
        assert len(revisions_asc) > 0
        assert len(revisions_desc) > 0
        assert revisions_asc[0].created_time == revisions_desc[-1].created_time
        assert revisions_desc[0].created_time == revisions_asc[-1].created_time

    def test_retrieve_model_revision(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_revision_external_id = seed_resource_names["simulator_model_revision_external_id"]
        model_revision = cognite_client.simulators.models.revisions.retrieve(external_ids=model_revision_external_id)
        assert model_revision is not None
        assert model_revision.model_external_id == seed_resource_names["simulator_model_external_id"]

    @pytest.mark.usefixtures("seed_model_revision_file", "seed_external_dependency_file", "seed_resource_names")
    def test_create_model_and_revisions(
        self,
        cognite_client: CogniteClient,
        seed_model_revision_file: FileMetadata,
        seed_external_dependency_file: FileMetadata,
        seed_resource_names,
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
        external_dependencies = [
            SimulatorModelRevisionExternalDependency(
                file=SimulatorModelExternalDependencyFileField(id=seed_external_dependency_file.id),
                arguments={
                    "fieldA": "value1",
                    "fieldB": "value2",
                },
            )
        ]
        model_revision_to_create = SimulatorModelRevisionWrite(
            external_id=model_revision_external_id,
            model_external_id=model_external_id_1,
            file_id=seed_model_revision_file.id,
            description="Test revision",
        )
        multiple_model_revisions_to_create = [
            SimulatorModelRevisionWrite(
                external_id=model_revision_external_id + "1",
                model_external_id=model_external_id_1,
                file_id=seed_model_revision_file.id,
                description="Test revision",
            ),
            SimulatorModelRevisionWrite(
                external_id=model_revision_external_id + "2",
                model_external_id=model_external_id_2,
                file_id=seed_model_revision_file.id,
                description="Test revision",
            ),
            SimulatorModelRevisionWrite(
                external_id=model_revision_external_id + "3",
                model_external_id=model_external_id_2,
                file_id=seed_model_revision_file.id,
                description="Test revision",
                external_dependencies=external_dependencies,
            ),
        ]

        multiple_model_revisions_created = cognite_client.simulators.models.revisions.create(
            multiple_model_revisions_to_create
        )
        model_revision_created = cognite_client.simulators.models.revisions.create(model_revision_to_create)

        assert model_revision_created is not None
        assert model_revision_created.external_id == model_revision_external_id
        assert model_revision_created.log_id is not None
        update_logs(
            cognite_client,
            model_revision_created.log_id,
            [
                {
                    "timestamp": 123456789,
                    "message": "Testing logs update for simulator model revision",
                    "severity": "Information",
                }
            ],
        )
        log = cognite_client.simulators.logs.retrieve(ids=model_revision_created.log_id)
        assert log is not None
        assert log.data is not None
        assert len(log.data) == 1
        assert log.data[0].message == "Testing logs update for simulator model revision"
        assert log.data[0].severity == "Information"
        assert len(multiple_model_revisions_created) == 3
        revision_with_external_dependencies = list(
            filter(lambda x: x.external_dependencies is not None, multiple_model_revisions_created)
        )
        assert len(revision_with_external_dependencies) == 1
        assert revision_with_external_dependencies[0].external_dependencies == external_dependencies
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
        cognite_client.simulators.models.delete(external_ids=[model_updated.external_id])
