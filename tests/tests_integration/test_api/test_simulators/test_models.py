import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimestampRange
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.simulators import (
    PropertySort,
    SimulatorFlowsheet,
    SimulatorModelDependencyFileId,
    SimulatorModelRevision,
    SimulatorModelRevisionDependency,
    SimulatorModelRevisionList,
    SimulatorModelRevisionWrite,
    SimulatorModelWrite,
)
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.conftest import upload_file
from tests.tests_integration.test_api.test_simulators.seed.data import (
    SIMULATOR_MODEL_REVISION_DATA_FLOWSHEET,
    ResourceNames,
)
from tests.tests_integration.test_api.test_simulators.utils import update_logs


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_model_revisions",
)
class TestSimulatorModels:
    def test_list_models(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        models = cognite_client.simulators.models.list(
            limit=5,
            simulator_external_ids=[seed_resource_names.simulator_external_id],
        )

        model_ids = []
        for model in cognite_client.simulators.models(
            limit=2,
            simulator_external_ids=[seed_resource_names.simulator_external_id],
            sort=PropertySort(order="asc", property="created_time"),
        ):
            assert model.created_time is not None
            model_ids.append(model.id)

        found_models = cognite_client.simulators.models.retrieve(ids=model_ids)

        assert found_models is not None
        assert len(found_models) == len(model_ids)

        assert len(models) > 0

    def test_retrieve_model(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        model_external_id = seed_resource_names.simulator_model_external_id
        model = cognite_client.simulators.models.retrieve(external_ids=model_external_id)
        assert model is not None
        assert model.external_id == model_external_id
        assert model.created_time > 0
        assert model.last_updated_time >= model.created_time
        assert model.type == "SteadyState"
        assert model.data_set_id == seed_resource_names.simulator_test_data_set_id
        assert model.name == "Test Simulator Model"

    def test_list_model_revisions(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        model_external_id = seed_resource_names.simulator_model_external_id

        revisions = cognite_client.simulators.models.revisions.list(
            limit=5,
            model_external_ids=[model_external_id],
        )

        model_revision_ids = []
        for revision in cognite_client.simulators.models.revisions(
            limit=2,
            sort=PropertySort(order="desc", property="created_time"),
        ):
            assert revision.created_time is not None
            model_revision_ids.append(revision.id)

        found_revisions = cognite_client.simulators.models.revisions.retrieve(ids=model_revision_ids)
        assert isinstance(found_revisions, SimulatorModelRevisionList)
        assert len(found_revisions) == len(model_revision_ids)

        assert len(revisions) > 0

    def test_list_model_revisions_filtering_all_versions(
        self, cognite_client: CogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        model_external_id = seed_resource_names.simulator_model_external_id
        revisions_all_versions = cognite_client.simulators.models.revisions.list(
            all_versions=True,
            model_external_ids=[model_external_id],
            created_time=TimestampRange(start=0, end=int(time.time() * 1000)),
            last_updated_time=TimestampRange(start=0, end=int(time.time() * 1000)),
        )
        revisions_all_versions_external_ids = [revision.external_id for revision in revisions_all_versions]
        revisions_default = cognite_client.simulators.models.revisions.list(
            model_external_ids=[seed_resource_names.simulator_model_external_id]
        )

        assert len(revisions_default) == 1
        assert revisions_default[0].external_id in revisions_all_versions_external_ids
        assert len(revisions_all_versions) != len(revisions_default)

    def test_list_model_revisions_filtering_sort(
        self, cognite_client: CogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        revisions_asc = cognite_client.simulators.models.revisions.list(
            sort=PropertySort(order="asc", property="created_time"),
            model_external_ids=[seed_resource_names.simulator_model_external_id],
            all_versions=True,
        )

        revisions_desc = cognite_client.simulators.models.revisions.list(
            sort=PropertySort(order="desc", property="created_time"),
            model_external_ids=[seed_resource_names.simulator_model_external_id],
            all_versions=True,
        )
        assert len(revisions_asc) > 0
        assert len(revisions_desc) > 0
        assert revisions_asc[0].created_time == revisions_desc[-1].created_time
        assert revisions_desc[0].created_time == revisions_asc[-1].created_time

    def test_retrieve_model_revision(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        model_revision_external_id = seed_resource_names.simulator_model_revision_external_id
        model_revision = cognite_client.simulators.models.revisions.retrieve(external_ids=model_revision_external_id)
        assert model_revision is not None

        assert isinstance(model_revision, SimulatorModelRevision)
        assert model_revision.model_external_id == seed_resource_names.simulator_model_external_id

    @pytest.mark.usefixtures("seed_model_revision_file", "seed_resource_names")
    def test_create_model_and_revisions(
        self,
        cognite_client: CogniteClient,
        seed_model_revision_file: FileMetadata,
        seed_resource_names: ResourceNames,
    ) -> None:
        model_external_id_1 = random_string(10)
        model_external_id_2 = random_string(10)
        models_to_create = [
            SimulatorModelWrite(
                name="sdk-test-model1",
                simulator_external_id=seed_resource_names.simulator_external_id,
                external_id=model_external_id_1,
                data_set_id=seed_resource_names.simulator_test_data_set_id,
                type="SteadyState",
            ),
            SimulatorModelWrite(
                name="sdk-test-model2",
                simulator_external_id=seed_resource_names.simulator_external_id,
                external_id=model_external_id_2,
                data_set_id=seed_resource_names.simulator_test_data_set_id,
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
        assert len(multiple_model_revisions_created) == 2
        cognite_client.simulators.models.delete(external_ids=[model_external_id_1, model_external_id_2])

    @pytest.mark.usefixtures("seed_model_revision_file", "seed_resource_names")
    def test_create_model_and_revisions_with_external_dependencies(
        self,
        cognite_client: CogniteClient,
        seed_model_revision_file: FileMetadata,
        seed_resource_names: ResourceNames,
    ) -> None:
        try:
            model_external_id = random_string(10)
            data_set_id = seed_resource_names.simulator_test_data_set_id
            models_to_create = [
                SimulatorModelWrite(
                    name="sdk-test-model1",
                    simulator_external_id=seed_resource_names.simulator_external_id,
                    external_id=model_external_id,
                    data_set_id=data_set_id,
                    type="SteadyState",
                )
            ]

            models_created = cognite_client.simulators.models.create(models_to_create)

            assert models_created is not None
            assert len(models_created) == 1
            model_revision_external_id = model_external_id + "v1"

            seed_external_dependency_file = upload_file(
                cognite_client,
                filename="ExtDependency.xml",
                external_id=seed_resource_names.simulator_model_external_dependency_file_external_id,
                data_set_id=data_set_id,
            )

            external_dependencies = [
                SimulatorModelRevisionDependency(
                    file=SimulatorModelDependencyFileId(id=seed_external_dependency_file.id),
                    arguments={
                        "fieldA": "value1",
                        "fieldB": "value2",
                    },
                )
            ]
            model_revision_to_create = SimulatorModelRevisionWrite(
                external_id=model_revision_external_id,
                model_external_id=model_external_id,
                file_id=seed_model_revision_file.id,
                description="Test revision",
                external_dependencies=external_dependencies,
            )

            model_revision_created = cognite_client.simulators.models.revisions.create(model_revision_to_create)

            assert model_revision_created is not None
            assert model_revision_created.external_id == model_revision_external_id
            assert model_revision_created.external_dependencies is not None
            assert isinstance(model_revision_created.external_dependencies[0].file, SimulatorModelDependencyFileId)
            assert isinstance(external_dependencies[0].file, SimulatorModelDependencyFileId)
            assert model_revision_created.external_dependencies[0].file.id == external_dependencies[0].file.id
        finally:
            cognite_client.simulators.models.delete(external_ids=[model_external_id])

    def test_update_model(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        model_external_id = random_string(10)
        models_to_create = SimulatorModelWrite(
            name="sdk-test-model1",
            simulator_external_id=seed_resource_names.simulator_external_id,
            external_id=model_external_id,
            data_set_id=seed_resource_names.simulator_test_data_set_id,
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

    def test_model_revision_retrieve_data(
        self, cognite_client: CogniteClient, seed_resource_names: ResourceNames
    ) -> None:
        revision_data = cognite_client.simulators._post(
            "/simulators/models/revisions/data/update",
            headers={"cdf-version": "alpha"},
            json={
                "items": [
                    {
                        "modelRevisionExternalId": seed_resource_names.simulator_model_revision_external_id,
                        "update": {"flowsheets": {"set": SIMULATOR_MODEL_REVISION_DATA_FLOWSHEET}},
                    }
                ]
            },
        )

        assert revision_data.status_code == 200

        model_revisions = cognite_client.simulators.models.revisions.list(
            model_external_ids=seed_resource_names.simulator_model_external_id
        )

        model_revision_data_item = model_revisions[0].get_data()
        assert model_revision_data_item is not None

        model_revision_data_list = cognite_client.simulators.models.revisions.retrieve_data(
            model_revision_external_id=model_revisions[0].external_id
        )
        assert model_revision_data_item == model_revision_data_list[0]
        assert model_revision_data_item.flowsheets is not None
        assert (
            model_revision_data_item.flowsheets[0].dump()
            == SimulatorFlowsheet._load(
                SIMULATOR_MODEL_REVISION_DATA_FLOWSHEET[0], cognite_client=cognite_client
            ).dump()
        )
