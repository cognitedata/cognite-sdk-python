import random
import string

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Transformation, TransformationDestination, TransformationUpdate, TransformationJob, TransformationJobStatus

COGNITE_CLIENT = CogniteClient()
prefix = "".join(random.choice(string.ascii_letters) for i in range(6))


@pytest.fixture
def new_transformation():
    transform = Transformation(
        name="any",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        query="select id, name from _cdf.assets limit 5000",
        source_api_key=COGNITE_CLIENT.config.api_key,
        destination_api_key=COGNITE_CLIENT.config.api_key,
        ignore_null_fields=True,
    )
    ts = COGNITE_CLIENT.transformations.create(transform)

    yield ts

    COGNITE_CLIENT.transformations.delete(id=ts.id)
    assert COGNITE_CLIENT.transformations.retrieve(ts.id) is None


class TestTransformationsAPI:
    def test_create_asset_transformation(self):
        transform = Transformation(name="any", destination=TransformationDestination.assets())
        ts = COGNITE_CLIENT.transformations.create(transform)
        COGNITE_CLIENT.transformations.delete(id=ts.id)

    def test_create_asset_hierarchy_transformation(self):
        transform = Transformation(name="any", destination=TransformationDestination.asset_hierarchy())
        ts = COGNITE_CLIENT.transformations.create(transform)
        COGNITE_CLIENT.transformations.delete(id=ts.id)

    def test_create_string_datapoints_transformation(self):
        transform = Transformation(name="any", destination=TransformationDestination.string_datapoints())
        ts = COGNITE_CLIENT.transformations.create(transform)
        COGNITE_CLIENT.transformations.delete(id=ts.id)

    def test_create(self, new_transformation):
        assert (
            new_transformation.name == "any"
            and new_transformation.destination.type == "assets"
            and new_transformation.id is not None
        )

    def test_retrieve(self, new_transformation):
        retrieved_transformation = COGNITE_CLIENT.transformations.retrieve(new_transformation.id)
        assert (
            new_transformation.name == retrieved_transformation.name
            and new_transformation.destination.type == retrieved_transformation.destination.type
            and new_transformation.id == retrieved_transformation.id
        )

    def test_update_full(self, new_transformation):
        new_transformation.name = "new name"
        new_transformation.query = "SELECT * from _cdf.assets"
        updated_transformation = COGNITE_CLIENT.transformations.update(new_transformation)
        retrieved_transformation = COGNITE_CLIENT.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
        )

    def test_update_partial(self, new_transformation):
        update_transformation = (
            TransformationUpdate(id=new_transformation.id).name.set("new name").query.set("SELECT * from _cdf.assets")
        )
        updated_transformation = COGNITE_CLIENT.transformations.update(update_transformation)
        retrieved_transformation = COGNITE_CLIENT.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
        )

    def test_list(self, new_transformation):
        retrieved_transformations = COGNITE_CLIENT.transformations.list()
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_run(self, new_transformation: Transformation):
        job = new_transformation.run()

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination_type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    @pytest.mark.asyncio
    async def test_run_async(self, new_transformation: Transformation):
        job = await new_transformation.run_async()

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination_type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    @pytest.mark.asyncio
    async def test_run_by_external_id_async(self, new_transformation: Transformation):
        job = await COGNITE_CLIENT.transformations.run_async(transformation_external_id=f"{prefix}-transformation")

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination_type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )
