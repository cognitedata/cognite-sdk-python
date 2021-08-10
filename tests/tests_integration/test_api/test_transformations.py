import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Transformation, TransformationDestination, TransformationUpdate

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_transformation():
    transform = Transformation(name="any", destination=TransformationDestination.raw())
    ts = COGNITE_CLIENT.transformations.create(transform)

    yield ts

    COGNITE_CLIENT.transformations.delete(id=ts.id)
    assert COGNITE_CLIENT.transformations.retrieve(ts.id) is None


class TestTransformationsAPI:
    def test_create_asset_transformation(self):
        transform = Transformation(name="any", destination=TransformationDestination.assets())
        ts = COGNITE_CLIENT.transformations.create(transform)
        COGNITE_CLIENT.transformations.delete(id=ts.id)

    def test_create(self, new_transformation):
        assert (
            new_transformation.name == "any"
            and new_transformation.destination.type == "raw_table"
            and new_transformation.destination.rawType == "plain_raw"
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
