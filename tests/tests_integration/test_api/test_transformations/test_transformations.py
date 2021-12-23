import random
import string

import pytest

from cognite.client.data_classes import Transformation, TransformationDestination, TransformationUpdate


@pytest.fixture
def new_transformation(cognite_client):
    prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
    transform = Transformation(
        name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.assets()
    )
    ts = cognite_client.transformations.create(transform)

    yield ts

    cognite_client.transformations.delete(id=ts.id)
    assert cognite_client.transformations.retrieve(ts.id) is None


other_transformation = new_transformation


class TestTransformationsAPI:
    def test_create_asset_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.assets()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_raw_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.raw("myDatabase", "myTable"),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)
        assert ts.destination == TransformationDestination.raw("myDatabase", "myTable")

    def test_create_asset_hierarchy_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.asset_hierarchy()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_string_datapoints_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.string_datapoints(),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create(self, new_transformation):
        assert (
            new_transformation.name == "any"
            and new_transformation.destination == TransformationDestination.assets()
            and new_transformation.id is not None
        )

    def test_retrieve(self, cognite_client, new_transformation):
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            new_transformation.name == retrieved_transformation.name
            and new_transformation.destination == retrieved_transformation.destination
            and new_transformation.id == retrieved_transformation.id
        )

    def test_retrieve_multiple(self, cognite_client, new_transformation, other_transformation):
        retrieved_transformations = cognite_client.transformations.retrieve_multiple(
            ids=[new_transformation.id, other_transformation.id]
        )
        assert len(retrieved_transformations) == 2
        assert new_transformation.id in [
            transformation.id for transformation in retrieved_transformations
        ] and other_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_update_full(self, cognite_client, new_transformation):
        expected_external_id = f"m__{new_transformation.external_id}"
        new_transformation.external_id = expected_external_id
        new_transformation.name = "new name"
        new_transformation.query = "SELECT * from _cdf.assets"
        new_transformation.destination = TransformationDestination.raw("myDatabase", "myTable")
        updated_transformation = cognite_client.transformations.update(new_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
            and updated_transformation.destination == TransformationDestination.raw("myDatabase", "myTable")
        )

    def test_update_partial(self, cognite_client, new_transformation):
        expected_external_id = f"m__{new_transformation.external_id}"
        update_transformation = (
            TransformationUpdate(id=new_transformation.id)
            .external_id.set(expected_external_id)
            .name.set("new name")
            .query.set("SELECT * from _cdf.assets")
        )
        updated_transformation = cognite_client.transformations.update(update_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
        )

    def test_list(self, cognite_client, new_transformation):
        retrieved_transformations = cognite_client.transformations.list(limit=None)
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_preview(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        assert (
            query_result.schema is not None
            and query_result.results is not None
            and len(query_result.schema) == 2
            and len(query_result.results) == 1
            and query_result.results[0]["id"] == 1
            and query_result.results[0]["name"] == "asd"
        )

    def test_preview_to_string(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        dumped = str(query_result)
