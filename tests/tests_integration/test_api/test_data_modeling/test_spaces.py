from __future__ import annotations

import pytest

import cognite.client.data_classes.data_modeling as models
from cognite.client import CogniteClient


@pytest.fixture(scope="function")
def cdf_spaces(cognite_client):
    spaces = cognite_client.data_modeling.spaces.list(limit=-1)
    assert len(spaces) > 0, "Please create at least one space in CDF."
    return spaces


def _dump(list_: models.SpaceList | models.Space) -> list[dict]:
    if isinstance(list_, models.Space):
        output = [list_.dump()]
    else:
        output = sorted((s.dump() for s in list_), key=lambda s: s["space"])
    for entry in output:
        if "last_updated_time" in entry:
            entry["last_updated_time"] = None
    return output


class TestSpacesAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_spaces: models.SpaceList):
        actual_space_in_cdf = cognite_client.data_modeling.spaces.list(limit=-1)

        assert _dump(actual_space_in_cdf) == _dump(cdf_spaces)

    def test_create_retrieve_and_delete(self, cognite_client: CogniteClient):
        # Arrange
        my_space = models.Space(
            space="myNewSpace", name="My New Space", description="This is part of the integration testing for the SDK."
        )

        # Act
        created_space = cognite_client.data_modeling.spaces.apply(my_space)
        retrieved_space = cognite_client.data_modeling.spaces.retrieve(my_space.space)

        # Assert
        assert retrieved_space.dump() == created_space.dump()
        expected = retrieved_space.dump()
        expected.pop("created_time")
        expected.pop("last_updated_time")
        assert my_space.dump() == expected

        # Act
        deleted_space = cognite_client.data_modeling.spaces.delete(my_space.space)[0]

        # Assert
        assert deleted_space == my_space.space
        assert cognite_client.data_modeling.spaces.retrieve(space=my_space.space) is None

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_spaces: models.SpaceList):
        retrieved_spaces = cognite_client.data_modeling.spaces.retrieve_multiple([s.space for s in cdf_spaces])

        assert _dump(retrieved_spaces) == _dump(cdf_spaces)

    def test_iterate_over_spaces(self, cognite_client: CogniteClient):
        for space in cognite_client.data_modeling.spaces(chunk_size=1):
            assert space

    def test_retrieve_non_existing_space(self, cognite_client: CogniteClient):
        actual_retrieved = cognite_client.data_modeling.spaces.retrieve("notExistingSpace")
        assert actual_retrieved is None

    def test_delete_non_existing_space(self, cognite_client: CogniteClient):
        deleted_space = cognite_client.data_modeling.spaces.delete("notExistingSpace")
        assert deleted_space == []
