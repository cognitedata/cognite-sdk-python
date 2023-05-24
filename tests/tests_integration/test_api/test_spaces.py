from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Space, SpaceList


@pytest.fixture(scope="function")
def cdf_spaces(cognite_client):
    yield cognite_client.models.spaces.list(limit=-1)


def _dump(list_: SpaceList | Space) -> list[dict]:
    if isinstance(list_, Space):
        return [list_.dump()]
    return sorted((s.dump() for s in list_), key=lambda s: s["space"])


class TestSpacesAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_spaces: SpaceList):
        actual_space_in_cdf = cognite_client.models.spaces.list(limit=-1)

        assert _dump(actual_space_in_cdf) == _dump(cdf_spaces)

    def test_create_retrieve_and_delete(self, cognite_client: CogniteClient):
        # Arrange
        my_space = Space(
            space="myNewSpace", name="My New Space", description="This is part of the integration testing for the SDK."
        )

        # Act
        created_space = cognite_client.models.spaces.apply(my_space)
        retrieved_space = cognite_client.models.spaces.retrieve(my_space.space)

        # Assert
        assert retrieved_space.dump() == created_space.dump()
        expected = retrieved_space.dump()
        expected.pop("created_time")
        expected.pop("last_updated_time")
        assert my_space.dump() == expected

        # Act
        cognite_client.models.spaces.delete(my_space.space)

        # Assert
        assert cognite_client.models.spaces.retrieve(space=my_space.space) is None

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_spaces: SpaceList):
        retrieved_spaces = cognite_client.models.spaces.retrieve_multiple([s.space for s in cdf_spaces])

        assert _dump(retrieved_spaces) == _dump(cdf_spaces)

    def test_iterate_over_spaces(self, cognite_client: CogniteClient):
        for space in cognite_client.models.spaces(chunk_size=1):
            assert space

    def test_retrieve_non_existing_space(self, cognite_client: CogniteClient):
        actual_retrieved = cognite_client.models.spaces.retrieve("notExistingSpace")
        assert actual_retrieved is None

    def test_delete_non_existing_space(self, cognite_client: CogniteClient):
        assert cognite_client.models.spaces.delete("notExistingSpace") is None
