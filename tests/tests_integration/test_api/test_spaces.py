import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Space, SpaceList


@pytest.fixture(scope="session")
def cdf_spaces(cognite_client):
    preexisting_spaces = cognite_client.models.spaces.list(limit=-1)
    description = "This is part of the integration testing for the SDK."
    # Creating a few new to ensure there at least two available spaces
    test_spaces = SpaceList(
        [
            Space(space="mySpace", name="My Space", description=description),
            Space(space="myOtherSpace", name="My Other Space", description=description),
        ]
    )
    test_spaces = cognite_client.models.spaces.apply(test_spaces)

    yield test_spaces + preexisting_spaces

    # Cleanup
    cognite_client.models.spaces.delete([s.space for s in test_spaces])


class TestSpacesAPI:
    @pytest.mark.skip("In development")
    def test_list(self, cognite_client: CogniteClient, cdf_spaces: SpaceList):
        actual_space_in_cdf = cognite_client.models.spaces.list(limit=-1)

        assert actual_space_in_cdf == cdf_spaces

    @pytest.mark.skip("In development")
    def test_create_retrieve_and_delete(self, cognite_client: CogniteClient):
        # Arrange
        my_space = Space(
            space="myNewSpace", name="My New Space", description="This is part of the integration testing for the SDK."
        )

        # Act
        cognite_client.models.spaces.apply(my_space)
        cdf_space = cognite_client.models.spaces.retrieve(my_space.id)

        # Assert
        assert cdf_space == my_space

        # Act
        cognite_client.models.spaces.delete(my_space.id)

        # Assert
        all_spaces = cognite_client.models.spaces.list(limit=-1)
        assert all(s.id != my_space.id for s in all_spaces)
