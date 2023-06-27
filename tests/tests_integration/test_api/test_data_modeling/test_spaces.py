from __future__ import annotations

from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space, SpaceApply, SpaceList
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="function")
def cdf_spaces(cognite_client: CogniteClient) -> SpaceList:
    spaces = cognite_client.data_modeling.spaces.list(limit=-1)
    assert len(spaces) > 0, "Please create at least one space in CDF."
    return spaces


def _dump(list_: SpaceList | Space) -> list[dict]:
    if isinstance(list_, Space):
        output = [list_.dump()]
    else:
        output = sorted((s.dump() for s in list_), key=lambda s: s["space"])
    for entry in output:
        if "last_updated_time" in entry:
            entry["last_updated_time"] = None
    return output


class TestSpacesAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_spaces: SpaceList) -> None:
        actual_space_in_cdf = cognite_client.data_modeling.spaces.list(limit=-1)

        assert _dump(actual_space_in_cdf) == _dump(cdf_spaces)

    def test_list_include_global(self, cognite_client: CogniteClient, cdf_spaces: SpaceList) -> None:
        spaces = cognite_client.data_modeling.spaces.list(include_global=True)

        assert len(spaces) > len(cdf_spaces)

    def test_create_retrieve_and_delete(self, cognite_client: CogniteClient) -> None:
        # Arrange
        my_space = SpaceApply(
            space="myNewSpace", name="My New Space", description="This is part of the integration testing for the SDK."
        )

        # Act
        created_space = cognite_client.data_modeling.spaces.apply(my_space)
        retrieved_space = cognite_client.data_modeling.spaces.retrieve(my_space.space)

        # Assert
        assert retrieved_space is not None
        assert retrieved_space.dump() == created_space.dump()
        expected = retrieved_space.as_apply().dump()
        assert my_space.dump() == expected

        # Act
        deleted_space = cognite_client.data_modeling.spaces.delete(my_space.space)[0]

        # Assert
        assert deleted_space == my_space.space
        assert cognite_client.data_modeling.spaces.retrieve(space=my_space.space) is None

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_spaces: SpaceList) -> None:
        retrieved_spaces = cognite_client.data_modeling.spaces.retrieve([s.space for s in cdf_spaces])

        assert _dump(retrieved_spaces) == _dump(cdf_spaces)

    def test_iterate_over_spaces(self, cognite_client: CogniteClient) -> None:
        for space in cognite_client.data_modeling.spaces(chunk_size=1):
            assert space

    def test_retrieve_non_existing_space(self, cognite_client: CogniteClient) -> None:
        actual_retrieved = cognite_client.data_modeling.spaces.retrieve("notExistingSpace")
        assert actual_retrieved is None

    def test_delete_non_existing_space(self, cognite_client: CogniteClient) -> None:
        deleted_space = cognite_client.data_modeling.spaces.delete("notExistingSpace")
        assert deleted_space == []

    def test_apply_invalid_space(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.spaces.apply(SpaceApply(space="myInvalidSpace", name="wayTooLong" * 255))

        # Assert
        assert error.value.code == 400
        assert "name size must be between 0 and 255" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
        # Arrange
        valid_space = SpaceApply(
            space="myNewValidSpace",
        )
        invalid_space = SpaceApply(space="myInvalidSpace", name="wayTooLong" * 255)
        monkeypatch.setattr(cognite_client.data_modeling.spaces, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.spaces.apply([valid_space, invalid_space])

            # Assert
            assert "name size must be between 0 and 255" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1

        finally:
            # Cleanup
            cognite_client.data_modeling.spaces.delete(valid_space.as_id())
