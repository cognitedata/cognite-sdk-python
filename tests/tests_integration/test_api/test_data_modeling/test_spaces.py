from __future__ import annotations

import json
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space, SpaceApply, SpaceList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def cdf_spaces(cognite_client: CogniteClient, integration_test_space: Space) -> SpaceList:
    # The integration test space is created in the fixture integration_test_space.
    # This ensures that at least one space exists in CDF.
    spaces = cognite_client.data_modeling.spaces.list(limit=-1)
    assert len(spaces) > 0, "Please create at least one space in CDF."
    return spaces


class TestSpacesAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_spaces: SpaceList) -> None:
        actual_space_in_cdf = cognite_client.data_modeling.spaces.list(limit=-1)
        assert actual_space_in_cdf.as_apply() == cdf_spaces.as_apply()

    def test_list_include_global(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        spaces_with_global = cognite_client.data_modeling.spaces.list(include_global=True, limit=-1)
        assert any(s.is_global for s in spaces_with_global), "Add at least one global space to CDF for testing."

        spaces_without_global = cognite_client.data_modeling.spaces.list(include_global=False, limit=-1)
        assert all(s.is_global is False for s in spaces_without_global)

    def test_create_retrieve_and_delete(self, cognite_client: CogniteClient) -> None:
        my_space = SpaceApply(
            space=f"myNewSpace-{random_string(10)}",
            name="My New Space",
            description="This is part of the integration testing for the SDK.",
        )
        created_space: Space | None = None
        deleted_spaces: list[str] = []
        try:
            created_space = cognite_client.data_modeling.spaces.apply(my_space)
            retrieved_space = cognite_client.data_modeling.spaces.retrieve(my_space.space)

            assert retrieved_space is not None
            assert retrieved_space.dump() == created_space.dump()
            expected = retrieved_space.as_apply().dump()
            assert my_space.dump() == expected

            deleted_spaces = cognite_client.data_modeling.spaces.delete(my_space.space)
            assert deleted_spaces, "The deleted spaces should be returned."
            assert deleted_spaces[0] == my_space.space
            assert cognite_client.data_modeling.spaces.retrieve(spaces=my_space.space) is None
        finally:
            if created_space and not deleted_spaces:
                cognite_client.data_modeling.spaces.delete(created_space.space)

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_spaces: SpaceList) -> None:
        spaces = cdf_spaces[: min(2, len(cdf_spaces))]

        retrieved_spaces = cognite_client.data_modeling.spaces.retrieve(spaces.as_ids())
        assert retrieved_spaces.as_apply() == spaces.as_apply()

    def test_iterate_over_spaces(self, cognite_client: CogniteClient) -> None:
        for space in cognite_client.data_modeling.spaces:
            assert isinstance(space, Space)

    def test_retrieve_non_existing_space(self, cognite_client: CogniteClient) -> None:
        actual_retrieved = cognite_client.data_modeling.spaces.retrieve("notExistingSpace")
        assert actual_retrieved is None

    def test_delete_non_existing_space(self, cognite_client: CogniteClient) -> None:
        deleted_space = cognite_client.data_modeling.spaces.delete("notExistingSpace")
        assert deleted_space == []

    def test_apply_invalid_space(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.spaces.apply(SpaceApply(space="myInvalidSpace", name="wayTooLong" * 255))

        assert error.value.code == 400
        assert error.value.message == "Name must be at most 255 characters long."

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
        valid_space = SpaceApply(space="myNewValidSpace")
        invalid_space = SpaceApply(space="myInvalidSpace", name="wayTooLong" * 255)
        monkeypatch.setattr(cognite_client.data_modeling.spaces, "_CREATE_LIMIT", 1)
        try:
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.spaces.apply([valid_space, invalid_space])

            assert error.value.message == "Name must be at most 255 characters long."
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            cognite_client.data_modeling.spaces.delete(valid_space.as_id())

    def test_dump_json_serialize_load(self, cdf_spaces: SpaceList) -> None:
        space = cdf_spaces[0]

        space_dump = space.dump(camel_case=True)
        space_json = json.dumps(space_dump)
        space_loaded = Space.load(space_json)
        assert space == space_loaded
