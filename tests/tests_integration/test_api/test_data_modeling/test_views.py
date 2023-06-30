import json
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    MappedPropertyApply,
    Space,
    View,
    ViewApply,
    ViewId,
    ViewList,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture()
def movie_views(cognite_client: CogniteClient) -> ViewList:
    movie_view_ids = [
        ViewId(space="IntegrationTestsImmutable", external_id="Actor", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="BestDirector", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="BestLeadingActor", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="BestLeadingActress", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Director", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Nomination", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Person", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Rating", version="2"),
        ViewId(space="IntegrationTestsImmutable", external_id="Role", version="2"),
    ]

    movie_views = cognite_client.data_modeling.views.retrieve(ids=movie_view_ids)
    assert len(movie_view_ids) == len(movie_views), "Some of the movie views are missing, please recreate them."
    return movie_views


class TestViewsAPI:
    def test_list(self, cognite_client: CogniteClient, movie_views: ViewList, integration_test_space: Space) -> None:
        # Arrange
        expected_views = ViewList([v for v in movie_views if v.space == integration_test_space.space])

        # Act
        actual_views = cognite_client.data_modeling.views.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_views, key=lambda v: v.external_id) == sorted(expected_views, key=lambda v: v.external_id)
        assert all(v.space == integration_test_space.space for v in actual_views)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        # Arrange
        new_view = ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestView",
            version="v1",
            description="Integration test, should not persist",
            name="Create and delete view",
            properties={
                "name": MappedPropertyApply(
                    container=ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_id = ViewId(new_view.space, new_view.external_id, new_view.version)

        # Act
        created = cognite_client.data_modeling.views.apply(new_view)
        retrieved = cognite_client.data_modeling.views.retrieve(new_id)[0]

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert created.as_apply().dump() == new_view.dump()
        assert retrieved.dump() == created.dump()

        # Act
        deleted_id = cognite_client.data_modeling.views.delete(new_id)
        retrieved_deleted = cognite_client.data_modeling.views.retrieve(new_id)

        # Assert
        assert deleted_id[0] == new_id
        assert not retrieved_deleted

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.views.delete(ViewId(space=space, external_id="DoesNotExists", version="v0"))
            == []
        )

    def test_retrieve_without_inherited_properties(self, cognite_client: CogniteClient, movie_views: ViewList) -> None:
        # Arrange
        view = movie_views[0]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(view.as_id(), include_inherited_properties=False)

        # Assert
        assert len(retrieved) == 1

    def test_retrieve_multiple(self, cognite_client: CogniteClient, movie_views: ViewList) -> None:
        # Arrange
        ids = [ViewId(v.space, v.external_id, v.version) for v in movie_views]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids)

        # Assert
        assert [view.as_id() for view in retrieved] == ids

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, movie_views: ViewList) -> None:
        # Arrange
        ids_without_missing = [v.as_id() for v in movie_views]
        ids_with_missing = [*ids_without_missing, ViewId("myNonExistingSpace", "myImaginaryView", "v0")]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids_with_missing)

        # Assert
        assert [view.as_id() for view in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert not cognite_client.data_modeling.views.retrieve(("myNonExistingSpace", "myImaginaryView", "v0"))

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        for containers in cognite_client.data_modeling.views(
            chunk_size=2, limit=-1, space=integration_test_space.space
        ):
            assert isinstance(containers, ViewList)

    def test_apply_invalid_view(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.views.apply(
                ViewApply(
                    space="nonExistingSpace",
                    external_id="myView",
                    version="v1",
                    properties={
                        "name": MappedPropertyApply(
                            container=ContainerId(
                                space=integration_test_space.space,
                                external_id="Person",
                            ),
                            container_property_identifier="name",
                            name="fullName",
                        ),
                    },
                )
            )

        # Assert
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
        # Arrange
        valid_view = ViewApply(
            space=integration_test_space.space,
            external_id="myView",
            version="v1",
            properties={
                "name": MappedPropertyApply(
                    container=ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        invalid_view = ViewApply(
            space="nonExistingSpace",
            external_id="myView",
            version="v1",
            properties={
                "name": MappedPropertyApply(
                    container=ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        monkeypatch.setattr(cognite_client.data_modeling.views, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.views.apply([valid_view, invalid_view])

            # Assert
            assert "One or more spaces do not exist" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            # Cleanup
            cognite_client.data_modeling.views.delete(valid_view.as_id())

    def test_dump_json_serialize_load(self, movie_views: ViewList) -> None:
        # Arrange
        view = movie_views.get(external_id="Movie")
        assert view is not None, "Movie view not found in test environment"

        # Act
        view_dumped = view.dump(camel_case=True)
        view_json = json.dumps(view_dumped)
        view_loaded = View.load(view_json)

        # Assert
        assert view == view_loaded
