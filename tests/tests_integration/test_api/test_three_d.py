from __future__ import annotations

from contextlib import suppress

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import ThreeDModel, ThreeDModelRevisionUpdate, ThreeDModelUpdate, ThreeDModelWrite
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="class")
def test_model_name():
    return f"NewTestModel-{random_string(6)}"


@pytest.fixture(scope="class")
def test_revision(cognite_client):
    model_id = cognite_client.three_d.models.list(published=True, limit=1)[0].id
    revision = cognite_client.three_d.revisions.list(model_id=model_id, limit=1)[0]
    return revision, model_id


@pytest.fixture(scope="class")
def new_model(cognite_client: CogniteClient, test_model_name: str) -> ThreeDModel:
    res = cognite_client.three_d.models.create(name=test_model_name)
    yield res
    cognite_client.three_d.models.delete(id=res.id)
    assert cognite_client.three_d.models.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def test_nodes_tree_index_order(cognite_client):
    revision, model_id = test_revision
    return cognite_client.three_d.list_nodes(model_id=model_id, revision_id=revision.id)


@pytest.mark.usefixtures("new_model")
class TestThreeDModelsAPI:
    def test_list_and_retrieve(self, cognite_client, test_model_name):
        res = cognite_client.three_d.models.list(limit=1)
        assert 1 == len(res)
        res = next(r for r in cognite_client.three_d.models(limit=None) if r.name == test_model_name)
        assert res == cognite_client.three_d.models.retrieve(res.id)

    def test_create_update_delete(self, cognite_client) -> None:
        my_model = ThreeDModelWrite(name="MyModel775", metadata={"key": "value"})

        created: ThreeDModel | None = None
        try:
            created = cognite_client.three_d.models.create(my_model)
            assert created.as_write().dump() == my_model.dump()

            update = ThreeDModelUpdate(id=created.id).metadata.add({"key2": "value2"})

            updated = cognite_client.three_d.models.update(update)

            assert updated.metadata == {"key": "value", "key2": "value2"}

            cognite_client.three_d.models.delete(id=created.id)
        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.three_d.models.delete(id=created.id)

    def test_update_with_resource(self, new_model, cognite_client):
        model = new_model
        model.name = "NewTestModelVer2"
        res = cognite_client.three_d.models.update(model)
        assert model.name == res.name

    def test_partial_update(self, new_model, cognite_client):
        added_metadata = {"key": "value"}
        update = ThreeDModelUpdate(id=new_model.id).metadata.add(added_metadata)
        res = cognite_client.three_d.models.update(update)
        assert added_metadata == res.metadata


class TestThreeDRevisionsAPI:
    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_list_and_retrieve(self, test_revision, cognite_client):
        revision, model_id = test_revision
        assert revision == cognite_client.three_d.revisions.retrieve(model_id=model_id, id=revision.id)
        res = cognite_client.three_d.revisions.list(model_id=model_id)
        assert len(res) > 0
        res = cognite_client.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)
        assert len(res) > 0

    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_list_ancestor_nodes(self, test_revision, cognite_client):
        revision, model_id = test_revision
        node_id = test_nodes_tree_index_order[0].id
        res = cognite_client.three_d.revisions.list_ancestor_nodes(
            model_id=model_id, revision_id=revision.id, node_id=node_id
        )
        assert len(res) > 0

    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_list_nodes_partitions(self, test_revision, cognite_client):
        revision, model_id = test_revision
        nodes = cognite_client.three_d.revisions.list_nodes(
            model_id=model_id, revision_id=revision.id, partitions=4, sort_by_node_id=True
        )
        assert len(nodes) == len(test_nodes_tree_index_order)
        # assert test_node_tree_index_order[nodes[0].tree_index].id == nodes[0].id

    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_update_with_resource(self, cognite_client, test_revision):
        revision, model_id = test_revision
        revision.metadata = {"key": "value"}
        cognite_client.three_d.revisions.update(model_id, revision)

    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_partial_update(self, cognite_client, test_revision):
        revision, model_id = test_revision
        added_metadata = {"key": "value"}
        revision_update = ThreeDModelRevisionUpdate(id=revision.id).metadata.add(added_metadata)
        cognite_client.three_d.revisions.update(model_id=model_id, item=revision_update)


class TestThreeDFilesAPI:
    @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_retrieve(self, cognite_client, test_revision):
        revision, model_id = test_revision
        project = cognite_client._config.project
        url = f"/api/v1/projects/{project}/3d/reveal/models/{model_id}/revisions/{revision.id}"
        response = cognite_client.get(url=url)
        threedFileId = response.json()["sceneThreedFiles"][0]["fileId"]

        res = cognite_client.three_d.files.retrieve(threedFileId)
        assert len(res) > 0
