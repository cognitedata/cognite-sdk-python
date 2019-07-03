import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    ThreeDAssetMapping,
    ThreeDModelRevision,
    ThreeDModelRevisionUpdate,
    ThreeDModelUpdate,
)

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def test_revision():
    model_id = COGNITE_CLIENT.three_d.models.list(limit=1)[0].id
    revision = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id, limit=1)[0]
    return revision, model_id


@pytest.fixture(scope="class")
def new_model():
    res = COGNITE_CLIENT.three_d.models.create(name="NewTestModel")
    yield res
    COGNITE_CLIENT.three_d.models.delete(id=res.id)
    assert COGNITE_CLIENT.three_d.models.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def new_revision():
    model_id = COGNITE_CLIENT.three_d.models.list(limit=1)[0].id
    revision = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id, limit=1)[0]

    new_revision = ThreeDModelRevision(file_id=revision.file_id)
    res = COGNITE_CLIENT.three_d.revisions.create(model_id=model_id, revision=new_revision)
    yield res, model_id
    COGNITE_CLIENT.three_d.revisions.delete(model_id=model_id, id=res.id)
    assert COGNITE_CLIENT.three_d.revisions.retrieve(model_id=model_id, id=res.id) is None


@pytest.fixture(scope="class")
def new_asset_mapping():
    model_id = COGNITE_CLIENT.three_d.models.list(limit=1)[0].id
    revision = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id, limit=1)[0]
    node_id = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)[0].id
    asset_id = COGNITE_CLIENT.assets.list(limit=1)[0].id
    asset_mapping = ThreeDAssetMapping(node_id=node_id, asset_id=asset_id)
    res = COGNITE_CLIENT.three_d.asset_mappings.create(
        model_id=model_id, revision_id=revision.id, asset_mapping=asset_mapping
    )
    yield res, model_id, revision.id

    COGNITE_CLIENT.three_d.asset_mappings.delete(model_id=model_id, revision_id=revision.id, asset_mapping=res)
    assert COGNITE_CLIENT.three_d.asset_mappings.list(model_id=model_id, revision_id=revision.id) == []


class TestThreeDModelsAPI:
    def test_list(self):
        res = COGNITE_CLIENT.three_d.models.list(limit=1)
        assert 1 == len(res)

    def test_retrieve(self):
        res = COGNITE_CLIENT.three_d.models.list(limit=1)
        assert res[0] == COGNITE_CLIENT.three_d.models.retrieve(res[0].id)

    def test_update_with_resource(self, new_model):
        model = new_model
        model.name = "NewTestModelVer2"
        res = COGNITE_CLIENT.three_d.models.update(model)
        assert model.name == res.name

    def test_partial_update(self, new_model):
        added_metadata = {"key": "value"}
        update = ThreeDModelUpdate(id=new_model.id).metadata.add(added_metadata)
        res = COGNITE_CLIENT.three_d.models.update(update)
        assert added_metadata == res.metadata


class TestThreeDRevisionsAPI:
    def test_list(self):
        model_id = COGNITE_CLIENT.three_d.models.list(limit=1)[0].id
        res = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id)
        assert len(res) > 0

    def test_retrieve(self, test_revision):
        revision, model_id = test_revision
        assert revision == COGNITE_CLIENT.three_d.revisions.retrieve(model_id=model_id, id=revision.id)

    def test_list_nodes(self, test_revision):
        revision, model_id = test_revision
        res = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)
        assert len(res) > 0

    def test_list_ancestor_nodes(self, test_revision):
        revision, model_id = test_revision
        node_id = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)[0].id
        res = COGNITE_CLIENT.three_d.revisions.list_ancestor_nodes(
            model_id=model_id, revision_id=revision.id, node_id=node_id
        )
        assert len(res) > 0

    # def test_update_with_resource(self, new_revision):
    #     revision, model_id = new_revision
    #     revision.metadata = {"key": "value"}
    #     res = COGNITE_CLIENT.three_d.revisions.update(model_id, revision)
    #     assert revision.metadata == res.metadata

    def test_partial_update(self, new_revision):
        revision, model_id = new_revision
        added_metadata = {"key": "value"}
        revision_update = ThreeDModelRevisionUpdate(id=revision.id).metadata.add(added_metadata)
        res = COGNITE_CLIENT.three_d.revisions.update(model_id=model_id, item=revision_update)
        assert added_metadata == res.metadata


# class TestThreeDFilesAPI:
#     def test_retrieve(self, test_revision):
#         revision, _ = test_revision
#         res = COGNITE_CLIENT.three_d.files.retrieve(revision.file_id)
#         assert len(res)  > 0


class TestThreeDAssetMappingAPI:
    def test_list(self, new_asset_mapping):
        asset_mapping, model_id, revision_id = new_asset_mapping
        res = COGNITE_CLIENT.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id, limit=1)[0]
        assert asset_mapping.node_id == res.node_id
        assert asset_mapping.asset_id == res.asset_id
