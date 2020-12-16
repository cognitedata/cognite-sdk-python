import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, FileMetadata, ThreeDModelRevisionUpdate, ThreeDModelUpdate
from cognite.client.data_classes.three_d import ThreeDAssetMapping, ThreeDModelRevision

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def new_file_id():
    metadata, url = COGNITE_CLIENT.files.create(file_metadata=FileMetadata(name="threeDtestFile"))
    yield metadata.id
    COGNITE_CLIENT.files.delete(id=metadata.id)
    assert COGNITE_CLIENT.files.retrieve(id=metadata.id) is None


@pytest.fixture(scope="class")
def new_file():
    res = COGNITE_CLIENT.files.upload_bytes(content="blabla", name="myspecialfile.txt")
    while True:
        if COGNITE_CLIENT.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)
    assert COGNITE_CLIENT.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def test_revision(new_model, new_file):
    # model_id = COGNITE_CLIENT.three_d.models.list(published=True, limit=1)[0].id
    revision = COGNITE_CLIENT.three_d.revisions.create(new_model.id, ThreeDModelRevision(file_id=new_file.id))
    # revision = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id, limit=1)[0]
    return revision, new_model.id


@pytest.fixture(scope="class")
def new_model():
    res = COGNITE_CLIENT.three_d.models.create(name="NewTestModel")
    yield res
    COGNITE_CLIENT.three_d.models.delete(id=res.id)
    assert COGNITE_CLIENT.three_d.models.retrieve(id=res.id) is None


@pytest.fixture
def new_asset():
    ts = COGNITE_CLIENT.assets.create(Asset(name="any", description="haha", metadata={"a": "b"}))
    yield ts
    COGNITE_CLIENT.assets.delete(id=ts.id)
    assert COGNITE_CLIENT.assets.retrieve(ts.id) is None


class TestThreeDModelsAPI:
    # def test_create(self, test_revision, new_asset):
    #     revision, model_id = test_revision
    #     node_id = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)[0].id
    #     COGNITE_CLIENT.three_d.asset_mappings.create(model_id=model_id, revision_id=revision.id, asset_mapping=ThreeDAssetMapping(node_id=node_id, asset_id=new_asset.id, tree_index=1234, subtree_size=2345))

    def test_list_and_retrieve(self):
        res = COGNITE_CLIENT.three_d.models.list(limit=1)
        assert 1 == len(res)
        res = [r for r in COGNITE_CLIENT.three_d.models(limit=None) if r.name == "MyModel775"][0]
        assert res == COGNITE_CLIENT.three_d.models.retrieve(res.id)

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
    # @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_list_and_retrieve(self, test_revision):
        revision, model_id = test_revision
        assert revision == COGNITE_CLIENT.three_d.revisions.retrieve(model_id=model_id, id=revision.id)
        res = COGNITE_CLIENT.three_d.revisions.list(model_id=model_id)
        assert len(res) > 0
        res = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)
        assert len(res) > 0

    # @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_list_ancestor_nodes(self, test_revision):
        revision, model_id = test_revision
        node_id = COGNITE_CLIENT.three_d.revisions.list_nodes(model_id=model_id, revision_id=revision.id)[0].id
        res = COGNITE_CLIENT.three_d.revisions.list_ancestor_nodes(
            model_id=model_id, revision_id=revision.id, node_id=node_id
        )
        assert len(res) > 0

    # @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_update_with_resource(self, test_revision):
        revision, model_id = test_revision
        revision.metadata = {"key": "value"}
        COGNITE_CLIENT.three_d.revisions.update(model_id, revision)

    # @pytest.mark.skip(reason="missing a 3d model to test revision against")
    def test_partial_update(self, test_revision):
        revision, model_id = test_revision
        added_metadata = {"key": "value"}
        revision_update = ThreeDModelRevisionUpdate(id=revision.id).metadata.add(added_metadata)
        COGNITE_CLIENT.three_d.revisions.update(model_id=model_id, item=revision_update)


# class TestThreeDFilesAPI:
# @pytest.mark.skip(reason="missing a 3d model to test revision against")
# def test_retrieve(self, test_revision):
#     revision, model_id = test_revision
#     project = COGNITE_CLIENT._config.project
#     url = "/api/v1/projects/{}/3d/reveal/models/{}/revisions/{}".format(project, model_id, revision.id)
#     response = COGNITE_CLIENT.get(url=url)
#     threedFileId = response.json()["sceneThreedFiles"][0]["fileId"]
#
#     res = COGNITE_CLIENT.three_d.files.retrieve(threedFileId)
#     assert len(res) > 0
