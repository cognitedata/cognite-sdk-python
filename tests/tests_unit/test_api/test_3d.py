import re
from urllib.parse import unquote_plus

import pytest

from cognite.client._api.three_d import (
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelUpdate,
    ThreeDNodeList,
)
from cognite.client.data_classes import BoundingBox3D
from cognite.client.exceptions import CogniteAPIError
from tests.utils import jsgz_load


@pytest.fixture
def mock_3d_model_response(rsps, cognite_client):
    response_body = {"items": [{"name": "My Model", "id": 1000, "createdTime": 0}]}
    url_pattern = re.compile(re.escape(cognite_client.three_d._get_base_url_with_base_path()) + "/3d/models.*")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    rsps.assert_all_requests_are_fired = False
    yield rsps


@pytest.fixture
def mock_retrieve_3d_model_response(rsps, cognite_client):
    response_body = {"name": "My Model", "id": 1000, "createdTime": 0}
    rsps.add(
        rsps.GET, cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1", status=200, json=response_body
    )
    yield rsps


class Test3DModels:
    def test_list(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.list(published=True, limit=100)
        assert isinstance(res, ThreeDModelList)
        assert mock_3d_model_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_published_default(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.list(limit=100)
        assert isinstance(res, ThreeDModelList)
        assert mock_3d_model_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert "published" not in mock_3d_model_response.calls[0].request.path_url

    def test_update_with_update_object(self, cognite_client, mock_3d_model_response):
        update = ThreeDModelUpdate(id=1).name.set("bla")
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]
        assert mock_3d_model_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_dataset(self, cognite_client, mock_3d_model_response):
        update = ThreeDModelUpdate(id=1).data_set_id.set(2)
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"dataSetId": {"set": 2}}} == jsgz_load(
            mock_3d_model_response.calls[0].request.body
        )["items"][0]
        assert mock_3d_model_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_reset_dataset(self, cognite_client, mock_3d_model_response):
        update = ThreeDModelUpdate(id=1).data_set_id.set(None)
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"dataSetId": {"setNull": True}}} == jsgz_load(
            mock_3d_model_response.calls[0].request.body
        )["items"][0]
        assert mock_3d_model_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_resource_object(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.update(ThreeDModel(id=1, name="bla", created_time=123))
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]
        assert mock_3d_model_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[0].request.body)
        assert res is None
        res = cognite_client.three_d.models.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[1].request.body)
        assert res is None

    def test_retrieve(self, cognite_client, mock_retrieve_3d_model_response):
        res = cognite_client.three_d.models.retrieve(id=1)
        assert isinstance(res, ThreeDModel)
        assert mock_retrieve_3d_model_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.create(name="My Model")
        assert isinstance(res, ThreeDModel)

        request_body = jsgz_load(mock_3d_model_response.calls[0].request.body)
        assert request_body == {"items": [{"name": "My Model"}]}
        assert mock_3d_model_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_3d_model_response):
        res = cognite_client.three_d.models.create(name=["My Model"])
        assert isinstance(res, ThreeDModelList)

        request_body = jsgz_load(mock_3d_model_response.calls[0].request.body)
        assert request_body == {"items": [{"name": "My Model"}]}
        assert mock_3d_model_response.calls[0].response.json()["items"] == res.dump(camel_case=True)


@pytest.fixture
def mock_3d_model_revision_response(rsps, cognite_client):
    response_body = {
        "items": [
            {
                "id": 1,
                "fileId": 1000,
                "published": False,
                "rotation": [0, 0, 0],
                "scale": [1, 1, 1],
                "translation": [0, 0, 0],
                "camera": {"target": [0, 0, 0], "position": [0, 0, 0]},
                "status": "Done",
                "thumbnailThreedFileId": 1000,
                "thumbnailUrl": "https://api.cognitedata.com/api/v1/project/myproject/3d/files/1000",
                "assetMappingCount": 0,
                "createdTime": 0,
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(cognite_client.three_d._get_base_url_with_base_path()) + "/3d/models/1/revisions.*"
    )
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    rsps.assert_all_requests_are_fired = False
    yield rsps


@pytest.fixture
def mock_retrieve_3d_model_revision_response(rsps, cognite_client):
    res = {
        "id": 1000,
        "fileId": 1000,
        "published": False,
        "rotation": [0, 0, 0],
        "scale": [1, 1, 1],
        "translation": [0, 0, 0],
        "camera": {"target": [0, 0, 0], "position": [0, 0, 0]},
        "status": "Done",
        "thumbnailThreedFileId": 1000,
        "thumbnailUrl": "https://api.cognitedata.com/api/v1/project/myproject/3d/files/1000",
        "assetMappingCount": 0,
        "createdTime": 0,
    }
    rsps.add(
        rsps.GET,
        cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1",
        status=200,
        json=res,
    )
    yield rsps


@pytest.fixture
def mock_3d_model_revision_thumbnail_response(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1/thumbnail",
        status=200,
        json={},
    )
    yield rsps


@pytest.fixture
def mock_3d_model_revision_node_response(rsps, cognite_client):
    response_body = {
        "items": [
            {
                "id": 1,
                "treeIndex": 3,
                "parentId": 2,
                "depth": 2,
                "name": "Node name",
                "subtreeSize": 4,
                "boundingBox": {"max": [0, 0, 0], "min": [0, 0, 0]},
            }
        ]
    }
    rsps.add(
        rsps.GET,
        cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1/nodes",
        status=200,
        json=response_body,
    )
    rsps.add(
        rsps.POST,
        cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1/nodes/list",
        status=200,
        json=response_body,
    )
    rsps.add(
        rsps.GET,
        cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1/nodes/ancestors",
        status=200,
        json=response_body,
    )
    rsps.assert_all_requests_are_fired = False
    yield rsps


class Test3DModelRevisions:
    def test_list(self, cognite_client, mock_3d_model_revision_response):
        res = cognite_client.three_d.revisions.list(model_id=1, published=True, limit=100)
        assert isinstance(res, ThreeDModelRevisionList)
        assert mock_3d_model_revision_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_update_with_update_object(self, cognite_client, mock_3d_model_revision_response):
        update = ThreeDModelRevisionUpdate(id=1).published.set(False)
        cognite_client.three_d.revisions.update(1, update)
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )["items"][0]

    def test_update_with_resource_object(self, cognite_client, mock_3d_model_revision_response):
        cognite_client.three_d.revisions.update(1, ThreeDModelRevision(id=1, published=False, created_time=123))
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )["items"][0]

    def test_delete(self, cognite_client, mock_3d_model_revision_response):
        res = cognite_client.three_d.revisions.delete(1, id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_revision_response.calls[0].request.body)
        assert res is None
        res = cognite_client.three_d.revisions.delete(1, id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_revision_response.calls[1].request.body)
        assert res is None

    def test_retrieve(self, cognite_client, mock_retrieve_3d_model_revision_response):
        res = cognite_client.three_d.revisions.retrieve(model_id=1, id=1)
        assert isinstance(res, ThreeDModelRevision)
        assert mock_retrieve_3d_model_revision_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_3d_model_revision_response):
        res = cognite_client.three_d.revisions.create(model_id=1, revision=ThreeDModelRevision(file_id=123))
        assert isinstance(res, ThreeDModelRevision)
        assert {"items": [{"fileId": 123, "published": False}]} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )
        assert mock_3d_model_revision_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_3d_model_revision_response):
        res = cognite_client.three_d.revisions.create(model_id=1, revision=[ThreeDModelRevision(file_id=123)])
        assert isinstance(res, ThreeDModelRevisionList)
        assert {"items": [{"fileId": 123, "published": False}]} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )
        assert mock_3d_model_revision_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_update_thumbnail(self, cognite_client, mock_3d_model_revision_thumbnail_response):
        res = cognite_client.three_d.revisions.update_thumbnail(model_id=1, revision_id=1, file_id=1)
        assert {"fileId": 1} == jsgz_load(mock_3d_model_revision_thumbnail_response.calls[0].request.body)
        assert res is None

    def test_list_3d_nodes(self, cognite_client, mock_3d_model_revision_node_response):
        res = cognite_client.three_d.revisions.list_nodes(model_id=1, revision_id=1, node_id=None, depth=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert mock_3d_model_revision_node_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_filter_3d_nodes(self, cognite_client, mock_3d_model_revision_node_response):
        res = cognite_client.three_d.revisions.filter_nodes(
            model_id=1, revision_id=1, properties={"Item": {"Type": ["Group"]}}, limit=10
        )
        assert isinstance(res, ThreeDNodeList)
        assert mock_3d_model_revision_node_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_3d_ancestor_nodes(self, cognite_client, mock_3d_model_revision_node_response):
        res = cognite_client.three_d.revisions.list_ancestor_nodes(model_id=1, revision_id=1, node_id=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert mock_3d_model_revision_node_response.calls[0].response.json()["items"] == res.dump(camel_case=True)


class Test3DFiles:
    @pytest.fixture
    def mock_3d_files_response(self, cognite_client, rsps):
        rsps.add(rsps.GET, cognite_client.three_d._get_base_url_with_base_path() + "/3d/files/1", body="bla")

    def test_retrieve(self, cognite_client, mock_3d_files_response):
        assert b"bla" == cognite_client.three_d.files.retrieve(1)


class Test3DAssetMappings:
    @pytest.fixture
    def mock_3d_asset_mappings_response(self, cognite_client, rsps):
        response_body = {"items": [{"nodeId": 1003, "assetId": 3001, "treeIndex": 5, "subtreeSize": 7}]}
        url_pattern = re.compile(
            re.escape(cognite_client.three_d._get_base_url_with_base_path()) + "/3d/models/1/revisions/1/mappings.*"
        )

        rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
        rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
        rsps.assert_all_requests_are_fired = False
        yield rsps

    def test_list(self, cognite_client, mock_3d_asset_mappings_response):
        res = cognite_client.three_d.asset_mappings.list(
            model_id=1, revision_id=1, node_id=None, asset_id=None, intersects_bounding_box=None, limit=None
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

        url = mock_3d_asset_mappings_response.calls[0].request.url
        assert "intersectsBoundingBox" not in unquote_plus(url)

    def test_list__with_intersects_bounding_box(self, cognite_client, mock_3d_asset_mappings_response):
        bbox = BoundingBox3D(min=[0.0, 0.0, 0.0], max=[1.0, 1.0, 1.0])
        res = cognite_client.three_d.asset_mappings.list(
            model_id=1, revision_id=1, node_id=None, asset_id=None, intersects_bounding_box=bbox, limit=None
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

        url = mock_3d_asset_mappings_response.calls[0].request.url
        assert 'intersectsBoundingBox={"max": [1.0, 1.0, 1.0], "min": [0.0, 0.0, 0.0]}' in unquote_plus(url)

    def test_create(self, cognite_client, mock_3d_asset_mappings_response):
        res = cognite_client.three_d.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=ThreeDAssetMapping(node_id=1, asset_id=1)
        )
        assert isinstance(res, ThreeDAssetMapping)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_3d_asset_mappings_response):
        res = cognite_client.three_d.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMapping(node_id=1, asset_id=1)]
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_3d_asset_mappings_response):
        res = cognite_client.three_d.asset_mappings.delete(
            model_id=1, revision_id=1, asset_mapping=ThreeDAssetMapping(1, 1)
        )
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.calls[0].request.body)[
            "items"
        ]

    def test_delete_multiple(self, cognite_client, mock_3d_asset_mappings_response):
        res = cognite_client.three_d.asset_mappings.delete(
            model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMapping(1, 1)]
        )
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.calls[0].request.body)[
            "items"
        ]

    def test_delete_fails(self, cognite_client, rsps):
        rsps.add(
            rsps.POST,
            cognite_client.three_d._get_base_url_with_base_path() + "/3d/models/1/revisions/1/mappings/delete",
            status=500,
            json={"error": {"message": "Server Error", "code": 500}},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.three_d.asset_mappings.delete(
                model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMapping(1, 1)]
            )
        assert e.value.unknown == [ThreeDAssetMapping.load({"assetId": 1, "nodeId": 1})]
