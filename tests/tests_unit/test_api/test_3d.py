import re

import pytest

from cognite.client import CogniteClient
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
    ThreeDRevealNodeList,
    ThreeDRevealRevision,
    ThreeDRevealSectorList,
)
from tests.utils import jsgz_load

THREE_D_API = CogniteClient().three_d


@pytest.fixture
def mock_3d_model_response(rsps):
    response_body = {"data": {"items": [{"name": "My Model", "id": 1000, "createdTime": 0}]}}
    url_pattern = re.compile(re.escape(THREE_D_API._base_url) + "/3d/models.*")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    rsps.assert_all_requests_are_fired = False
    yield rsps


@pytest.fixture
def mock_retrieve_3d_model_response(rsps):
    response_body = {"name": "My Model", "id": 1000, "createdTime": 0}
    rsps.add(rsps.GET, THREE_D_API._base_url + "/3d/models/1", status=200, json=response_body)
    yield rsps


class Test3DModels:
    def test_list(self, mock_3d_model_response):
        res = THREE_D_API.models.list(published=True, limit=100)
        assert isinstance(res, ThreeDModelList)
        assert mock_3d_model_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_update_with_update_object(self, mock_3d_model_response):
        update = ThreeDModelUpdate(id=1).name.set("bla")
        res = THREE_D_API.models.update(update)
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]
        assert mock_3d_model_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_with_resource_object(self, mock_3d_model_response):
        res = THREE_D_API.models.update(ThreeDModel(id=1, name="bla", created_time=123))
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]
        assert mock_3d_model_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_delete(self, mock_3d_model_response):
        res = THREE_D_API.models.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[0].request.body)
        assert res is None
        res = THREE_D_API.models.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[1].request.body)
        assert res is None

    def test_get(self, mock_retrieve_3d_model_response):
        res = THREE_D_API.models.get(id=1)
        assert isinstance(res, ThreeDModel)
        assert mock_retrieve_3d_model_response.calls[0].response.json() == res.dump(camel_case=True)


@pytest.fixture
def mock_3d_model_revision_response(rsps):
    response_body = {
        "data": {
            "items": [
                {
                    "id": 1,
                    "fileId": 1000,
                    "published": False,
                    "rotation": [0, 0, 0],
                    "camera": {"target": [0, 0, 0], "position": [0, 0, 0]},
                    "status": "Done",
                    "thumbnailThreedFileId": 1000,
                    "thumbnailUrl": "https://api.cognitedata.com/api/v1/project/myproject/3d/files/1000",
                    "assetMappingCount": 0,
                    "createdTime": 0,
                }
            ]
        }
    }
    url_pattern = re.compile(re.escape(THREE_D_API._base_url) + "/3d/models/1/revisions.*")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    rsps.assert_all_requests_are_fired = False
    yield rsps


@pytest.fixture
def mock_retrieve_3d_model_revision_response(rsps):
    res = {
        "id": 1000,
        "fileId": 1000,
        "published": False,
        "rotation": [0, 0, 0],
        "camera": {"target": [0, 0, 0], "position": [0, 0, 0]},
        "status": "Done",
        "thumbnailThreedFileId": 1000,
        "thumbnailUrl": "https://api.cognitedata.com/api/v1/project/myproject/3d/files/1000",
        "assetMappingCount": 0,
        "createdTime": 0,
    }
    rsps.add(rsps.GET, THREE_D_API._base_url + "/3d/models/1/revisions/1", status=200, json=res)
    yield rsps


@pytest.fixture
def mock_3d_model_revision_thumbnail_response(rsps):
    rsps.add(rsps.POST, THREE_D_API._base_url + "/3d/models/1/revisions/1/thumbnail", status=200, json={})
    yield rsps


@pytest.fixture
def mock_3d_model_revision_node_response(rsps):
    response_body = {
        "data": {
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
    }
    rsps.add(rsps.GET, THREE_D_API._base_url + "/3d/models/1/revisions/1/nodes", status=200, json=response_body)
    rsps.add(
        rsps.GET, THREE_D_API._base_url + "/3d/models/1/revisions/1/nodes/ancestors", status=200, json=response_body
    )
    rsps.assert_all_requests_are_fired = False
    yield rsps


class Test3DModelRevisions:
    def test_list(self, mock_3d_model_revision_response):
        res = THREE_D_API.revisions.list(model_id=1, published=True, limit=100)
        assert isinstance(res, ThreeDModelRevisionList)
        assert mock_3d_model_revision_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_update_with_update_object(self, mock_3d_model_revision_response):
        update = ThreeDModelRevisionUpdate(id=1).published.set(False)
        THREE_D_API.revisions.update(1, update)
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )["items"][0]

    def test_update_with_resource_object(self, mock_3d_model_revision_response):
        THREE_D_API.revisions.update(1, ThreeDModelRevision(id=1, published=False, created_time=123))
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.calls[0].request.body
        )["items"][0]

    def test_delete(self, mock_3d_model_revision_response):
        res = THREE_D_API.revisions.delete(1, id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_revision_response.calls[0].request.body)
        assert res is None
        res = THREE_D_API.revisions.delete(1, id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_revision_response.calls[1].request.body)
        assert res is None

    def test_get(self, mock_retrieve_3d_model_revision_response):
        res = THREE_D_API.revisions.get(model_id=1, id=1)
        assert isinstance(res, ThreeDModelRevision)
        assert mock_retrieve_3d_model_revision_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_create(self, mock_3d_model_revision_response):
        res = THREE_D_API.revisions.create(model_id=1, revision=ThreeDModelRevision(file_id=123))
        assert isinstance(res, ThreeDModelRevision)
        assert {"items": [{"fileId": 123}]} == jsgz_load(mock_3d_model_revision_response.calls[0].request.body)
        assert mock_3d_model_revision_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_3d_model_revision_response):
        res = THREE_D_API.revisions.create(model_id=1, revision=[ThreeDModelRevision(file_id=123)])
        assert isinstance(res, ThreeDModelRevisionList)
        assert {"items": [{"fileId": 123}]} == jsgz_load(mock_3d_model_revision_response.calls[0].request.body)
        assert mock_3d_model_revision_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_update_thumbnail(self, mock_3d_model_revision_thumbnail_response):
        res = THREE_D_API.revisions.update_thumbnail(model_id=1, revision_id=1, file_id=1)
        assert {"fileId": 1} == jsgz_load(mock_3d_model_revision_thumbnail_response.calls[0].request.body)
        assert res is None

    def test_list_3d_nodes(self, mock_3d_model_revision_node_response):
        res = THREE_D_API.revisions.list_nodes(model_id=1, revision_id=1, node_id=None, depth=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert mock_3d_model_revision_node_response.calls[0].response.json()["data"]["items"] == res.dump(
            camel_case=True
        )

    def test_list_3d_ancestor_nodes(self, mock_3d_model_revision_node_response):
        res = THREE_D_API.revisions.list_ancestor_nodes(model_id=1, revision_id=1, node_id=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert mock_3d_model_revision_node_response.calls[0].response.json()["data"]["items"] == res.dump(
            camel_case=True
        )


class Test3DFiles:
    @pytest.fixture
    def mock_3d_files_response(self, rsps):
        rsps.add(rsps.GET, THREE_D_API._base_url + "/3d/files/1", body="bla")

    def test_get(self, mock_3d_files_response):
        assert b"bla" == THREE_D_API.files.get(1)


class Test3DAssetMappings:
    @pytest.fixture
    def mock_3d_asset_mappings_response(self, rsps):
        response_body = {"data": {"items": [{"nodeId": 1003, "assetId": 3001, "treeIndex": 5, "subtreeSize": 7}]}}
        url_pattern = re.compile(re.escape(THREE_D_API._base_url) + "/3d/models/1/revisions/1/mappings.*")

        rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
        rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
        rsps.assert_all_requests_are_fired = False
        yield rsps

    def test_list(self, mock_3d_asset_mappings_response):
        res = THREE_D_API.asset_mappings.list(model_id=1, revision_id=1, node_id=None, asset_id=None, limit=None)
        assert isinstance(res, ThreeDAssetMappingList)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_create(self, mock_3d_asset_mappings_response):
        res = THREE_D_API.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=ThreeDAssetMapping(node_id=1, asset_id=1)
        )
        assert isinstance(res, ThreeDAssetMapping)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_3d_asset_mappings_response):
        res = THREE_D_API.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMapping(node_id=1, asset_id=1)]
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert mock_3d_asset_mappings_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_delete(self, mock_3d_asset_mappings_response):
        res = THREE_D_API.asset_mappings.delete(model_id=1, revision_id=1, asset_mapping=ThreeDAssetMapping(1, 1))
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.calls[0].request.body)[
            "items"
        ]

    def test_delete_multiple(self, mock_3d_asset_mappings_response):
        res = THREE_D_API.asset_mappings.delete(model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMapping(1, 1)])
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.calls[0].request.body)[
            "items"
        ]


class Test3DReveal:
    @pytest.fixture
    def mock_get_reveal_revision_response(self, rsps):
        res = {
            "id": 1000,
            "fileId": 1000,
            "published": False,
            "rotation": [0, 0, 0],
            "camera": {"target": [0, 0, 0], "position": [0, 0, 0]},
            "status": "Done",
            "thumbnailThreedFileId": 1000,
            "thumbnailUrl": "https://api.cognitedata.com/api/v1/project/myproject/3d/files/1000",
            "assetMappingCount": 0,
            "createdTime": 0,
            "sceneThreedFiles": [{"version": 1, "fileId": 1000}],
        }
        rsps.add(rsps.GET, THREE_D_API._base_url + "/3d/reveal/models/1/revisions/1", status=200, json=res)
        yield rsps

    def test_get_revision(self, mock_get_reveal_revision_response):
        res = THREE_D_API.reveal.get_revision(model_id=1, revision_id=1)
        assert isinstance(res, ThreeDRevealRevision)
        assert mock_get_reveal_revision_response.calls[0].response.json() == res.dump(camel_case=True)

    @pytest.fixture
    def mock_list_reveal_nodes_response(self, rsps):
        res = {
            "data": {
                "items": [
                    {
                        "id": 1000,
                        "treeIndex": 3,
                        "parentId": 2,
                        "depth": 2,
                        "name": "Node name",
                        "subtreeSize": 4,
                        "boundingBox": {"max": [0, 0, 0], "min": [0, 0, 0]},
                        "sectorId": 1000,
                    }
                ]
            }
        }
        rsps.add(
            rsps.GET,
            re.compile(re.escape(THREE_D_API._base_url) + "/3d/reveal/models/1/revisions/1/nodes.*"),
            status=200,
            json=res,
        )
        yield rsps

    def test_list_nodes(self, mock_list_reveal_nodes_response):
        res = THREE_D_API.reveal.list_nodes(model_id=1, revision_id=1, node_id=None, depth=None, limit=None)
        assert isinstance(res, ThreeDRevealNodeList)
        assert mock_list_reveal_nodes_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_list_ancestor_nodes(self, mock_list_reveal_nodes_response):
        res = THREE_D_API.reveal.list_ancestor_nodes(model_id=1, revision_id=1, node_id=None, limit=None)
        assert isinstance(res, ThreeDRevealNodeList)
        assert mock_list_reveal_nodes_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    @pytest.fixture
    def mock_list_reveal_sectors_response(self, rsps):
        res = {
            "data": {
                "items": [
                    {
                        "id": 1000,
                        "parentId": 900,
                        "path": "0/100/500/900/1000",
                        "depth": 4,
                        "boundingBox": {"max": [0, 0, 0], "min": [0, 0, 0]},
                        "threedFiles": [{"version": 1, "fileId": 1000}],
                    }
                ]
            }
        }
        rsps.add(
            rsps.GET,
            re.compile(re.escape(THREE_D_API._base_url) + "/3d/reveal/models/1/revisions/1/sectors"),
            status=200,
            json=res,
        )
        yield rsps

    def test_list_sectors(self, mock_list_reveal_sectors_response):
        res = THREE_D_API.reveal.list_sectors(
            model_id=1, revision_id=1, bounding_box={"max": [1, 1, 1], "min": [0, 0, 0]}, limit=None
        )
        assert isinstance(res, ThreeDRevealSectorList)
        assert "boundingBox=%7B" in mock_list_reveal_sectors_response.calls[0].request.url
        assert mock_list_reveal_sectors_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)
