import re
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_plus

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.three_d import (
    BoundingBox3D,
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDAssetMappingWrite,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelRevisionWrite,
    ThreeDModelUpdate,
    ThreeDNodeList,
)
from cognite.client.exceptions import CogniteAPIError
from tests.utils import get_url, jsgz_load

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@pytest.fixture
def expected_items() -> list[dict[str, Any]]:
    return [{"name": "My Model", "id": 1000, "createdTime": 0}]


@pytest.fixture
def mock_3d_model_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, expected_items: list[dict[str, Any]]
) -> HTTPXMock:
    response_body = {"items": expected_items}
    url_pattern = re.compile(re.escape(get_url(cognite_client.three_d)) + "/3d/models.*")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    return httpx_mock


@pytest.fixture
def mock_retrieve_3d_model_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, expected_items: list[dict[str, Any]]
) -> HTTPXMock:
    response_body = expected_items[0]
    httpx_mock.add_response(
        method="GET", url=get_url(cognite_client.three_d, "/3d/models/1"), status_code=200, json=response_body
    )
    return httpx_mock


class Test3DModels:
    def test_list(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        res = cognite_client.three_d.models.list(published=True, limit=100)
        assert isinstance(res, ThreeDModelList)
        assert expected_items == res.dump(camel_case=True)

    def test_list_published_default(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        res = cognite_client.three_d.models.list(limit=100)
        assert isinstance(res, ThreeDModelList)
        assert expected_items == res.dump(camel_case=True)
        assert "published" not in str(mock_3d_model_response.get_requests()[0].url)

    def test_update_with_update_object(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        update = ThreeDModelUpdate(id=1).name.set("bla")
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(
            mock_3d_model_response.get_requests()[0].content
        )["items"][0]
        assert expected_items[0] == res.dump(camel_case=True)

    def test_update_dataset(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        update = ThreeDModelUpdate(id=1).data_set_id.set(2)
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"dataSetId": {"set": 2}}} == jsgz_load(
            mock_3d_model_response.get_requests()[0].content
        )["items"][0]
        assert expected_items[0] == res.dump(camel_case=True)

    def test_reset_dataset(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        update = ThreeDModelUpdate(id=1).data_set_id.set(None)
        res = cognite_client.three_d.models.update(update)
        assert {"id": 1, "update": {"dataSetId": {"setNull": True}}} == jsgz_load(
            mock_3d_model_response.get_requests()[0].content
        )["items"][0]
        assert expected_items[0] == res.dump(camel_case=True)

    def test_update_with_resource_object(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        res = cognite_client.three_d.models.update(
            ThreeDModel(id=1, name="bla", created_time=123, data_set_id=None, metadata=None, cognite_client=None)
        )
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(
            mock_3d_model_response.get_requests()[0].content
        )["items"][0]
        assert expected_items[0] == res.dump(camel_case=True)

    @pytest.mark.parametrize("identifier", [1, [1]])
    def test_delete(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, identifier: int | list[int]
    ) -> None:
        res = cognite_client.three_d.models.delete(id=identifier)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.get_requests()[0].content)
        assert res is None

    def test_retrieve(
        self,
        cognite_client: CogniteClient,
        mock_retrieve_3d_model_response: HTTPXMock,
        expected_items: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.models.retrieve(id=1)
        assert isinstance(res, ThreeDModel)
        assert expected_items[0] == res.dump(camel_case=True)

    def test_create(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        res = cognite_client.three_d.models.create(name="My Model")
        assert isinstance(res, ThreeDModel)

        request_body = jsgz_load(mock_3d_model_response.get_requests()[0].content)
        assert request_body == {"items": [{"name": "My Model"}]}
        assert expected_items[0] == res.dump(camel_case=True)

    def test_create_multiple(
        self, cognite_client: CogniteClient, mock_3d_model_response: HTTPXMock, expected_items: list[dict[str, Any]]
    ) -> None:
        res = cognite_client.three_d.models.create(name=["My Model"])
        assert isinstance(res, ThreeDModelList)

        request_body = jsgz_load(mock_3d_model_response.get_requests()[0].content)
        assert request_body == {"items": [{"name": "My Model"}]}
        assert expected_items == res.dump(camel_case=True)


@pytest.fixture
def expected_items2() -> list[dict[str, Any]]:
    return [
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


@pytest.fixture
def mock_3d_model_revision_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, expected_items2: list[dict[str, Any]]
) -> HTTPXMock:
    response_body = {"items": expected_items2}
    url_pattern = re.compile(re.escape(get_url(cognite_client.three_d)) + "/3d/models/1/revisions.*")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    return httpx_mock


@pytest.fixture
def mock_retrieve_3d_model_revision_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, expected_items2: list[dict[str, Any]]
) -> HTTPXMock:
    res = expected_items2[0]
    res["id"] = 1000
    httpx_mock.add_response(
        method="GET",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1"),
        status_code=200,
        json=res,
    )
    return httpx_mock


@pytest.fixture
def mock_3d_model_revision_thumbnail_response(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> HTTPXMock:
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/thumbnail"),
        status_code=200,
        json={},
    )
    return httpx_mock


@pytest.fixture
def expected_items3() -> list[dict[str, Any]]:
    return [
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


@pytest.fixture
def mock_3d_model_revision_node_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, expected_items3: list[dict[str, Any]]
) -> HTTPXMock:
    response_body = {"items": expected_items3}
    httpx_mock.add_response(
        method="GET",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/nodes?sortByNodeId=false&limit=10"),
        status_code=200,
        json=response_body,
        is_optional=True,
    )
    httpx_mock.add_response(
        method="GET",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/nodes?nodeId=&limit=10"),
        status_code=200,
        json=response_body,
        is_optional=True,
    )
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/nodes/list"),
        status_code=200,
        json=response_body,
        is_optional=True,
    )
    httpx_mock.add_response(
        method="GET",
        url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/nodes/ancestors"),
        status_code=200,
        json=response_body,
        is_optional=True,
    )
    return httpx_mock


class Test3DModelRevisions:
    def test_list(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_response: HTTPXMock,
        expected_items2: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.list(model_id=1, published=True, limit=100)
        assert isinstance(res, ThreeDModelRevisionList)
        assert expected_items2 == res.dump(camel_case=True)

    def test_update_with_update_object(
        self, cognite_client: CogniteClient, mock_3d_model_revision_response: HTTPXMock
    ) -> None:
        update = ThreeDModelRevisionUpdate(id=1).published.set(False)
        cognite_client.three_d.revisions.update(1, update)
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.get_requests()[0].content
        )["items"][0]

    def test_update_with_resource_object(
        self, cognite_client: CogniteClient, mock_3d_model_revision_response: HTTPXMock
    ) -> None:
        cognite_client.three_d.revisions.update(
            1,
            ThreeDModelRevision(
                id=1,
                published=False,
                created_time=123,
                file_id=1,
                rotation=None,
                scale=None,
                translation=None,
                camera=None,
                status="bla",
                metadata=None,
                thumbnail_threed_file_id=None,
                thumbnail_url=None,
                asset_mapping_count=1,
            ),
        )
        assert {"id": 1, "update": {"published": {"set": False}}} == jsgz_load(
            mock_3d_model_revision_response.get_requests()[0].content
        )["items"][0]

    @pytest.mark.parametrize("identifier", [1, [1]])
    def test_delete(
        self, cognite_client: CogniteClient, mock_3d_model_revision_response: HTTPXMock, identifier: int | list[int]
    ) -> None:
        res = cognite_client.three_d.revisions.delete(1, id=identifier)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_revision_response.get_requests()[0].content)
        assert res is None

    def test_retrieve(
        self,
        cognite_client: CogniteClient,
        mock_retrieve_3d_model_revision_response: HTTPXMock,
        expected_items2: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.retrieve(model_id=1, id=1)
        assert isinstance(res, ThreeDModelRevision)
        assert expected_items2[0] == res.dump(camel_case=True)

    def test_create(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_response: HTTPXMock,
        expected_items2: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.create(model_id=1, revision=ThreeDModelRevisionWrite(file_id=123))
        assert isinstance(res, ThreeDModelRevision)
        assert {"items": [{"fileId": 123, "published": False}]} == jsgz_load(
            mock_3d_model_revision_response.get_requests()[0].content
        )
        assert expected_items2[0] == res.dump(camel_case=True)

    def test_create_multiple(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_response: HTTPXMock,
        expected_items2: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.create(model_id=1, revision=[ThreeDModelRevisionWrite(file_id=123)])
        assert isinstance(res, ThreeDModelRevisionList)
        assert {"items": [{"fileId": 123, "published": False}]} == jsgz_load(
            mock_3d_model_revision_response.get_requests()[0].content
        )
        assert expected_items2 == res.dump(camel_case=True)

    def test_update_thumbnail(
        self, cognite_client: CogniteClient, mock_3d_model_revision_thumbnail_response: HTTPXMock
    ) -> None:
        res = cognite_client.three_d.revisions.update_thumbnail(model_id=1, revision_id=1, file_id=1)
        assert {"fileId": 1} == jsgz_load(mock_3d_model_revision_thumbnail_response.get_requests()[0].content)
        assert res is None

    def test_list_3d_nodes(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_node_response: HTTPXMock,
        expected_items3: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.list_nodes(model_id=1, revision_id=1, node_id=None, depth=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert expected_items3 == res.dump(camel_case=True)

    def test_filter_3d_nodes(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_node_response: HTTPXMock,
        expected_items3: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.filter_nodes(
            model_id=1, revision_id=1, properties={"Item": {"Type": ["Group"]}}, limit=10
        )
        assert isinstance(res, ThreeDNodeList)
        assert expected_items3 == res.dump(camel_case=True)

    def test_list_3d_ancestor_nodes(
        self,
        cognite_client: CogniteClient,
        mock_3d_model_revision_node_response: HTTPXMock,
        expected_items3: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.revisions.list_ancestor_nodes(model_id=1, revision_id=1, node_id=None, limit=10)
        assert isinstance(res, ThreeDNodeList)
        assert expected_items3 == res.dump(camel_case=True)


class Test3DFiles:
    @pytest.fixture
    def mock_3d_files_response(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(method="GET", url=get_url(cognite_client.three_d, "/3d/files/1"), text="bla")

    def test_retrieve(self, cognite_client: CogniteClient, mock_3d_files_response: HTTPXMock) -> None:
        assert b"bla" == cognite_client.three_d.files.retrieve(1)


class Test3DAssetMappings:
    @pytest.fixture
    def expected_items4(self) -> list[dict[str, Any]]:
        return [{"nodeId": 1003, "assetId": 3001, "treeIndex": 5, "subtreeSize": 7}]

    @pytest.fixture
    def mock_3d_asset_mappings_response(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, expected_items4: list[dict[str, Any]]
    ) -> HTTPXMock:
        response_body = {"items": expected_items4}
        url_pattern = re.compile(re.escape(get_url(cognite_client.three_d)) + "/3d/models/1/revisions/1/mappings.*")

        httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
        return httpx_mock

    def test_list(
        self,
        cognite_client: CogniteClient,
        mock_3d_asset_mappings_response: HTTPXMock,
        expected_items4: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.asset_mappings.list(
            model_id=1, revision_id=1, node_id=None, asset_id=None, intersects_bounding_box=None, limit=None
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert expected_items4 == res.dump(camel_case=True)

        url = str(mock_3d_asset_mappings_response.get_requests()[0].url)
        assert "intersectsBoundingBox" not in unquote_plus(url)

    def test_list__with_intersects_bounding_box(
        self,
        cognite_client: CogniteClient,
        mock_3d_asset_mappings_response: HTTPXMock,
        expected_items4: list[dict[str, Any]],
    ) -> None:
        bbox = BoundingBox3D(min=[0.0, 0.0, 0.0], max=[1.0, 1.0, 1.0])
        res = cognite_client.three_d.asset_mappings.list(
            model_id=1, revision_id=1, node_id=None, asset_id=None, intersects_bounding_box=bbox, limit=None
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert expected_items4 == res.dump(camel_case=True)

        url = str(mock_3d_asset_mappings_response.calls[0].request.url)
        assert 'intersectsBoundingBox={"max":[1.0,1.0,1.0],"min":[0.0,0.0,0.0]}' in unquote_plus(url)

    def test_create(
        self,
        cognite_client: CogniteClient,
        mock_3d_asset_mappings_response: HTTPXMock,
        expected_items4: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=ThreeDAssetMappingWrite(node_id=1, asset_id=1)
        )
        assert isinstance(res, ThreeDAssetMapping)
        assert expected_items4[0] == res.dump(camel_case=True)

    def test_create_multiple(
        self,
        cognite_client: CogniteClient,
        mock_3d_asset_mappings_response: HTTPXMock,
        expected_items4: list[dict[str, Any]],
    ) -> None:
        res = cognite_client.three_d.asset_mappings.create(
            model_id=1, revision_id=1, asset_mapping=[ThreeDAssetMappingWrite(node_id=1, asset_id=1)]
        )
        assert isinstance(res, ThreeDAssetMappingList)
        assert expected_items4 == res.dump(camel_case=True)

    def test_delete(self, cognite_client: CogniteClient, mock_3d_asset_mappings_response: HTTPXMock) -> None:
        res = cognite_client.three_d.asset_mappings.delete(
            model_id=1,
            revision_id=1,
            asset_mapping=ThreeDAssetMapping(1, 1, tree_index=None, subtree_size=None, cognite_client=None),
        )
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.get_requests()[0].content)[
            "items"
        ]

    def test_delete_multiple(self, cognite_client: CogniteClient, mock_3d_asset_mappings_response: HTTPXMock) -> None:
        res = cognite_client.three_d.asset_mappings.delete(
            model_id=1,
            revision_id=1,
            asset_mapping=[ThreeDAssetMapping(1, 1, tree_index=None, subtree_size=None, cognite_client=None)],
        )
        assert res is None
        assert [{"nodeId": 1, "assetId": 1}] == jsgz_load(mock_3d_asset_mappings_response.get_requests()[0].content)[
            "items"
        ]

    def test_delete_fails(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(cognite_client.three_d, "/3d/models/1/revisions/1/mappings/delete"),
            status_code=500,
            json={"error": {"message": "Server Error", "code": 500}},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.three_d.asset_mappings.delete(
                model_id=1,
                revision_id=1,
                asset_mapping=[ThreeDAssetMapping(1, 1, tree_index=None, subtree_size=None, cognite_client=None)],
            )
        assert e.value.unknown == [ThreeDAssetMapping.load({"assetId": 1, "nodeId": 1})]
