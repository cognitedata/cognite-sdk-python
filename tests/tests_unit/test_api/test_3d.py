import re

import pytest

from cognite.client import CogniteClient
from cognite.client._api.three_d import ThreeDModel, ThreeDModelList, ThreeDModelUpdate
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


class Test3DModels:
    def test_list(self, mock_3d_model_response):
        res = THREE_D_API.models.list(published=True, limit=100)
        assert isinstance(res, ThreeDModelList)
        assert mock_3d_model_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_update_with_update_object(self, mock_3d_model_response):
        update = ThreeDModelUpdate(id=1).name.set("bla")
        THREE_D_API.models.update(update)
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]

    def test_update_with_resource_object(self, mock_3d_model_response):
        THREE_D_API.models.update(ThreeDModel(id=1, name="bla", created_time=123))
        assert {"id": 1, "update": {"name": {"set": "bla"}}} == jsgz_load(mock_3d_model_response.calls[0].request.body)[
            "items"
        ][0]

    def test_delete(self, mock_3d_model_response):
        res = THREE_D_API.models.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[0].request.body)
        assert res is None
        res = THREE_D_API.models.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_3d_model_response.calls[1].request.body)
        assert res is None

    def test_get(self, mock_3d_model_response):
        res = THREE_D_API.models.get(id=1)
        assert mock_3d_model_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
