import re
from unittest import mock

import pytest

from cognite.client.experimental import CogniteClient
from cognite.client.data_classes import Dataset, DatasetFilter, DatasetList, DatasetUpdate
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient()
DS_API = COGNITE_CLIENT.datasets


@pytest.fixture
def mock_ds_response(rsps):
    response_body = {
        "items": [
            {
                "id": 0,
                "externalId": "string",
                "name": "stringname",
                "metadata": {"metadata-key": "metadata-value"},
                "description": "string",
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(DS_API._get_base_url_with_base_path()) + "/datasets(?:/byids|/create|/update|/delete|/list|/search|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestDataset:
    def test_retrieve_single(self, mock_ds_response):
        res = DS_API.retrieve(id=1)
        assert isinstance(res, Dataset)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, mock_ds_response):
        res = DS_API.retrieve_multiple(ids=[1])
        assert isinstance(res, DatasetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, mock_ds_response):
        res = DS_API.list()
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_filters(self, mock_ds_response):
        res = DS_API.list(metadata={"a": "b"}, last_updated_time={"min": 45}, created_time={"max": 123})
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"metadata": {"a": "b"}, "createdTime": {"max": 123}, "lastUpdatedTime": {"min": 45}} == jsgz_load(
            mock_ds_response.calls[0].request.body
        )["filter"]

    def test_create_single(self, mock_ds_response):
        res = DS_API.create(Dataset(external_id="1", name="blabla"))
        assert isinstance(res, Dataset)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_ds_response):
        res = DS_API.create([Dataset(external_id="1", name="blabla")])
        assert isinstance(res, DatasetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_ds_response):
        for asset in DS_API:
            assert mock_ds_response.calls[0].response.json()["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, mock_ds_response):
        for assets in DS_API(chunk_size=1):
            assert mock_ds_response.calls[0].response.json()["items"] == assets.dump(camel_case=True)

    #    def test_delete_single(self, mock_ds_response):
    #        res = DS_API.delete(id=1)
    #        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.calls[0].request.body)
    #        assert res is None
    #    def test_delete_multiple(self, mock_ds_response):
    #        res = DS_API.delete(id=[1])
    #        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.calls[0].request.body)
    #        assert res is None

    def test_update_with_resource_class(self, mock_ds_response):
        res = DS_API.update(Dataset(id=1))
        assert isinstance(res, Dataset)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, mock_ds_response):
        res = DS_API.update(DatasetUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Dataset)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_ds_response):
        res = DS_API.update([DatasetUpdate(id=1).description.set("blabla")])
        assert isinstance(res, DatasetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_update_multiple_list(self, mock_ds_response):
        tsl = DS_API.retrieve_multiple(ids=[0])
        res = DS_API.update(tsl)
        assert isinstance(res, DatasetList)
        assert 1 == len(res)

    def test_update_object(self):
        assert isinstance(
            DatasetUpdate(1)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .name.set("")
            .name.set(None),
            DatasetUpdate,
        )
