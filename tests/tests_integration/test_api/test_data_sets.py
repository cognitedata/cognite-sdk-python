from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client.data_classes import DataSet, DataSetFilter, DataSetUpdate
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.experimental import CogniteClient
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()
DS_API = COGNITE_CLIENT.data_sets


@pytest.fixture(scope="class")
def new_dataset():
    dataset = DS_API.create(DataSet())
    yield dataset
    # todo: uncomment when delete is implemented
    # DS_API.delete(id=dataset.id)
    # assert DS_API.retrieve(dataset.id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(DS_API, "_post", wraps=DS_API._post) as _:
        yield


class TestDataSetsAPI:
    def test_retrieve(self, new_dataset):
        assert new_dataset.id == DS_API.retrieve(new_dataset.id).id

    def test_retrieve_multiple(self):
        res_listed_ids = [e.id for e in DS_API.list(limit=2)]
        res_lookup_ids = [e.id for e in DS_API.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_retrieve_unknown(self, new_dataset):
        invalid_external_id = "this does not exist"
        with pytest.raises(CogniteNotFoundError) as error:
            DS_API.retrieve_multiple(ids=[new_dataset.id], external_ids=[invalid_external_id])
        assert error.value.not_found[0]["externalId"] == invalid_external_id

    def test_list(self, post_spy):
        with set_request_limit(DS_API, 1):
            res = DS_API.list(limit=2)

        assert 2 == len(res)
        assert 2 == COGNITE_CLIENT.data_sets._post.call_count

    def test_aggregate(self):
        res = COGNITE_CLIENT.data_sets.aggregate(filter=DataSetFilter(metadata={"1": "1"}))
        assert res[0].count > 0

    def test_update(self, new_dataset):
        update_asset = DataSetUpdate(new_dataset.id).metadata.set({"1": "1"}).name.set("newname")
        res = DS_API.update(update_asset)
        assert {"1": "1"} == res.metadata
        assert "newname" == res.name
