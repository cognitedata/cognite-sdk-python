from unittest import mock

import pytest

from cognite.client.data_classes import DataSet, DataSetFilter, DataSetUpdate
from cognite.client.exceptions import CogniteNotFoundError
from tests.utils import set_request_limit


@pytest.fixture(scope="class")
def new_dataset(cognite_client):
    dataset = cognite_client.data_sets.create(DataSet())
    yield dataset
    # todo: uncomment when delete is implemented
    # cognite_client.data_sets.delete(id=dataset.id)
    # assert cognite_client.data_sets.retrieve(dataset.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.data_sets, "_post", wraps=cognite_client.data_sets._post) as _:
        yield


class TestDataSetsAPI:
    def test_retrieve(self, cognite_client, new_dataset):
        assert new_dataset.id == cognite_client.data_sets.retrieve(new_dataset.id).id

    def test_retrieve_multiple(self, cognite_client):
        res_listed_ids = [e.id for e in cognite_client.data_sets.list(limit=2)]
        res_lookup_ids = [e.id for e in cognite_client.data_sets.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_retrieve_unknown(self, cognite_client, new_dataset):
        invalid_external_id = "this does not exist"
        with pytest.raises(CogniteNotFoundError) as error:
            cognite_client.data_sets.retrieve_multiple(ids=[new_dataset.id], external_ids=[invalid_external_id])
        assert error.value.not_found[0]["externalId"] == invalid_external_id

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.data_sets, 1):
            res = cognite_client.data_sets.list(limit=2)

        assert 2 == len(res)
        assert 2 == cognite_client.data_sets._post.call_count

    def test_aggregate(self, cognite_client):
        res = cognite_client.data_sets.aggregate(filter=DataSetFilter(metadata={"1": "1"}))
        assert res[0].count > 0

    def test_update(self, cognite_client, new_dataset):
        update_asset = DataSetUpdate(new_dataset.id).metadata.set({"1": "1"}).name.set("newname")
        res = cognite_client.data_sets.update(update_asset)
        assert {"1": "1"} == res.metadata
        assert "newname" == res.name
