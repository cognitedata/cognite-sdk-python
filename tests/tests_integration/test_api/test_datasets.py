from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client.data_classes import Dataset, DatasetFilter, DatasetUpdate
from cognite.client.experimental import CogniteClient
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_dataset():
    dataset = COGNITE_CLIENT.datasets.create(Dataset())
    yield dataset
    COGNITE_CLIENT.datasets.delete(id=dataset.id)
    assert COGNITE_CLIENT.datasets.retrieve(dataset.id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.datasets, "_post", wraps=COGNITE_CLIENT.datasets._post) as _:
        yield


@pytest.mark.skip(reason="disabled until delete exists")
class TestDatasetsAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.datasets.list(limit=1)
        assert res[0] == COGNITE_CLIENT.datasets.retrieve(res[0].id)

    def test_retrieve_multiple(self, root_test_asset):
        res_listed_ids = [e.id for e in COGNITE_CLIENT.datasets.list(limit=2)]
        res_lookup_ids = [e.id for e in COGNITE_CLIENT.datasets.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.datasets, 10):
            res = COGNITE_CLIENT.datasets.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.datasets._post.call_count

    def test_update(self, new_dataset):
        update_asset = DatasetUpdate(new_dataset.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.datasets.update(update_asset)
        assert {"bla": "bla"} == res.metadata
