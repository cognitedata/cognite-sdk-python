from unittest import mock

import pytest

from cognite.client.data_classes import Sequence, SequenceFilter, SequenceUpdate
from tests.utils import set_request_limit


@pytest.fixture(scope="class")
def new_seq(cognite_client):
    seq = cognite_client.sequences.create(Sequence(name="test_temp", columns=[{}], metadata={"a": "b"}))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture
def get_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences, "_get", wraps=cognite_client.sequences._get) as _:
        yield


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences, "_post", wraps=cognite_client.sequences._post) as _:
        yield


class TestSequencesAPI:
    def test_retrieve(self, cognite_client):
        listed_asset = cognite_client.sequences.list(limit=1)[0]
        retrieved_asset = cognite_client.sequences.retrieve(listed_asset.id)
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self, cognite_client):
        res = cognite_client.sequences.list(limit=2)
        retrieved_assets = cognite_client.sequences.retrieve_multiple([s.id for s in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_call(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.sequences, 10):
            res = [s for s in cognite_client.sequences(limit=20)]

        assert 20 == len(res)
        assert 2 == cognite_client.sequences._post.call_count

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.sequences, 10):
            res = cognite_client.sequences.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.sequences._post.call_count

    def test_list_assetid_nothing(self, cognite_client):
        res = cognite_client.sequences.list(asset_ids=[12345678910], limit=20)
        assert 0 == len(res)

    def test_list_assetid(self, cognite_client):
        res = cognite_client.sequences.list(asset_ids=[42], limit=20)
        assert 1 == len(res)

    def test_aggregate(self, cognite_client):
        res = cognite_client.sequences.aggregate(filter=SequenceFilter(name="42"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.sequences.search(name="42", filter=SequenceFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, cognite_client, new_seq):
        assert new_seq.metadata == {"a": "b"}
        update_seq = SequenceUpdate(new_seq.id).name.set("newname").metadata.set(None)
        res = cognite_client.sequences.update(update_seq)
        assert "newname" == res.name
        assert res.metadata == {}

    def test_get_new(self, cognite_client, new_seq):
        res = cognite_client.sequences.retrieve(id=new_seq.id)
        # assert ["DOUBLE"] == res.column_value_types # soon to change
        assert ["column0"] == res.column_external_ids
