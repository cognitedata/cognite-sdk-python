import time
from unittest import mock

import pytest

from cognite.client import utils
from cognite.client.data_classes import Sequence, SequenceFilter, SequenceUpdate
from cognite.client.exceptions import CogniteAPIError
from cognite.client.experimental import CogniteClient
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def new_seq():
    seq = COGNITE_CLIENT.sequences.create(Sequence(name="test_temp", columns=[{}]))
    yield seq
    COGNITE_CLIENT.sequences.delete(id=seq.id)
    assert COGNITE_CLIENT.sequences.retrieve(seq.id) is None


@pytest.fixture
def get_spy():
    with mock.patch.object(COGNITE_CLIENT.sequences, "_get", wraps=COGNITE_CLIENT.sequences._get) as _:
        yield


class TestSequencesAPI:
    def test_retrieve(self):
        listed_asset = COGNITE_CLIENT.sequences.list(limit=1)[0]
        retrieved_asset = COGNITE_CLIENT.sequences.retrieve(listed_asset.id)
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self):
        res = COGNITE_CLIENT.sequences.list(limit=2)
        retrieved_assets = COGNITE_CLIENT.sequences.retrieve_multiple([s.id for s in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_list(self, get_spy):
        with set_request_limit(COGNITE_CLIENT.sequences, 10):
            res = COGNITE_CLIENT.sequences.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.sequences._get.call_count

    def test_search(self):
        res = COGNITE_CLIENT.sequences.search(name="42", filter=SequenceFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, new_seq):
        update_seq = SequenceUpdate(new_seq.id).name.set("newname")
        res = COGNITE_CLIENT.sequences.update(update_seq)
        assert "newname" == res.name
