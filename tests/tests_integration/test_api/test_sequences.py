import sys
from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Sequence, SequenceFilter, SequenceUpdate
from cognite.client.exceptions import IncompatibleColumnTypesError
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


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.sequences, "_post", wraps=COGNITE_CLIENT.sequences._post) as _:
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

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.sequences, 10):
            res = COGNITE_CLIENT.sequences.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.sequences._post.call_count

    def test_list_assetid_nothing(self):
        res = COGNITE_CLIENT.sequences.list(asset_ids=[12345678910], limit=20)
        assert 0 == len(res)

    def test_list_assetid(self):
        res = COGNITE_CLIENT.sequences.list(asset_ids=[42], limit=20)
        assert 1 == len(res)

    def test_search(self):
        res = COGNITE_CLIENT.sequences.search(name="42", filter=SequenceFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, new_seq):
        update_seq = SequenceUpdate(new_seq.id).name.set("newname")
        res = COGNITE_CLIENT.sequences.update(update_seq)
        assert "newname" == res.name

    def test_get_new(self, new_seq):
        res = COGNITE_CLIENT.sequences.retrieve(id=new_seq.id)
        # assert ["DOUBLE"] == res.column_value_types # soon to change
        assert ["column0"] == res.column_external_ids


@pytest.mark.dsl
class TestSequencesPandas:
    def test_create_from_dataframe(self):
        import pandas as pd

        extid = "__test_df{}.{}".format(sys.version_info.major, sys.version_info.minor)

        df = pd.DataFrame([[1, 2.3, "a"], [4, 4.1, "b"]])
        df.columns = ["intcol", "floatcol", "strcol"]
        df2 = pd.DataFrame([[1, 2.3], [4, 4.1]])

        seq = COGNITE_CLIENT.sequences.create_from_dataframe(dataframe=df, external_id=extid, force_recreate=True)
        assert 3 == len(seq.columns)
        assert "LONG" == seq.columns[0]["valueType"]
        assert "DOUBLE" == seq.columns[1]["valueType"]
        assert "STRING" == seq.columns[2]["valueType"]

        with pytest.raises(IncompatibleColumnTypesError):
            COGNITE_CLIENT.sequences.create_from_dataframe(dataframe=df2, external_id=extid)

        COGNITE_CLIENT.sequences.create_from_dataframe(dataframe=df2, external_id=extid, force_recreate=True)
        COGNITE_CLIENT.sequences.delete(external_id=extid)
