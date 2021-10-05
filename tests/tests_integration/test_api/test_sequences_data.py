from unittest import mock

import numpy as np
import pytest

from cognite.client.data_classes import Sequence, SequenceData, SequenceDataList


@pytest.fixture(scope="session")
def named_long_str(cognite_client):
    seq = cognite_client.sequences.retrieve(external_id="named_long_str")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def string200(cognite_client):
    seq = cognite_client.sequences.retrieve(external_id="string200")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def small_sequence(cognite_client):
    seq = cognite_client.sequences.retrieve(external_id="small")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def pretend_timeseries(cognite_client):
    seq = cognite_client.sequences.retrieve(external_id="pretend_timeseries")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def new_seq(cognite_client):
    seq = cognite_client.sequences.create(Sequence(columns=[{"valueType": "STRING"}]))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture(scope="session")
def new_small_seq(cognite_client, small_sequence):
    seq = cognite_client.sequences.create(Sequence(columns=small_sequence.columns))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture(scope="session")
def new_seq_long(cognite_client):
    seq = cognite_client.sequences.create(Sequence(columns=[{"valueType": "LONG", "externalId": "a"}]))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture(scope="session")
def new_seq_mixed(cognite_client):
    seq = cognite_client.sequences.create(Sequence(columns=[{"valueType": "DOUBLE"}, {"valueType": "STRING"}]))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences.data, "_post", wraps=cognite_client.sequences.data._post) as _:
        yield


class TestSequencesDataAPI:
    def test_retrieve(self, cognite_client, small_sequence):
        dps = cognite_client.sequences.data.retrieve(id=small_sequence.id, start=0, end=None)

        assert isinstance(dps, SequenceData)
        assert len(dps) > 0

    def test_retrieve_multi(self, cognite_client, small_sequence, pretend_timeseries):
        dps = cognite_client.sequences.data.retrieve(
            id=[small_sequence.id], external_id=pretend_timeseries.external_id, start=0, end=None
        )
        assert isinstance(dps, SequenceDataList)
        assert len(dps[0]) > 0
        assert len(dps[1]) > 0
        assert small_sequence.id == dps[0].id
        assert pretend_timeseries.external_id == dps[1].external_id

    def test_retrieve_multi_dataframe(self, cognite_client, small_sequence, pretend_timeseries):
        df = cognite_client.sequences.data.retrieve_dataframe(
            id=[small_sequence.id, pretend_timeseries.id], start=0, end=None, column_names="id"
        )
        assert df.shape[0] > 0
        assert 3 == df.shape[1]
        assert all([str(small_sequence.id), str(small_sequence.id), str(pretend_timeseries.id)] == df.columns)

    def test_retrieve_dataframe(self, cognite_client, small_sequence):
        df = cognite_client.sequences.data.retrieve_dataframe(id=small_sequence.id, start=0, end=5)
        assert df.shape[0] == 4
        assert df.shape[1] == 2
        assert np.diff(df.index).all()

    def test_insert_dataframe(self, cognite_client, small_sequence, new_small_seq):
        df = cognite_client.sequences.data.retrieve_dataframe(id=small_sequence.id, start=0, end=5)
        cognite_client.sequences.data.insert_dataframe(df, id=new_small_seq.id)

    def test_insert(self, cognite_client, new_seq):
        data = {i: ["str"] for i in range(1, 61)}
        cognite_client.sequences.data.insert(rows=data, column_external_ids=new_seq.column_external_ids, id=new_seq.id)

    def test_insert_raw(self, cognite_client, new_seq_long):
        data = [{"rowNumber": i, "values": [2 * i]} for i in range(1, 61)]
        cognite_client.sequences.data.insert(
            rows=data, column_external_ids=new_seq_long.column_external_ids, id=new_seq_long.id
        )

    def test_insert_implicit_rows(self, cognite_client, new_seq_mixed):
        data = {i: [i, "str"] for i in range(1, 10)}
        cognite_client.sequences.data.insert(data, id=new_seq_mixed.id, column_external_ids=["column0", "column1"])

    def test_insert_copy(self, cognite_client, small_sequence, new_small_seq):
        data = cognite_client.sequences.data.retrieve(id=small_sequence.id, start=0, end=5)
        cognite_client.sequences.data.insert(rows=data, id=new_small_seq.id, column_external_ids=None)

    def test_delete_multiple(self, cognite_client, new_seq):
        cognite_client.sequences.data.delete(rows=[1, 2, 42, 3524], id=new_seq.id)

    def test_retrieve_paginate(self, cognite_client, string200, post_spy):
        data = cognite_client.sequences.data.retrieve(start=1, end=996, id=string200.id)
        assert 200 == len(data.values[0])
        assert 995 == len(data)
        assert 4 == cognite_client.sequences.data._post.call_count  # around 300 rows per request for this case

    def test_retrieve_paginate_max(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve(start=0, end=None, id=pretend_timeseries.id)
        assert 1 == len(data.values[0])
        assert 54321 == len(data)
        assert 6 == cognite_client.sequences.data._post.call_count  # 10k rows each of 54321 rows

    def test_retrieve_paginate_limit_small(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve(start=0, end=None, id=pretend_timeseries.id, limit=23)
        assert 1 == len(data.values[0])
        assert 23 == len(data)
        assert 1 == cognite_client.sequences.data._post.call_count  # 10k rows each of 54321 rows

    def test_retrieve_paginate_limit_paged(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve_dataframe(
            start=0, end=None, id=pretend_timeseries.id, limit=40023
        )
        assert 1 == data.shape[1]
        assert 40023 == data.shape[0]
        assert 5 == cognite_client.sequences.data._post.call_count

    def test_retrieve_one_column(self, cognite_client, named_long_str):
        dps = cognite_client.sequences.data.retrieve(
            id=named_long_str.id, start=42, end=43, column_external_ids=["strcol"]
        )
        assert 1 == len(dps)
        assert 1 == len(dps.column_external_ids)
        assert isinstance(dps.values[0][0], str)

    def test_retrieve_mixed(self, cognite_client, named_long_str):
        dps = cognite_client.sequences.data.retrieve(id=named_long_str.id, start=42, end=43)
        assert 1 == len(dps)
        assert isinstance(dps.values[0][0], int)
        assert isinstance(dps.values[0][1], str)
        assert 1 == len(dps.get_column("longcol"))
        assert isinstance(dps.get_column("longcol")[0], int)
        assert isinstance(dps.get_column(external_id="strcol")[0], str)
        with pytest.raises(ValueError):
            dps.get_column("missingcol")

    def test_retrieve_paginate_end_coinciding_with_page(self, cognite_client, string200, post_spy):
        cognite_client.sequences.data.retrieve(start=1, end=118, id=string200.id)
        assert 1 == cognite_client.sequences.data._post.call_count

    def test_delete_range(self, cognite_client, new_seq_long):
        data = [(i, [10 * i]) for i in [1, 2, 3, 5, 8, 13, 21, 34]]
        cognite_client.sequences.data.insert(
            column_external_ids=new_seq_long.column_external_ids, rows=data, id=new_seq_long.id
        )
        cognite_client.sequences.data.delete_range(start=4, end=15, id=new_seq_long.id)
        dps = cognite_client.sequences.data.retrieve(start=0, end=None, id=new_seq_long.id)
        # potential delay, so can't assert, but tested in notebook
        # assert [10, 20, 30, 210, 340] == [d[0] for d in dps.values]
        # assert [1, 2, 3, 21, 34] == dps.row_numbers
