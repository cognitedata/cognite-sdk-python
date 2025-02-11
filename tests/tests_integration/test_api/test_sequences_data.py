import random
import string
from unittest import mock

import numpy as np
import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Sequence,
    SequenceColumnWrite,
    SequenceRow,
    SequenceRows,
    SequenceRowsList,
    SequenceWrite,
)
from tests.utils import rng_context


@pytest.fixture(scope="session")
def named_long_str(cognite_client) -> Sequence:
    seq = cognite_client.sequences.retrieve(external_id="named_long_str")
    if seq is None:
        seq = cognite_client.sequences.create(
            SequenceWrite(
                external_id="named_long_str",
                columns=[
                    SequenceColumnWrite(external_id="longcol", value_type="Long"),
                    SequenceColumnWrite(external_id="strcol", value_type="String"),
                ],
            )
        )
    if not cognite_client.sequences.data.retrieve(external_id=seq.external_id):
        cognite_client.sequences.data.insert(
            SequenceRows(
                [SequenceRow(i, [i, f"str{i}"]) for i in range(1000)],
                columns=seq.columns,
                external_id=seq.external_id,
            ),
            external_id=seq.external_id,
        )
    return seq


@pytest.fixture(scope="session")
@rng_context(seed=42)
def string200(cognite_client) -> Sequence:
    seq = cognite_client.sequences.retrieve(external_id="string200")
    if seq is None:
        seq = cognite_client.sequences.create(
            SequenceWrite(
                external_id="string200",
                columns=[SequenceColumnWrite(external_id=f"col{i}", value_type="String") for i in range(200)],
            )
        )

    if not cognite_client.sequences.data.retrieve(external_id=seq.external_id):
        cognite_client.sequences.data.insert(
            SequenceRows(
                [
                    SequenceRow(i, ["".join(random.choices(string.ascii_letters, k=100)) for _ in range(200)])
                    for i in range(1000)
                ],
                columns=seq.columns,
                external_id=seq.external_id,
            ),
            external_id=seq.external_id,
        )
    return seq


@pytest.fixture(scope="session")
def small_sequence(cognite_client: CogniteClient) -> Sequence:
    seq = cognite_client.sequences.retrieve(external_id="small")
    if seq is None:
        seq = cognite_client.sequences.create(
            SequenceWrite(
                external_id="small",
                columns=[
                    SequenceColumnWrite(external_id="col0", value_type="String"),
                    SequenceColumnWrite(external_id="col1", value_type="Long"),
                ],
            )
        )
    if not cognite_client.sequences.data.retrieve(external_id=seq.external_id):
        cognite_client.sequences.data.insert(
            SequenceRows(
                [
                    SequenceRow(0, ["str1", 678]),
                    SequenceRow(1, ["str2", 679]),
                    SequenceRow(2, ["str3", 680]),
                    SequenceRow(3, ["str4", 681]),
                ],
                columns=seq.columns,
                external_id=seq.external_id,
            ),
            external_id=seq.external_id,
        )
    return seq


@pytest.fixture(scope="session")
def pretend_timeseries(cognite_client: CogniteClient) -> Sequence:
    seq = cognite_client.sequences.retrieve(external_id="pretend_timeseries")
    if seq is None:
        seq = cognite_client.sequences.create(
            SequenceWrite(
                external_id="pretend_timeseries",
                columns=[
                    SequenceColumnWrite(external_id="value", value_type="Double"),
                ],
            )
        )
    if not cognite_client.sequences.data.retrieve(external_id=seq.external_id):
        cognite_client.sequences.data.insert(
            SequenceRows(
                [SequenceRow(row_no, [value]) for row_no, value in enumerate(np.random.rand(54321).tolist())],
                columns=seq.columns,
                external_id=seq.external_id,
            ),
            external_id=seq.external_id,
        )
    return seq


@pytest.fixture(scope="session")
def new_seq(cognite_client: CogniteClient) -> Sequence:
    created = cognite_client.sequences.create(SequenceWrite(columns=[SequenceColumnWrite("col0", value_type="String")]))
    yield created
    cognite_client.sequences.delete(id=created.id)
    assert cognite_client.sequences.retrieve(id=created.id) is None


@pytest.fixture(scope="session")
def new_small_seq(cognite_client: CogniteClient, small_sequence: Sequence) -> Sequence:
    seq = cognite_client.sequences.create(SequenceWrite(columns=small_sequence.columns.as_write()))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(id=seq.id) is None


@pytest.fixture(scope="session")
def new_seq_long(cognite_client):
    seq = cognite_client.sequences.create(Sequence(columns=[{"valueType": "LONG", "externalId": "a"}]))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(id=seq.id) is None


@pytest.fixture(scope="session")
def new_seq_mixed(cognite_client):
    seq = cognite_client.sequences.create(
        SequenceWrite(
            columns=[
                SequenceColumnWrite(external_id="column0", value_type="Long"),
                SequenceColumnWrite(external_id="column1", value_type="String"),
            ],
        )
    )
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(id=seq.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences.data, "_post", wraps=cognite_client.sequences.data._post) as _:
        yield


class TestSequencesDataAPI:
    def test_retrieve(self, cognite_client, small_sequence: Sequence) -> None:
        dps = cognite_client.sequences.data.retrieve(id=small_sequence.id)

        assert isinstance(dps, SequenceRows)
        assert len(dps) > 0

    def test_retrieve_latest(self, cognite_client, small_sequence):
        dps = cognite_client.sequences.data.retrieve_last_row(id=small_sequence.id)
        assert len(dps) == 1

    def test_retrieve_multi(self, cognite_client, small_sequence, pretend_timeseries):
        dps = cognite_client.sequences.rows.retrieve(
            external_id=pretend_timeseries.external_id, id=small_sequence.id, start=0, end=None
        )
        assert isinstance(dps, SequenceRowsList)
        assert len(dps[0]) > 0
        assert len(dps[1]) > 0
        assert small_sequence.id == dps[0].id
        assert pretend_timeseries.external_id == dps[1].external_id

    def test_retrieve_multi_dataframe(
        self, cognite_client: CogniteClient, small_sequence: Sequence, pretend_timeseries: Sequence
    ) -> None:
        df = cognite_client.sequences.data.retrieve(
            id=[small_sequence.id, pretend_timeseries.id], start=0, end=None
        ).to_pandas(column_names="id", concat=True)
        assert df.shape[0] > 0
        assert 3 == df.shape[1]
        assert sorted([str(small_sequence.id), str(small_sequence.id), str(pretend_timeseries.id)]) == sorted(
            df.columns
        )

    def test_retrieve_dataframe(self, cognite_client, small_sequence):
        df = cognite_client.sequences.data.retrieve(id=small_sequence.id, start=0, end=5).to_pandas()
        assert df.shape[0] == 4
        assert df.shape[1] == 2
        assert np.diff(df.index).all()

    def test_insert_dataframe(self, cognite_client, small_sequence, new_small_seq):
        df = cognite_client.sequences.data.retrieve(id=small_sequence.id, start=0, end=5).to_pandas()
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

    def test_retrieve_paginate(self, cognite_client: CogniteClient, string200: Sequence, post_spy):
        data = cognite_client.sequences.data.retrieve(id=string200.id, start=1, end=996)
        assert 200 == len(data.values[0])
        assert 995 == len(data)
        assert 4 == cognite_client.sequences.data._post.call_count  # around 300 rows per request for this case

    def test_retrieve_paginate_max(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve(id=pretend_timeseries.id, start=0, end=None)
        assert 1 == len(data.values[0])
        assert 54321 == len(data)
        assert 6 == cognite_client.sequences.data._post.call_count  # 10k rows each of 54321 rows

    def test_retrieve_paginate_limit_small(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve(id=pretend_timeseries.id, start=0, end=None, limit=23)
        assert 1 == len(data.values[0])
        assert 23 == len(data)
        assert 1 == cognite_client.sequences.data._post.call_count  # 10k rows each of 54321 rows

    def test_retrieve_paginate_limit_paged(self, cognite_client, pretend_timeseries, post_spy):
        data = cognite_client.sequences.data.retrieve(
            id=pretend_timeseries.id, start=0, end=None, limit=40023
        ).to_pandas()
        assert 1 == data.shape[1]
        assert 40023 == data.shape[0]
        assert 5 == cognite_client.sequences.data._post.call_count

    def test_retrieve_one_column(self, cognite_client, named_long_str: Sequence) -> None:
        dps = cognite_client.sequences.data.retrieve(id=named_long_str.id, start=42, end=43, columns=["strcol"])
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
        cognite_client.sequences.data.retrieve(id=string200.id, start=1, end=118)
        assert 1 == cognite_client.sequences.data._post.call_count

    def test_delete_range(self, cognite_client, new_seq_long):
        data = [(i, [10 * i]) for i in [1, 2, 3, 5, 8, 13, 21, 34]]
        cognite_client.sequences.data.insert(
            column_external_ids=new_seq_long.column_external_ids, rows=data, id=new_seq_long.id
        )
        cognite_client.sequences.data.delete_range(start=4, end=15, id=new_seq_long.id)

        # potential delay, so can't assert, but tested in notebook
        # dps = cognite_client.sequences.data.retrieve(start=0, end=None, id=new_seq_long.id)
        # assert [10, 20, 30, 210, 340] == [d[0] for d in dps.values]
        # assert [1, 2, 3, 21, 34] == dps.row_numbers
