import re
from datetime import datetime, timedelta
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Sequence
from cognite.client.experimental import CogniteClient
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def named_long_str():
    seq = COGNITE_CLIENT.sequences.retrieve(external_id="named_long_str")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def string200():
    seq = COGNITE_CLIENT.sequences.retrieve(external_id="string200")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def small_sequence():
    seq = COGNITE_CLIENT.sequences.retrieve(external_id="small")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def pretend_timeseries():
    seq = COGNITE_CLIENT.sequences.retrieve(external_id="pretend_timeseries")
    assert isinstance(seq, Sequence)
    yield seq


@pytest.fixture(scope="session")
def new_seq():
    seq = COGNITE_CLIENT.sequences.create(Sequence(columns=[{}]))
    yield seq
    COGNITE_CLIENT.sequences.delete(id=seq.id)
    assert COGNITE_CLIENT.sequences.retrieve(seq.id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.sequences.data, "_post", wraps=COGNITE_CLIENT.sequences.data._post) as _:
        yield


class TestSequencesDataAPI:
    def test_retrieve(self, small_sequence):
        dps = COGNITE_CLIENT.sequences.data.retrieve(id=small_sequence.id, start=0, end=None)
        assert len(dps) > 0

    def test_retrieve_dataframe(self, small_sequence):
        df = COGNITE_CLIENT.sequences.data.retrieve_dataframe(id=small_sequence.id, start=0, end=5)
        assert df.shape[0] == 4
        assert df.shape[1] == 2
        assert np.diff(df.index).all()

    def test_insert(self, new_seq):
        data = {i: ["str"] for i in range(1, 61)}
        COGNITE_CLIENT.sequences.data.insert(rows=data, columns=[new_seq.columns[0]["id"]], id=new_seq.id)

    def test_delete_multiple(self, new_seq):
        COGNITE_CLIENT.sequences.data.delete(rows=[1, 2, 42, 3524], id=new_seq.id)

    def test_retrieve_paginate(self, string200, post_spy):
        data = COGNITE_CLIENT.sequences.data.retrieve(start=0, end=1000, id=string200.id)
        assert 200 == len(data[0]["values"])
        assert 999 == len(data)
        assert 9 == COGNITE_CLIENT.sequences.data._post.call_count

    def test_retrieve_mixed(self, named_long_str):
        dps = COGNITE_CLIENT.sequences.data.retrieve(id=named_long_str.id, start=42, end=43)
        assert 1 == len(dps)
        assert isinstance(dps[0]["values"][0], int)
        assert isinstance(dps[0]["values"][1], str)
