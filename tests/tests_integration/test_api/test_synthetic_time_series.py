import math
import re
from datetime import datetime, timedelta
from unittest import mock

import numpy
import pandas
import pytest

from cognite.client.data_classes import DatapointsQuery, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.experimental import CogniteClient
from cognite.client.utils._time import timestamp_to_ms
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def test_time_series():
    time_series = {}
    for ts in COGNITE_CLIENT.time_series.list(limit=150):
        if ts.name in ["test__constant_{}_with_noise".format(i) for i in range(0, 10)]:
            value = int(re.match(r"test__constant_(\d+)_with_noise", ts.name).group(1))
            time_series[value] = ts
    yield time_series


@pytest.fixture
def post_spy():
    with mock.patch.object(
        COGNITE_CLIENT.datapoints.synthetic, "_post", wraps=COGNITE_CLIENT.datapoints.synthetic._post
    ) as _:
        yield


class TestSyntheticDatapointsAPI:
    def test_retrieve(self, test_time_series, post_spy):
        query = "ts{id:%d} + ts{id:%d}" % (test_time_series[0].id, test_time_series[1].id)
        dps = COGNITE_CLIENT.datapoints.synthetic.retrieve(
            expression=query, start=datetime(2017, 1, 1), end="now", limit=23456
        )
        assert 23456 == len(dps)
        assert 3 == COGNITE_CLIENT.datapoints.synthetic._post.call_count
