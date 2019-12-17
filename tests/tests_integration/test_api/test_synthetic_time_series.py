import re
from datetime import datetime
from unittest import mock

import pytest

from cognite.client.experimental import CogniteClient

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def test_time_series():
    time_series = {}
    for ts in COGNITE_CLIENT.time_series.list(limit=1000):
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

    @pytest.mark.dsl
    def test_expression_builder_time_series_vs_string(self, test_time_series):
        from sympy import symbols

        dps1 = COGNITE_CLIENT.datapoints.synthetic.retrieve(
            expression=symbols("a"),
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"a": test_time_series[0].external_id},
        )
        dps2 = COGNITE_CLIENT.datapoints.synthetic.retrieve(
            expression=symbols("a"),
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"a": test_time_series[0]},
        )
        assert 100 == len(dps1)
        assert 100 == len(dps2)
        assert dps1 == dps2

    @pytest.mark.dsl
    def test_expression_builder_complex(self, test_time_series):
        from sympy import symbols, cos, sin, pi, log, sqrt

        abc = list("abcdefghij")
        syms = symbols(abc)
        expression = syms[0]
        for s in syms:
            expression = expression + s
        expression = (
            (expression * expression)
            + sqrt(sin(pi * 0.1 ** syms[1]))
            + log(23 + syms[5] ** 1.234)
            + cos(syms[3] ** (1 + 0.1 ** syms[4]))
            + sqrt(log(abs(syms[8]) + 1))
        )
        dps1 = COGNITE_CLIENT.datapoints.synthetic.retrieve(
            expression=expression,
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={v: test_time_series[tsi] for v, tsi in zip(abc, range(10))},
        )
        assert 100 == len(dps1)
