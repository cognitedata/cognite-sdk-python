import re
from datetime import datetime, timezone
from unittest import mock

import pytest

from cognite.client.data_classes import Datapoints, DatapointsList


@pytest.fixture(scope="session")
def test_time_series(cognite_client):
    time_series_names = [f"test__constant_{i}_with_noise" for i in range(10)]
    time_series = {}
    for ts in cognite_client.time_series.retrieve_multiple(external_ids=time_series_names):
        value = int(re.match(r"test__constant_(\d+)_with_noise", ts.name).group(1))
        time_series[value] = ts
    yield time_series


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(
        cognite_client.time_series.data.synthetic, "_post", wraps=cognite_client.time_series.data.synthetic._post
    ) as _:
        yield


class TestSyntheticDatapointsAPI:
    def test_query(self, cognite_client, test_time_series, post_spy):
        query = f"ts{{id:{test_time_series[0].id}}} + ts{{id:{test_time_series[1].id}}}"
        dps = cognite_client.time_series.data.synthetic.query(
            expressions=query, start=datetime(2017, 1, 1), end="now", limit=23456
        )
        assert 23456 == len(dps)
        assert 3 == cognite_client.time_series.data.synthetic._post.call_count

    def test_query_with_start_before_epoch(self, cognite_client, test_time_series, post_spy):
        query = f"ts{{id:{test_time_series[0].id}}} + ts{{id:{test_time_series[1].id}}}"
        dps = cognite_client.time_series.data.synthetic.query(
            expressions=query, start=datetime(1920, 1, 1, tzinfo=timezone.utc), end="now", limit=23456
        )
        assert 23456 == len(dps)
        assert 3 == cognite_client.time_series.data.synthetic._post.call_count

    def test_query_with_multiple_expressions(self, cognite_client, test_time_series, post_spy):
        expressions = [f"ts{{id:{test_time_series[0].id}}}", f"ts{{id:{test_time_series[1].id}}}"]
        dps = cognite_client.time_series.data.synthetic.query(
            expressions=expressions, start=datetime(2017, 1, 1), end="now", limit=23456
        )
        assert 23456 == len(dps[0])
        assert 23456 == len(dps[1])
        assert 6 == cognite_client.time_series.data.synthetic._post.call_count

    def test_query_with_errors(self, cognite_client, test_time_series, post_spy):
        dps = cognite_client.time_series.data.synthetic.query(
            expressions=["A / (B - B)"],
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"A": test_time_series[0], "B": test_time_series[1]},
        )[0]
        assert 100 == len(dps)
        assert 100 == len(dps.error)
        assert all(x is not None for x in dps.error)
        assert all(x is None for x in dps.value)
        assert (100, 1) == dps.to_pandas().shape
        assert (100, 2) == dps.to_pandas(include_errors=True).shape

    @pytest.mark.dsl
    def test_expression_builder_time_series_vs_string(self, cognite_client, test_time_series):
        from sympy import symbols

        dps1 = cognite_client.time_series.data.synthetic.query(
            expressions=symbols("a"),
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"a": test_time_series[0].external_id},
        )
        dps2 = cognite_client.time_series.data.synthetic.query(
            expressions=[symbols("a"), symbols("b")],
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"a": test_time_series[0], "b": test_time_series[0].external_id},
        )
        assert 100 == len(dps1)
        assert 100 == len(dps2[0])
        assert dps1 == dps2.get(external_id="a")
        assert isinstance(dps1, Datapoints)
        assert isinstance(dps2, DatapointsList)

    @pytest.mark.skip("flaky")
    @pytest.mark.dsl
    def test_expression_builder_complex(self, cognite_client, test_time_series):
        from sympy import cos, log, pi, sin, sqrt, symbols

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
        dps1 = cognite_client.time_series.data.synthetic.query(
            expressions=[expression],
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={v: test_time_series[tsi] for v, tsi in zip(abc, range(10))},
        )[0]
        assert 100 == len(dps1)
