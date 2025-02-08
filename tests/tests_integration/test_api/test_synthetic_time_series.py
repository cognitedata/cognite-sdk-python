import random
from datetime import datetime, timezone
from unittest import mock

import numpy as np
import pytest

from cognite.client.data_classes import Datapoints, DatapointsList, TimeSeries, TimeSeriesWriteList
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.utils._time import datetime_to_ms


@pytest.fixture(scope="session")
def test_time_series(cognite_client) -> dict[int, TimeSeries]:
    time_series_names = [f"test__constant_{i}_with_noise" for i in range(10)]
    retrieved = cognite_client.time_series.retrieve_multiple(external_ids=time_series_names, ignore_unknown_ids=True)
    if missing := set(time_series_names) - set(retrieved.as_external_ids()):
        to_create = TimeSeriesWriteList([])
        datapoints: list[dict] = []
        size = 100_000
        start = datetime_to_ms(datetime(2025, 1, 1, tzinfo=timezone.utc))
        step = 100  # every 100 ms
        stop = start + size * step
        timestamps = np.arange(start, stop, step).tolist()
        for name in missing:
            number = int(name.removeprefix("test__constant_").removesuffix("_with_noise"))
            to_create.append(
                TimeSeries(
                    name=name,
                    external_id=name,
                    description=f"Constant {number} with ±0.1 uniform noise",
                    is_step=False,
                    is_string=False,
                )
            )
            datapoints.append(
                {
                    "external_id": name,
                    "datapoints": Datapoints(
                        external_id=name,
                        timestamp=timestamps,
                        value=np.random.uniform(number - 0.1, number + 0.1, size).tolist(),
                    ),
                }
            )

        created = cognite_client.time_series.create(to_create)
        retrieved.extend(created)
        cognite_client.time_series.data.insert_multiple(datapoints)

    return {int(ts.name.removeprefix("test__constant_").removesuffix("_with_noise")): ts for ts in retrieved}


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

    def test_query_using_time_series_objs__with_errors(self, cognite_client, test_time_series, post_spy):
        dps = cognite_client.time_series.data.synthetic.query(
            expressions=["A / (B - B)"],
            start=datetime(2017, 1, 1),
            end="now",
            limit=100,
            variables={"A": test_time_series[0], "B": test_time_series[1]},
        )[0]
        assert 100 == len(dps)
        assert 100 == len(dps.error)
        assert 100 == len(dps.value)
        assert all(x is not None for x in dps.error)
        assert all(x is None for x in dps.value)
        assert (100, 1) == dps.to_pandas().shape
        assert (100, 2) == dps.to_pandas(include_errors=True).shape

    def test_query_using_time_series_objs__missing_external_id(self, cognite_client, test_time_series):
        (whoopsie_ts := test_time_series[1].as_write()).external_id = None
        # Before SDK version 7.32.8, when a passed TimeSeries missing external_id was passed, None
        # was just cast to string and passed to the API, most likely leading to a "not found" error
        with pytest.raises(
            ValueError, match="^TimeSeries passed in 'variables' is missing required field 'external_id'"
        ):
            cognite_client.time_series.data.synthetic.query(
                expressions="A / B",
                start=datetime(2017, 1, 1),
                end="now",
                limit=100,
                variables={"A": test_time_series[0], "B": whoopsie_ts},
            )

    def test_query_using_instance_ids(self, cognite_client):
        node_id = NodeId("PySDK-DMS-time-series-integration-test", "PYSDK integration test 126: clone of 114")
        ext_id = "PYSDK integration test 114: 1mill dps, random distribution, 1950-2020, numeric"
        ts_with_ext_id, ts_with_instance_id = cognite_client.time_series.retrieve_multiple(
            external_ids=ext_id, instance_ids=node_id
        )
        n_dps = random.choice(range(100, 1000))
        res = cognite_client.time_series.data.synthetic.query(
            expressions="(A / B) * (C / D) - 1",  # should yield zeros only
            variables={
                "A": node_id,  # NodeId
                "B": ts_with_ext_id,  # TimeSeries using external_id
                "C": ts_with_instance_id,  # TimeSeries using instance_id
                "D": ext_id,  # str (external ID)
            },
            start=random.choice(range(1483228800000)),  # start between 1970 and 2017
            end="now",
            limit=n_dps,
        )
        assert len(res) == n_dps
        assert all(err is None for err in res.error)
        assert all(x == 0.0 for x in res.value)  # float, plz

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

    @pytest.mark.dsl
    def test_expression_builder_complex(self, cognite_client, test_time_series):
        from sympy import Abs, cos, log, pi, sin, sqrt, symbols

        string_symbols = list("abcdefghij")
        syms = symbols(string_symbols)
        expression = (
            sum(syms) ** 2
            + sqrt(sin(pi * 0.1 ** syms[1]))
            + log(23 + syms[5] ** 1.234)
            + cos(syms[3] ** (1 + 0.1 ** syms[4]))
            + sqrt(log(Abs(syms[8]) + 1))
        )
        symbolic_vars = {sym: ts for sym, ts in zip(syms, test_time_series.values())}
        string_variables = {ss: ts for ss, ts in zip(string_symbols, test_time_series.values())}

        for variables in symbolic_vars, string_variables:
            dps1 = cognite_client.time_series.data.synthetic.query(
                expressions=expression,
                start=datetime(2017, 1, 1, tzinfo=timezone.utc),
                end="now",
                limit=100,
                variables=variables,
                aggregate="average",
                granularity="3s",
            )
            assert 100 == len(dps1)
