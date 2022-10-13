import json
import re
from random import random

import pytest

from cognite.client.data_classes import Datapoints
from tests.utils import jsgz_load


def generate_datapoints(start: int, end: int, granularity=1):
    dps = []
    for i in range(start, end, granularity):
        dp = {}
        dp["value"] = random()
        dp["timestamp"] = i
        dps.append(dp)
    return dps


@pytest.fixture
def mock_get_datapoints(rsps, cognite_client):
    def request_callback(request):
        payload = jsgz_load(request.body)

        items = []
        for dps_query in payload["items"]:

            if "start" in dps_query and "end" in dps_query:
                start, end = dps_query["start"], dps_query["end"]
            else:
                start, end = payload["start"], payload["end"]

            limit = 10000
            if "limit" in dps_query:
                limit = dps_query["limit"]
            elif "limit" in payload:
                limit = payload["limit"]

            dps = generate_datapoints(start, end)
            dps = dps[:limit]
            items.append({"isString": False, "datapoints": dps})
        response = {"items": items}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        cognite_client.time_series.data.synthetic._get_base_url_with_base_path() + "/timeseries/synthetic/query",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        re.compile(
            re.escape(cognite_client.time_series.data.synthetic._get_base_url_with_base_path())
            + "/timeseries/synthetic/.*"
        ),
        status=200,
        json={"items": [{"isString": False, "datapoints": []}]},
    )
    yield rsps


class TestSyntheticQuery:
    def test_query(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions='TS{externalID:"abc"} + TS{id:1}', start=1000000, end=1100001
        )
        assert isinstance(dps_res, Datapoints)
        assert 100001 == len(dps_res)
        assert 11 == len(mock_get_datapoints.calls)

    def test_query_limit(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions=['TS{externalID:"abc"}', "TS{id:1}"], start=1000000, end=1100001, limit=20000
        )
        assert 20000 == len(dps_res[0])
        assert 20000 == len(dps_res[1])
        assert 4 == len(mock_get_datapoints.calls)

    def test_query_empty(self, cognite_client, mock_get_datapoints_empty):
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions=['TS{externalID:"abc"} + TS{id:1}'], start=1000000, end=1100001
        )
        assert isinstance(dps_res[0], Datapoints)
        assert 0 == len(dps_res[0])
        assert 1 == len(mock_get_datapoints_empty.calls)

    @pytest.mark.dsl
    def test_expression_builder(self, cognite_client):
        from sympy import symbols

        assert ("ts{externalId:'x'}", "a") == cognite_client.time_series.data.synthetic._build_expression(
            symbols("a"), {"a": "x"}
        )
        assert (
            "ts{externalId:'x',aggregate:'average',granularity:'1m'}",
            "a",
        ) == cognite_client.time_series.data.synthetic._build_expression(
            symbols("a"), {"a": "x"}, aggregate="average", granularity="1m"
        )
        assert (
            "(ts{externalId:'x'}+ts{externalId:'y'}+ts{externalId:'z'})",
            "(a+b+c)",
        ) == cognite_client.time_series.data.synthetic._build_expression(
            symbols("a") + symbols("b") + symbols("c"), {"a": "x", "b": "y", "c": "z"}
        )
        assert ("(1/ts{externalId:'a'})", "(1/a)") == cognite_client.time_series.data.synthetic._build_expression(
            1 / symbols("a"), {"a": "a"}
        )

    @pytest.mark.dsl
    def test_expression_builder_variables_missing(self, cognite_client):
        from sympy import symbols

        with pytest.raises(
            ValueError, match="sympy expressions are only supported in combination with the `variables` parameter"
        ):
            cognite_client.time_series.data.synthetic.query([symbols("a")], start=0, end="now")

    @pytest.mark.dsl
    def test_expression_builder_unsupported_missing(self, cognite_client):
        from sympy import cot, symbols

        with pytest.raises(ValueError, match="Unsupported sympy class cot"):
            cognite_client.time_series.data.synthetic.query(
                [symbols("a") + cot(symbols("a"))], start=0, end="now", variables={"a": "a"}
            )
