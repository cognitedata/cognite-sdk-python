import re
from random import random
from typing import Any

import pytest
from httpx import Request, Response
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoints
from tests.utils import get_url, jsgz_load


def generate_datapoints(start: int, end: int, granularity: int = 1) -> list[dict[str, Any]]:
    return [{"value": random(), "timestamp": i} for i in range(start, end, granularity)]


@pytest.fixture
def mock_get_datapoints(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> HTTPXMock:
    def request_callback(request: Request) -> Response:
        payload = jsgz_load(request.content)

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
        return Response(200, headers={}, json={"items": items})

    httpx_mock.add_callback(
        request_callback,
        method="POST",
        url=get_url(cognite_client.time_series.data.synthetic) + "/timeseries/synthetic/query",
        match_headers={"content-type": "application/json"},
        is_reusable=True,
    )
    return httpx_mock


@pytest.fixture
def mock_get_datapoints_empty(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> HTTPXMock:
    httpx_mock.add_response(
        method="POST",
        url=re.compile(re.escape(get_url(cognite_client.time_series.data.synthetic)) + "/timeseries/synthetic/.*"),
        status_code=200,
        json={"items": [{"isString": False, "datapoints": []}]},
    )
    return httpx_mock


class TestSyntheticQuery:
    def test_query(self, cognite_client: CogniteClient, mock_get_datapoints: HTTPXMock) -> None:
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions='TS{externalID:"abc"} + TS{id:1}', start=1000000, end=1100001
        )
        assert isinstance(dps_res, Datapoints)
        assert 100001 == len(dps_res)
        assert 11 == len(mock_get_datapoints.get_requests())

    def test_query_limit(self, cognite_client: CogniteClient, mock_get_datapoints: HTTPXMock) -> None:
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions=['TS{externalID:"abc"}', "TS{id:1}"], start=1000000, end=1100001, limit=20000
        )
        assert 20000 == len(dps_res[0])
        assert 20000 == len(dps_res[1])
        assert 4 == len(mock_get_datapoints.get_requests())

    def test_query_empty(self, cognite_client: CogniteClient, mock_get_datapoints_empty: HTTPXMock) -> None:
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions=['TS{externalID:"abc"} + TS{id:1}'], start=1000000, end=1100001
        )
        assert isinstance(dps_res[0], Datapoints)
        assert 0 == len(dps_res[0])
        assert 1 == len(mock_get_datapoints_empty.get_requests())

    @pytest.mark.dsl
    def test_expression_builder(self, cognite_client: CogniteClient) -> None:
        from sympy import symbols

        build_fn = cognite_client.time_series.data.synthetic._build_expression
        assert ("ts{externalId:'x'}", "a") == build_fn(symbols("a"), {"a": "x"})
        assert (
            "ts{externalId:'x',aggregate:'average',granularity:'1m'}",
            "a",
        ) == build_fn(symbols("a"), {"a": "x"}, aggregate="average", granularity="1m")
        assert (
            "(ts{externalId:'x'}+ts{externalId:'y'}+ts{externalId:'z'})",
            "(a+b+c)",
        ) == build_fn(symbols("a") + symbols("b") + symbols("c"), {"a": "x", "b": "y", "c": "z"})
        assert ("(1/ts{externalId:'a'})", "(1/a)") == build_fn(1 / symbols("a"), {"a": "a"})
        assert (
            "ts{externalId:'x',targetUnit:'temperature:deg_c'}",
            "a",
        ) == build_fn(symbols("a"), {"a": "x"}, target_unit="temperature:deg_c")
        assert (
            "ts{externalId:'x',targetUnitSystem:'Imperial'}",
            "a",
        ) == build_fn(symbols("a"), {"a": "x"}, target_unit_system="Imperial")

    @pytest.mark.dsl
    def test_expression_builder__overlapping(self, cognite_client: CogniteClient) -> None:
        # Before SDK version 7.30.1, variable replacements were done one-by-one, which could mean
        # that a later replacement would affect an earlier one.
        from sympy import symbols

        build_fn = cognite_client.time_series.data.synthetic._build_expression
        x, y = symbols("x y")
        long_expr, short_expr = build_fn(x + y, {x: "test-x-y-z", y: "foo"})
        assert short_expr == "(x+y)"
        assert long_expr == "(ts{externalId:'test-x-y-z'}+ts{externalId:'foo'})", "(x+y)"

    @pytest.mark.dsl
    def test_expression_builder_variables_missing(self, cognite_client: CogniteClient) -> None:
        from sympy import symbols

        with pytest.raises(
            ValueError, match="sympy expressions are only supported in combination with the `variables` parameter"
        ):
            cognite_client.time_series.data.synthetic.query([symbols("a")], start=0, end="now")

    @pytest.mark.dsl
    def test_expression_builder_unsupported_missing(self, cognite_client: CogniteClient) -> None:
        from sympy import cot, symbols

        with pytest.raises(TypeError, match="^Unsupported sympy class cot"):
            cognite_client.time_series.data.synthetic.query(
                [symbols("a") + cot(symbols("a"))], start=0, end="now", variables={"a": "a"}
            )
