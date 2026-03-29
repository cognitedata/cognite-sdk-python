from __future__ import annotations

import re
from datetime import timedelta, timezone
from random import random
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest
from httpx import Request, Response
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.datapoints import SyntheticDatapoints, SyntheticDatapointsList
from tests.utils import get_url, jsgz_load

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


def generate_datapoints(start: int, end: int, granularity: int = 1) -> list[dict[str, Any]]:
    return [{"value": random(), "timestamp": i} for i in range(start, end, granularity)]


@pytest.fixture
def mock_get_datapoints(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
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
            items.append({"isString": False, "type": "numeric", "datapoints": dps})
        return Response(200, headers={}, json={"items": items})

    httpx_mock.add_callback(
        request_callback,
        method="POST",
        url=get_url(async_client.time_series.data.synthetic) + "/timeseries/synthetic/query",
        match_headers={"content-type": "application/json"},
        is_reusable=True,
    )
    return httpx_mock


@pytest.fixture
def mock_get_datapoints_empty(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    httpx_mock.add_response(
        method="POST",
        url=re.compile(re.escape(get_url(async_client.time_series.data.synthetic)) + "/timeseries/synthetic/.*"),
        status_code=200,
        json={"items": [{"isString": False, "type": "numeric", "datapoints": []}]},
    )
    return httpx_mock


class TestSyntheticQuery:
    def test_query(self, cognite_client: CogniteClient, mock_get_datapoints: HTTPXMock) -> None:
        dps_res = cognite_client.time_series.data.synthetic.query(
            expressions='TS{externalID:"abc"} + TS{id:1}', start=1000000, end=1100001
        )
        assert isinstance(dps_res, SyntheticDatapoints)
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
        assert isinstance(dps_res[0], SyntheticDatapoints)
        assert 0 == len(dps_res[0])
        assert 1 == len(mock_get_datapoints_empty.get_requests())

    @pytest.mark.dsl
    def test_expression_builder(self, async_client: AsyncCogniteClient) -> None:
        from sympy import symbols

        build_fn = async_client.time_series.data.synthetic._build_expression
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
    def test_expression_builder__overlapping(self, async_client: AsyncCogniteClient) -> None:
        # Before SDK version 7.30.1, variable replacements were done one-by-one, which could mean
        # that a later replacement would affect an earlier one.
        from sympy import symbols

        build_fn = async_client.time_series.data.synthetic._build_expression
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

        with pytest.raises(TypeError, match=r"^Unsupported sympy class cot"):
            cognite_client.time_series.data.synthetic.query(
                [symbols("a") + cot(symbols("a"))], start=0, end="now", variables={"a": "a"}
            )


@pytest.mark.dsl
class TestSyntheticDatapointsToPandas:
    @pytest.mark.parametrize(
        "epoch_ms, offset_hours, zone, expected",
        (
            (1716589737000, 2, "Europe/Oslo", "2024-05-25 00:28:57.000+02:00"),
            (1616589737000, 1, "Europe/Oslo", "2021-03-24 13:42:17.000+01:00"),
        ),
    )
    def test_to_pandas_with_timezone_and_zoneinfo(
        self, epoch_ms: int, offset_hours: int, zone: str, expected: str
    ) -> None:
        """Test that SyntheticDatapoints.to_pandas() respects timezone parameter."""
        import pandas as pd

        expression = "TS{id:123}"
        sdp_zoneinfo = SyntheticDatapoints(
            expression=expression,
            timestamp=[epoch_ms, epoch_ms + 1000],
            value=[1.0, 2.0],
            error=[None, None],
            is_string=False,
            timezone=ZoneInfo(zone),
        )
        sdp_timedelta = SyntheticDatapoints(
            expression=expression,
            timestamp=[epoch_ms, epoch_ms + 1000],
            value=[1.0, 2.0],
            error=[None, None],
            is_string=False,
            timezone=timezone(timedelta(hours=offset_hours)),
        )
        df_zoneinfo = sdp_zoneinfo.to_pandas()
        df_timedelta = sdp_timedelta.to_pandas()

        assert len(df_zoneinfo) == 2
        assert len(df_timedelta) == 2
        assert expression in df_zoneinfo.columns
        assert expression in df_timedelta.columns
        assert pd.Timestamp(expected) == df_zoneinfo.index[0] == df_timedelta.index[0]

    def test_to_pandas_with_errors_and_no_timezone(self) -> None:
        """Test that SyntheticDatapoints.to_pandas() includes errors when present."""

        expression = "TS{id:123}"
        sdp = SyntheticDatapoints(
            expression=expression,
            timestamp=[1000, 2000, 3000],
            value=[1.0, None, 3.0],
            error=[None, "Division by zero", None],
            is_string=False,
            timezone=None,
        )
        df = sdp.to_pandas(include_errors=True)
        assert "error" in df.columns
        assert df["error"].iloc[1] == "Division by zero"

        df_no_errors = sdp.to_pandas(include_errors=False)
        assert "error" not in df_no_errors.columns

        # We used timezone=None, so the index should be UTC and timezone-aware:
        assert df.index.tz is timezone.utc

    @pytest.mark.parametrize(
        "epoch_ms, offset_hours, zone, expected",
        (
            (1716589737000, 2, "Europe/Oslo", "2024-05-25 00:28:57.000+02:00"),
            (1616589737000, 1, "Europe/Oslo", "2021-03-24 13:42:17.000+01:00"),
        ),
    )
    def test_list_to_pandas_with_timezone(self, epoch_ms: int, offset_hours: int, zone: str, expected: str) -> None:
        """Test that SyntheticDatapointsList.to_pandas() respects timezone parameter."""
        import pandas as pd

        sdp1 = SyntheticDatapoints(
            expression="TS{id:123}",
            timestamp=[epoch_ms, epoch_ms + 1000],
            value=[1.0, 2.0],
            error=[None, None],
            is_string=False,
            timezone=ZoneInfo(zone),
        )
        sdp2 = SyntheticDatapoints(
            expression="TS{id:456}",
            timestamp=[epoch_ms, epoch_ms + 1000],
            value=[3.0, 4.0],
            error=[None, None],
            is_string=False,
            timezone=ZoneInfo(zone),
        )
        sdp_list = SyntheticDatapointsList([sdp1, sdp2])
        df = sdp_list.to_pandas()

        assert len(df) == 2
        assert pd.Timestamp(expected) == df.index[0]
        assert "TS{id:123}" in df.columns
        assert "TS{id:456}" in df.columns
        assert df["TS{id:123}"].iloc[0] == 1.0
        assert df["TS{id:456}"].iloc[0] == 3.0

    def test_list_to_pandas_empty(self) -> None:
        """Test that empty SyntheticDatapointsList returns empty DataFrame."""
        import pandas as pd

        sdp_list = SyntheticDatapointsList([])
        df = sdp_list.to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
