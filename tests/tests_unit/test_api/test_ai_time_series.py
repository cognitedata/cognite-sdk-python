from __future__ import annotations

import re
from typing import Any

import pytest
from httpx2 import Request, Response
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.ai import InputDatapoint, InputTimeSeries
from cognite.client.exceptions import CogniteAPIError
from tests.utils import get_url, jsgz_load

QUANTILE_LEVELS = ["0.05", "0.5", "0.95"]


def fake_model(request: Request) -> Response:
    """Answers like the API: echoes label and cohort; 3 forecast steps, or one point per `missing` datapoint."""
    body = jsgz_load(request.content)
    is_forecast = request.url.path.endswith("/forecast")
    items = []
    for s in body["timeSeries"]:
        dps = s["datapoints"]
        if is_forecast:
            step = dps[1]["timestamp"] - dps[0]["timestamp"]
            timestamps = [dps[-1]["timestamp"] + step * (i + 1) for i in range(3)]
        else:
            timestamps = [dp["timestamp"] for dp in dps if dp.get("missing")]
        points = [{"timestamp": t, "quantiles": dict.fromkeys(QUANTILE_LEVELS, 1.0)} for t in timestamps]
        item: dict[str, Any] = {"label": s["label"], ("forecast" if is_forecast else "imputed"): points}
        if "cohort" in s:
            item["cohort"] = s["cohort"]
        items.append(item)
    cohorts = sorted({s["cohort"] for s in body["timeSeries"] if "cohort" in s})
    metadata = {"quantileLevels": [float(q) for q in QUANTILE_LEVELS], "cohorts": cohorts}
    return Response(200, json={"timeSeries": items, "metadata": metadata})


@pytest.fixture
def mock_forecast(httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient) -> HTTPXMock:
    url = get_url(async_client.ai.time_series, "/ai/timeseries/forecast")
    httpx2_mock.add_callback(fake_model, method="POST", url=url, is_reusable=True)
    return httpx2_mock


@pytest.fixture
def mock_impute(httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient) -> HTTPXMock:
    url = get_url(async_client.ai.time_series, "/ai/timeseries/impute")
    httpx2_mock.add_callback(fake_model, method="POST", url=url, is_reusable=True)
    return httpx2_mock


def series(label: str, cohort: str | None = None) -> InputTimeSeries:
    return InputTimeSeries(label, [(0, 1.0), (60_000, 2.0)], cohort=cohort)


class TestForecast:
    def test_sends_the_api_request_with_beta_header(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        cognite_client.ai.time_series.forecast(
            InputTimeSeries(
                "23-PT-1101",
                [InputDatapoint(0, 42.1), InputDatapoint(60_000, None, missing=True)],
                cohort="compression-train-a",
            )
        )

        [request] = mock_forecast.get_requests()
        assert request.headers["cdf-version"] == "beta"
        assert jsgz_load(request.content) == {
            "timeSeries": [
                {
                    "label": "23-PT-1101",
                    "cohort": "compression-train-a",
                    "datapoints": [
                        {"timestamp": 0, "value": 42.1},
                        {"timestamp": 60_000, "value": None, "missing": True},
                    ],
                }
            ]
        }

    def test_returns_results_with_metadata(self, cognite_client: CogniteClient, mock_forecast: HTTPXMock) -> None:
        res = cognite_client.ai.time_series.forecast(
            [series("23-PT-1101", "compression-train-a"), series("24-TT-3001")]
        )

        assert res.quantile_levels == [0.05, 0.5, 0.95]
        assert res.cohorts == ["compression-train-a"]
        result = res.get(label="24-TT-3001")
        assert result is not None
        assert [p.timestamp for p in result.forecast] == [120_000, 180_000, 240_000]

    def test_large_request_is_split_and_results_keep_input_order(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        inputs = [series(f"s{i}") for i in range(300)]

        res = cognite_client.ai.time_series.forecast(inputs)

        assert [len(jsgz_load(r.content)["timeSeries"]) for r in mock_forecast.get_requests()] == [256, 44]
        assert [r.label for r in res] == [s.label for s in inputs]

    def test_failed_request_raises_with_failed_labels(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.ai.time_series, "/ai/timeseries/forecast"),
            status_code=400,
            json={"error": {"code": 400, "message": "Datapoints must be evenly spaced"}},
        )

        with pytest.raises(CogniteAPIError, match="evenly spaced") as err:
            cognite_client.ai.time_series.forecast(series("21-PT-1019"))
        assert err.value.failed == ["21-PT-1019"]

    def test_duplicate_labels_are_rejected(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match=re.escape("Duplicated labels: {'21-PT-1019'}")):
            cognite_client.ai.time_series.forecast([series("21-PT-1019"), series("21-PT-1019")])

    def test_oversized_cohort_is_rejected(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="Cohort 'compression-train-a' has 257 series"):
            cognite_client.ai.time_series.forecast([series(f"s{i}", "compression-train-a") for i in range(257)])

    async def test_async_client(self, async_client: AsyncCogniteClient, mock_forecast: HTTPXMock) -> None:
        res = await async_client.ai.time_series.forecast(series("21-PT-1019"))
        assert res.get(label="21-PT-1019") is not None


class TestImpute:
    def test_returns_only_points_marked_missing(self, cognite_client: CogniteClient, mock_impute: HTTPXMock) -> None:
        res = cognite_client.ai.time_series.impute(
            InputTimeSeries(
                "21-PT-1019",
                [
                    InputDatapoint(0, 42.1),
                    InputDatapoint(60_000, None, missing=True),
                    InputDatapoint(120_000, None),
                    InputDatapoint(180_000, 42.9),
                ],
            )
        )

        result = res.get(label="21-PT-1019")
        assert result is not None
        assert [p.timestamp for p in result.imputed] == [60_000]
