from __future__ import annotations

import json
import re
from typing import Any

import pytest
from httpx2 import Request, Response
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.config import global_config
from cognite.client.data_classes.ai import ForecastInputDatapoint, ForecastInputTimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental import FeaturePreviewWarning
from tests.utils import get_url, jsgz_load

QUANTILE_LEVELS = ["0.05", "0.5", "0.95"]


def fake_model(request: Request) -> Response:
    """Answers like the API: echoes label and cohort; 3 forecast steps, or one point per `missing` datapoint."""
    body = (
        jsgz_load(request.content) if request.headers.get("content-encoding") == "gzip" else json.loads(request.content)
    )
    is_forecast = request.url.path.endswith("/forecast")
    items = []
    for s in body["items"]:
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
    cohorts = list(dict.fromkeys(s["cohort"] for s in body["items"] if "cohort" in s))
    metadata = {"quantileLevels": [float(q) for q in QUANTILE_LEVELS], "cohorts": cohorts}
    return Response(200, json={"items": items, "metadata": metadata})


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


def series(label: str, cohort: str | None = None) -> ForecastInputTimeSeries:
    return ForecastInputTimeSeries(
        label, [ForecastInputDatapoint(0, 1.0), ForecastInputDatapoint(60_000, 2.0)], cohort=cohort
    )


class TestForecast:
    def test_sends_the_api_request_with_beta_header(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        cognite_client.ai.time_series.forecast(
            [
                ForecastInputTimeSeries(
                    "23-PT-1101",
                    [ForecastInputDatapoint(0, 42.1), ForecastInputDatapoint(60_000, None, missing=True)],
                    cohort="compression-train-a",
                )
            ]
        )

        [request] = mock_forecast.get_requests()
        assert request.headers["cdf-version"] == "20230101-beta"
        assert jsgz_load(request.content) == {
            "items": [
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
        inputs = [series(f"s{i}") for i in range(100)]

        res = cognite_client.ai.time_series.forecast(inputs)

        assert [len(jsgz_load(r.content)["items"]) for r in mock_forecast.get_requests()] == [32, 32, 32, 4]
        assert [r.label for r in res] == [s.label for s in inputs]

    def test_results_keep_input_order_with_cohorts_spread_through_the_input(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        inputs = [series(f"s{i}", "train-a" if i % 3 == 0 else None) for i in range(60)]

        res = cognite_client.ai.time_series.forecast(inputs)

        sent = [[s["label"] for s in jsgz_load(r.content)["items"]] for r in mock_forecast.get_requests()]
        assert len(sent) == 2
        assert all(labels == sorted(labels, key=lambda label: int(label[1:])) for labels in sent)
        assert [r.label for r in res] == [s.label for s in inputs]

    def test_cohorts_are_in_input_order_across_requests(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        inputs = [series(f"b{i}", "train-b") for i in range(20)] + [series(f"a{i}", "train-a") for i in range(20)]

        res = cognite_client.ai.time_series.forecast(inputs)

        assert len(mock_forecast.get_requests()) == 2
        assert res.cohorts == ["train-b", "train-a"]

    def test_failed_request_raises_with_failed_labels(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.ai.time_series, "/ai/timeseries/forecast"),
            status_code=400,
            json={"detail": "items[0].datapoints[1].timestamp: must be 60000 ms after datapoints[0].timestamp"},
        )

        with pytest.raises(CogniteAPIError, match=r"items\[0\]\.datapoints\[1\]\.timestamp: must be 60000 ms") as err:
            cognite_client.ai.time_series.forecast([series("21-PT-1019")])
        assert err.value.failed == ["21-PT-1019"]

    def test_duplicate_labels_are_rejected(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match=re.escape("Duplicated labels: {'21-PT-1019'}")):
            cognite_client.ai.time_series.forecast([series("21-PT-1019"), series("21-PT-1019")])

    def test_oversized_cohort_is_rejected(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="Cohort 'compression-train-a' has 33 series"):
            cognite_client.ai.time_series.forecast([series(f"s{i}", "compression-train-a") for i in range(33)])

    @pytest.mark.parametrize("use_gzip", [True, False])
    def test_size_used_for_splitting_is_the_size_sent(
        self,
        use_gzip: bool,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_forecast: HTTPXMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(global_config, "disable_gzip", not use_gzip)
        inputs = [series("21-PT-1019", "compression-train-a"), series("24-TT-3001")]

        cognite_client.ai.time_series.forecast(inputs)

        [request] = mock_forecast.get_requests()
        assert async_client.ai.time_series._size_as_sent(inputs) == len(request.content)

    async def test_async_client(self, async_client: AsyncCogniteClient, mock_forecast: HTTPXMock) -> None:
        res = await async_client.ai.time_series.forecast([series("21-PT-1019")])
        assert res.get(label="21-PT-1019") is not None


class TestImpute:
    def test_returns_only_points_marked_missing(self, cognite_client: CogniteClient, mock_impute: HTTPXMock) -> None:
        res = cognite_client.ai.time_series.impute(
            [
                ForecastInputTimeSeries(
                    "21-PT-1019",
                    [
                        ForecastInputDatapoint(0, 42.1),
                        ForecastInputDatapoint(60_000, None, missing=True),
                        ForecastInputDatapoint(120_000, None),
                        ForecastInputDatapoint(180_000, 42.9),
                    ],
                )
            ]
        )

        result = res.get(label="21-PT-1019")
        assert result is not None
        assert [p.timestamp for p in result.imputed] == [60_000]


class TestBetaApi:
    @pytest.mark.parametrize(
        "api_subversion, expected_header",
        [("20230101", "20230101-beta"), ("alpha", "beta"), ("20230101-beta", "20230101-beta"), ("beta", "beta")],
    )
    async def test_every_call_sends_the_beta_header_and_warns(
        self,
        api_subversion: str,
        expected_header: str,
        async_client: AsyncCogniteClient,
        mock_forecast: HTTPXMock,
        mock_impute: HTTPXMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = async_client.ai.time_series
        monkeypatch.setattr(api, "_api_subversion", api_subversion)
        monkeypatch.setattr(global_config, "silence_feature_preview_warnings", False)

        with pytest.warns(FeaturePreviewWarning, match="Time series forecasting and imputation") as warnings:
            await api.forecast([series("a")])
            await api.impute([series("a")])

        assert len([w for w in warnings if issubclass(w.category, FeaturePreviewWarning)]) == 2
        assert {r.headers["cdf-version"] for r in mock_forecast.get_requests()} == {expected_header}
