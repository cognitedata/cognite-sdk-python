from __future__ import annotations

import json
import math
import re
from typing import Any

import pytest
from httpx2 import Request, Response
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client._api.ai.utils import size_as_sent
from cognite.client.config import global_config
from cognite.client.data_classes.ai import (
    ForecastInputDatapoint,
    ForecastInputTimeSeries,
    ForecastResult,
    ForecastResultList,
    ImputeResult,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._importing import local_import
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

        assert res.metadata.quantile_levels == [0.05, 0.5, 0.95]
        assert res.metadata.cohorts == ["compression-train-a"]
        result = res.get(label="24-TT-3001")
        assert result is not None
        assert [p.timestamp for p in result.forecast] == [120_000, 180_000, 240_000]

    def test_single_time_series_returns_a_single_result(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        res = cognite_client.ai.time_series.forecast(series("24-TT-3001"))

        assert isinstance(res, ForecastResult)
        assert res.label == "24-TT-3001"

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
        assert res.metadata.cohorts == ["train-b", "train-a"]

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

    def test_failed_request_stops_the_others_and_reports_every_label(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        def fail_the_first_request(request: Request) -> Response:
            if jsgz_load(request.content)["items"][0]["label"] == "s0":
                return Response(400, json={"detail": "items[3].datapoints[1].timestamp: must be 60000 ms after ..."})
            return fake_model(request)

        url = get_url(async_client.ai.time_series, "/ai/timeseries/forecast")
        httpx2_mock.add_callback(fail_the_first_request, method="POST", url=url, is_reusable=True)
        inputs = [series(f"s{i}") for i in range(40)]

        with pytest.raises(CogniteAPIError) as err:
            cognite_client.ai.time_series.forecast(inputs)

        assert err.value.failed == [f"s{i}" for i in range(32)]
        assert sorted(err.value.successful + err.value.skipped) == sorted(f"s{i}" for i in range(32, 40))

    def test_empty_input_returns_no_results_without_a_request(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        res = cognite_client.ai.time_series.forecast([])

        assert isinstance(res, ForecastResultList)
        assert len(res) == 0
        assert httpx2_mock.get_requests() == []

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
        mock_forecast: HTTPXMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(global_config, "disable_gzip", not use_gzip)
        inputs = [series("21-PT-1019", "compression-train-a"), series("24-TT-3001")]

        cognite_client.ai.time_series.forecast(inputs)

        [request] = mock_forecast.get_requests()
        assert size_as_sent(inputs) == len(request.content)

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

    def test_single_time_series_returns_a_single_result(
        self, cognite_client: CogniteClient, mock_impute: HTTPXMock
    ) -> None:
        res = cognite_client.ai.time_series.impute(series("21-PT-1019"))

        assert isinstance(res, ImputeResult)
        assert res.label == "21-PT-1019"


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


@pytest.mark.dsl
class TestDataFrames:
    @staticmethod
    def frame(columns: dict[Any, list[float]], tz: str | None = None) -> Any:
        pd = local_import("pandas")
        return pd.DataFrame(columns, index=pd.date_range("2026-10-01", periods=3, freq="1min", tz=tz))

    def test_forecast_dataframe_sends_one_series_per_column(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        df = self.frame(
            {"23-PT-1101": [42.1, math.nan, 42.9], "23-PT-1201": [39.8, 40.1, 40.0], "24-TT-3001": [31.0] * 3}
        )

        out = cognite_client.ai.time_series.forecast_dataframe(
            df, cohort={"23-PT-1101": "compression-train-a", "23-PT-1201": "compression-train-a"}
        )

        sent = jsgz_load(mock_forecast.get_requests()[0].content)["items"]
        assert [(s["label"], s.get("cohort")) for s in sent] == [
            ("23-PT-1101", "compression-train-a"),
            ("23-PT-1201", "compression-train-a"),
            ("24-TT-3001", None),
        ]
        assert sent[0]["datapoints"][:2] == [
            {"timestamp": 1790812800000, "value": 42.1},
            {"timestamp": 1790812860000, "value": None},
        ]
        assert list(out.columns) == ["23-PT-1101", "23-PT-1201", "24-TT-3001"]

    def test_forecast_dataframe_sends_marked_values_as_missing(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        df = self.frame({"a": [1.0, 2.0, 3.0]})
        missing = self.frame({"a": [False, True, False]})  # exclude an observed value from the history

        cognite_client.ai.time_series.forecast_dataframe(df, missing=missing)

        sent = jsgz_load(mock_forecast.get_requests()[0].content)["items"][0]["datapoints"]
        assert sent == [
            {"timestamp": 1790812800000, "value": 1.0},
            {"timestamp": 1790812860000, "value": 2.0, "missing": True},
            {"timestamp": 1790812920000, "value": 3.0},
        ]

    def test_impute_dataframe_predicts_nothing_without_missing(
        self, cognite_client: CogniteClient, mock_impute: HTTPXMock
    ) -> None:

        imputed = cognite_client.ai.time_series.impute_dataframe(self.frame({"a": [1.0, math.nan, 3.0]}))

        sent = jsgz_load(mock_impute.get_requests()[0].content)["items"][0]["datapoints"]
        assert all("missing" not in point for point in sent)
        assert imputed.empty

    def test_a_single_cohort_applies_to_every_column(
        self, cognite_client: CogniteClient, mock_forecast: HTTPXMock
    ) -> None:
        cognite_client.ai.time_series.forecast_dataframe(self.frame({"a": [1.0] * 3, "b": [2.0] * 3}), cohort="train")

        sent = jsgz_load(mock_forecast.get_requests()[0].content)["items"]
        assert [s["cohort"] for s in sent] == ["train", "train"]

    def test_impute_dataframe_predicts_the_nans_and_fills_them(
        self, cognite_client: CogniteClient, mock_impute: HTTPXMock
    ) -> None:
        pd = local_import("pandas")
        df = self.frame({"21-PT-1019": [42.1, math.nan, 42.9]})

        imputed = cognite_client.ai.time_series.impute_dataframe(df, missing=df.isna())

        sent = jsgz_load(mock_impute.get_requests()[0].content)["items"][0]["datapoints"]
        assert sent[1] == {"timestamp": 1790812860000, "value": None, "missing": True}
        assert imputed.index.tolist() == [pd.Timestamp("2026-10-01 00:01:00")]
        assert list(imputed.columns) == ["21-PT-1019"]
        assert df.fillna(imputed)["21-PT-1019"].tolist() == [42.1, 1.0, 42.9]

    def test_impute_dataframe_sends_unmarked_nans_as_gaps_and_marked_values_as_missing(
        self, cognite_client: CogniteClient, mock_impute: HTTPXMock
    ) -> None:
        df = self.frame({"a": [1.0, math.nan, 3.0]})
        missing = self.frame({"a": [False, False, True]})  # an observed value, to compare with its prediction

        cognite_client.ai.time_series.impute_dataframe(df, missing=missing)

        sent = jsgz_load(mock_impute.get_requests()[0].content)["items"][0]["datapoints"]
        assert sent[1:] == [
            {"timestamp": 1790812860000, "value": None},
            {"timestamp": 1790812920000, "value": 3.0, "missing": True},
        ]

    def test_column_names_must_be_strings(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(
            ValueError, match=r"DataFrame column names must be strings, the labels of the time series\. Not: \[1, 2\]"
        ):
            cognite_client.ai.time_series.forecast_dataframe(self.frame({1: [1.0] * 3, 2: [2.0] * 3}))

    def test_timezone_aware_index_is_sent_and_returned_in_utc(
        self, cognite_client: CogniteClient, mock_impute: HTTPXMock
    ) -> None:
        df = self.frame({"a": [1.0, math.nan, 3.0]}, tz="Europe/Oslo")

        imputed = cognite_client.ai.time_series.impute_dataframe(df, missing=df.isna())

        sent = jsgz_load(mock_impute.get_requests()[0].content)["items"][0]["datapoints"]
        assert sent[1]["timestamp"] == int(df.index[1].timestamp() * 1000)
        assert imputed.index.tolist() == [df.index[1].tz_convert("UTC").tz_localize(None)]

    @pytest.mark.parametrize(
        "columns, index, cohort, match",
        [
            (["a", "a"], None, None, r"columns must be unique\. Duplicated cols: \{'a'\}"),
            (["a", "b"], None, {"c": "train"}, r"cohort mapping names columns that aren't in the DataFrame: \['c'\]"),
            (["a", "b"], "range", None, "index must be `pd.DatetimeIndex`, got: <class '.*RangeIndex'>"),
        ],
    )
    def test_invalid_input_is_rejected(
        self, cognite_client: CogniteClient, columns: list[str], index: str | None, cohort: Any, match: str
    ) -> None:
        pd = local_import("pandas")
        df = pd.DataFrame([[1.0, 2.0]] * 3, columns=columns)
        if index is None:
            df.index = pd.date_range("2026-10-01", periods=3, freq="1min")
        with pytest.raises(ValueError, match=match):
            cognite_client.ai.time_series.forecast_dataframe(df, cohort=cohort)
        with pytest.raises(ValueError, match=match):
            cognite_client.ai.time_series.impute_dataframe(df, missing=df.isna(), cohort=cohort)

    @pytest.mark.parametrize("missing", ["other_index", "other_columns", "not_boolean"])
    @pytest.mark.parametrize("method", ["forecast_dataframe", "impute_dataframe"])
    def test_a_missing_that_does_not_match_is_rejected(
        self, cognite_client: CogniteClient, missing: str, method: str
    ) -> None:
        df = self.frame({"a": [1.0] * 3, "b": [2.0] * 3})
        mask = {
            "other_index": df.isna().iloc[:2],
            "other_columns": df.isna()[["a"]],
            "not_boolean": df.isna().astype(int),
        }[missing]
        with pytest.raises(ValueError, match="missing must be booleans with the same index and columns"):
            getattr(cognite_client.ai.time_series, method)(df, missing=mask)

    @pytest.mark.parametrize(
        "method, mock", [("forecast_dataframe", "mock_forecast"), ("impute_dataframe", "mock_impute")]
    )
    async def test_every_call_sends_the_beta_header_and_warns(
        self,
        method: str,
        mock: str,
        async_client: AsyncCogniteClient,
        request: pytest.FixtureRequest,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        httpx_mock: HTTPXMock = request.getfixturevalue(mock)
        api = async_client.ai.time_series
        monkeypatch.setattr(api, "_api_subversion", "20230101")
        monkeypatch.setattr(global_config, "silence_feature_preview_warnings", False)

        with pytest.warns(FeaturePreviewWarning, match="Time series forecasting and imputation"):
            df = self.frame({"a": [1.0] * 3})
            await getattr(api, method)(df, missing=df.isna())

        assert {r.headers["cdf-version"] for r in httpx_mock.get_requests()} == {"20230101-beta"}

    @pytest.mark.parametrize("values", [["x", "y", "z"], [True, False, True]])
    def test_non_numeric_columns_are_rejected(self, cognite_client: CogniteClient, values: list[Any]) -> None:
        with pytest.raises(ValueError, match=r"columns must be numeric\. Non-numeric cols: \['b'\]"):
            cognite_client.ai.time_series.forecast_dataframe(self.frame({"a": [1.0] * 3, "b": values}))

    def test_infinity_is_rejected(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="Infinity"):
            cognite_client.ai.time_series.forecast_dataframe(self.frame({"a": [1.0, math.inf, 3.0]}))
