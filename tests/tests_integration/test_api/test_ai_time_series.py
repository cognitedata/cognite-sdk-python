from __future__ import annotations

import math
from collections.abc import Collection

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.ai import ForecastInputDatapoint, ForecastInputTimeSeries

START_MS = 1_790_812_800_000  # 2026-10-01T00:00:00Z
STEP_MS = 60_000
FORECAST_LENGTH = 512


def signal(
    label: str, cohort: str | None = None, length: int = 200, phase: float = 0.0, missing: Collection[int] = ()
) -> ForecastInputTimeSeries:
    """An evenly spaced, deterministic wave with some ripple. The points at the indices in `missing` are marked missing."""
    datapoints = []
    for i in range(length):
        timestamp = START_MS + i * STEP_MS
        if i in missing:
            datapoints.append(ForecastInputDatapoint(timestamp, None, missing=True))
        else:
            value = 50 + 10 * math.sin(2 * math.pi * i / 60 + phase) + 0.1 * ((i * 7919) % 13)
            datapoints.append(ForecastInputDatapoint(timestamp, value))
    return ForecastInputTimeSeries(label, datapoints, cohort)


class TestForecast:
    def test_forecast_a_single_time_series(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.ai.time_series.forecast([signal("a")])

        assert [r.label for r in res] == ["a"]
        assert 0.5 in res.metadata.quantile_levels
        forecast = res[0].forecast
        assert len(forecast) == FORECAST_LENGTH
        # The forecast continues at the interval of the history, one step after its last point:
        assert [p.timestamp for p in forecast[:2]] == [START_MS + 200 * STEP_MS, START_MS + 201 * STEP_MS]
        assert all(set(p.quantiles) == set(res.metadata.quantile_levels) for p in forecast)

    @pytest.mark.dsl
    def test_forecast_to_pandas_has_the_median_by_default(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.ai.time_series.forecast([signal("a"), signal("b", phase=1.0)])

        assert list(res.to_pandas().columns) == ["a", "b"]
        assert len(res.to_pandas()) == FORECAST_LENGTH
        assert list(res.to_pandas(include_all_quantiles=True).columns) == [
            (label, q) for label in ["a", "b"] for q in res.metadata.quantile_levels
        ]

    def test_forecast_time_series_in_a_cohort(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.ai.time_series.forecast(
            [signal("a", cohort="train"), signal("b", cohort="train", phase=1.0), signal("c", phase=2.0)]
        )

        assert res.metadata.cohorts == ["train"]
        assert [(r.label, r.cohort) for r in res] == [("a", "train"), ("b", "train"), ("c", None)]

    def test_a_call_over_the_series_limit_is_split_and_keeps_the_input_order(
        self, cognite_client: CogniteClient
    ) -> None:
        labels = [f"series-{i}" for i in range(40)]

        res = cognite_client.ai.time_series.forecast([signal(label, phase=i) for i, label in enumerate(labels)])

        assert [r.label for r in res] == labels
        assert all(len(r.forecast) == FORECAST_LENGTH for r in res)


class TestImpute:
    def test_impute_returns_a_point_for_each_datapoint_marked_missing(self, cognite_client: CogniteClient) -> None:
        missing = [100, 101, 150]

        res = cognite_client.ai.time_series.impute([signal("a", missing=missing), signal("b", phase=1.0)])

        assert [r.label for r in res] == ["a", "b"]
        assert [p.timestamp for p in res[0].imputed] == [START_MS + i * STEP_MS for i in missing]
        assert all(0.5 in p.quantiles for p in res[0].imputed)
        assert res[1].imputed == []  # nothing marked missing, nothing returned
        assert 0.5 in res.metadata.quantile_levels
