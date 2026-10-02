from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cognite.client.data_classes.ai import (
    ForecastResultList,
    ImputeResultList,
    InputDatapoint,
    InputTimeSeries,
    QuantileDatapoint,
    TimeSeriesForecast,
    TimeSeriesForecastList,
    TimeSeriesImpute,
    TimeSeriesImputeList,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._importing import local_import

FORECAST_RESPONSE = {
    "timeSeries": [
        {
            "label": "23-PT-1101",
            "cohort": "compression-train-a",
            "forecast": [
                {"timestamp": 60_000, "quantiles": {"0.05": 41.0, "0.50": 42.0, "0.95": 43.0}},
                {"timestamp": 120_000, "quantiles": {"0.05": 41.5, "0.50": 42.5, "0.95": 43.5}},
            ],
        },
        {
            "label": "24-TT-3001",
            "forecast": [{"timestamp": 60_000, "quantiles": {"0.05": 30.0, "0.50": 31.0, "0.95": 32.0}}],
        },
    ],
    "metadata": {"quantileLevels": [0.05, 0.5, 0.95], "cohorts": ["compression-train-a"]},
}


class TestInputTimeSeries:
    def test_dump_converts_timestamps_and_omits_unset_fields(self) -> None:
        series = InputTimeSeries(
            "21-PT-1019",
            [
                InputDatapoint(datetime(2026, 10, 1, tzinfo=timezone.utc), 42.1),
                InputDatapoint(1790812860000, None, missing=True),
            ],
        )
        assert series.dump() == {
            "label": "21-PT-1019",
            "datapoints": [
                {"timestamp": 1790812800000, "value": 42.1},
                {"timestamp": 1790812860000, "value": None, "missing": True},
            ],
        }

    def test_tuples_and_dicts_are_accepted_as_datapoints(self) -> None:
        series = InputTimeSeries(
            "23-PT-1101",
            [(0, 1.0), {"timestamp": 60_000, "value": None, "missing": True}],
            cohort="compression-train-a",
        )
        assert series.dump() == {
            "label": "23-PT-1101",
            "cohort": "compression-train-a",
            "datapoints": [{"timestamp": 0, "value": 1.0}, {"timestamp": 60_000, "value": None, "missing": True}],
        }


class TestForecastResultList:
    def test_load_response(self) -> None:
        res = ForecastResultList._load_response(FORECAST_RESPONSE)

        assert res.quantile_levels == [0.05, 0.5, 0.95]
        assert res.cohorts == ["compression-train-a"]
        first = res.get(label="23-PT-1101")
        assert first is not None
        assert first.cohort == "compression-train-a"
        assert first.forecast[0] == QuantileDatapoint(60_000, {0.05: 41.0, 0.5: 42.0, 0.95: 43.0})
        assert res.get(label="unknown") is None

    @pytest.mark.dsl
    def test_to_pandas_has_datetime_index_and_label_quantile_columns(self) -> None:
        pd = local_import("pandas")
        df = ForecastResultList._load_response(FORECAST_RESPONSE).to_pandas()

        assert isinstance(df.index, pd.DatetimeIndex)
        assert list(df.columns) == [(label, q) for label in ["23-PT-1101", "24-TT-3001"] for q in [0.05, 0.5, 0.95]]
        assert df[("23-PT-1101", 0.5)].tolist() == [42.0, 42.5]
        assert df[("24-TT-3001", 0.5)].iloc[0] == 31.0
        assert pd.isna(df[("24-TT-3001", 0.5)].iloc[1])  # no forecast at the second timestamp

    @pytest.mark.dsl
    def test_single_result_to_pandas(self) -> None:
        result = ForecastResultList._load_response(FORECAST_RESPONSE).get(label="23-PT-1101")
        assert result is not None
        df = result.to_pandas()
        assert list(df.columns) == [0.05, 0.5, 0.95]
        assert df.iloc[1][0.5] == 42.5


class TestImputeResultList:
    @pytest.mark.dsl
    def test_series_with_nothing_imputed_keeps_its_columns(self) -> None:
        response = {
            "timeSeries": [
                {"label": "a", "imputed": [{"timestamp": 60_000, "quantiles": {"0.5": 1.0}}]},
                {"label": "b", "imputed": []},
            ],
            "metadata": {"quantileLevels": [0.5], "cohorts": []},
        }
        df = ImputeResultList._load_response(response).to_pandas()
        assert list(df.columns) == [("a", 0.5), ("b", 0.5)]


class TestQuantileDatapoint:
    @pytest.mark.parametrize("key", ["0.5", "0.50", "5e-1"])
    def test_quantile_levels_are_parsed_as_floats(self, key: str) -> None:
        assert QuantileDatapoint._load({"timestamp": 0, "quantiles": {key: 1.0}}).quantiles == {0.5: 1.0}

    def test_non_numeric_keys_are_ignored(self) -> None:
        point = QuantileDatapoint._load({"timestamp": 0, "quantiles": {"0.5": 1.0, "unexpected": 2.0}})
        assert point.quantiles == {0.5: 1.0}


POINTS = [QuantileDatapoint(60_000, {0.05: 1.0, 0.5: 2.0}), QuantileDatapoint(120_000, {0.05: 1.5, 0.5: 2.5})]
NODE = NodeId("north_sea_asset", "21-PT-1039")


class TestTimeSeriesForecastList:
    def test_get_by_any_identifier(self) -> None:
        res = TimeSeriesForecastList(
            [
                TimeSeriesForecast(POINTS, id=123),
                TimeSeriesForecast(POINTS, id=456, external_id="21-PT-1029"),
                TimeSeriesForecast(POINTS, instance_id=NODE),
            ],
            quantile_levels=[0.05, 0.5],
        )
        assert res.get(id=123) is res[0]
        assert res.get(external_id="21-PT-1029") is res[1]
        assert res.get(instance_id=NODE) is res[2]

    def test_dump_uses_camel_case_identifiers(self) -> None:
        dumped = TimeSeriesForecast(POINTS[:1], external_id="21-PT-1019", instance_id=NODE, cohort="train-a").dump()
        assert dumped == {
            "forecast": [{"timestamp": 60_000, "quantiles": {"0.05": 1.0, "0.5": 2.0}}],
            "cohort": "train-a",
            "externalId": "21-PT-1019",
            "instanceId": {"space": "north_sea_asset", "externalId": "21-PT-1039"},
        }

    @pytest.mark.dsl
    def test_to_pandas_names_columns_like_retrieve_dataframe(self) -> None:
        res = TimeSeriesForecastList(
            [
                TimeSeriesForecast(POINTS, id=123),
                TimeSeriesForecast(POINTS, id=456, external_id="21-PT-1029"),
                TimeSeriesForecast(POINTS, external_id="ignored", instance_id=NODE),
            ],
            quantile_levels=[0.05, 0.5],
        )
        assert list(res.to_pandas().columns.get_level_values(0).unique()) == [123, "21-PT-1029", NODE]


class TestTimeSeriesImputeList:
    @pytest.mark.dsl
    def test_series_with_nothing_imputed_keeps_its_columns(self) -> None:
        res = TimeSeriesImputeList(
            [TimeSeriesImpute(POINTS, external_id="a"), TimeSeriesImpute([], external_id="b")], quantile_levels=[0.5]
        )
        assert list(res.to_pandas().columns) == [("a", 0.5), ("b", 0.5)]
