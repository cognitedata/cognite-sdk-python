from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from cognite.client.data_classes.ai import (
    ForecastInputDatapoint,
    ForecastInputTimeSeries,
    ForecastResultList,
    ImputeResultList,
    QuantileDatapoint,
)
from cognite.client.utils._importing import local_import

FORECAST_RESPONSE = {
    "items": [
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


class TestForecastInputTimeSeries:
    def test_dump_converts_timestamps_and_omits_unset_fields(self) -> None:
        series = ForecastInputTimeSeries(
            "21-PT-1019",
            [
                ForecastInputDatapoint(datetime(2026, 10, 1, tzinfo=timezone.utc), 42.1),
                ForecastInputDatapoint(1790812860000, None, missing=True),
            ],
        )
        assert series.dump() == {
            "label": "21-PT-1019",
            "datapoints": [
                {"timestamp": 1790812800000, "value": 42.1},
                {"timestamp": 1790812860000, "value": None, "missing": True},
            ],
        }


class TestForecastInputDatapoint:
    @pytest.mark.parametrize(
        "data, expected",
        [
            ((0, 1.0), ForecastInputDatapoint(0, 1.0)),
            ([0, None, True], ForecastInputDatapoint(0, None, missing=True)),
            ({"timestamp": 0, "value": 1.0}, ForecastInputDatapoint(0, 1.0)),
            ({"timestamp": 0, "missing": True}, ForecastInputDatapoint(0, None, missing=True)),
        ],
    )
    def test_load(self, data: tuple[Any, ...] | list[Any] | dict[str, Any], expected: ForecastInputDatapoint) -> None:
        assert ForecastInputDatapoint.load(data) == expected

    def test_load_returns_a_datapoint_as_is(self) -> None:
        dp = ForecastInputDatapoint(0, 1.0)
        assert ForecastInputDatapoint.load(dp) is dp

    @pytest.mark.parametrize(
        "data, type_name", [("0,1.0", "str"), ({"value": 1.0}, "dict"), ((0, 1.0, True, 5), "tuple"), ((0,), "tuple")]
    )
    def test_load_rejects_other_types(self, data: Any, type_name: str) -> None:
        with pytest.raises(TypeError, match=f"from {type_name}"):
            ForecastInputDatapoint.load(data)


class TestForecastResultList:
    def test_load_raw_api_response_merges_the_responses_of_several_requests(self) -> None:
        def response(label: str, cohort: str | None) -> dict[str, Any]:
            item = {"label": label, "forecast": [{"timestamp": 60_000, "quantiles": {"0.5": 1.0}}]}
            return {
                "items": [item | ({"cohort": cohort} if cohort else {})],
                "metadata": {"quantileLevels": [0.5], "cohorts": [cohort] if cohort else []},
            }

        res = ForecastResultList._load_raw_api_response(
            [response("a", "train-b"), response("b", None), response("c", "train-a"), response("d", "train-b")]
        )

        assert [r.label for r in res] == ["a", "b", "c", "d"]
        assert res.cohorts == ["train-b", "train-a"]
        assert res.quantile_levels == [0.5]

    def test_load_response(self) -> None:
        res = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE])

        assert res.quantile_levels == [0.05, 0.5, 0.95]
        assert res.cohorts == ["compression-train-a"]
        first = res.get(label="23-PT-1101")
        assert first is not None
        assert first.cohort == "compression-train-a"
        assert first.forecast[0] == QuantileDatapoint(60_000, {0.05: 41.0, 0.5: 42.0, 0.95: 43.0})
        assert res.get("23-PT-1101") is first
        assert res.get(label="unknown") is None

    @pytest.mark.dsl
    def test_to_pandas_has_datetime_index_and_label_quantile_columns(self) -> None:
        pd = local_import("pandas")
        df = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE]).to_pandas(include_all_quantiles=True)

        assert isinstance(df.index, pd.DatetimeIndex)
        assert list(df.columns) == [(label, q) for label in ["23-PT-1101", "24-TT-3001"] for q in [0.05, 0.5, 0.95]]
        assert df[("23-PT-1101", 0.5)].tolist() == [42.0, 42.5]
        assert df[("24-TT-3001", 0.5)].iloc[0] == 31.0
        assert pd.isna(df[("24-TT-3001", 0.5)].iloc[1])  # no forecast at the second timestamp

    @pytest.mark.dsl
    def test_to_pandas_has_the_median_by_default(self) -> None:
        res = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE])
        single = res.get(label="23-PT-1101")
        assert single is not None

        df = res.to_pandas()
        assert list(df.columns) == ["23-PT-1101", "24-TT-3001"]
        assert df["23-PT-1101"].tolist() == [42.0, 42.5]
        assert list(single.to_pandas().columns) == ["23-PT-1101"]

    @pytest.mark.dsl
    def test_notebook_view_shows_the_median_only(self) -> None:
        res = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE])
        single = res.get(label="23-PT-1101")
        assert single is not None
        for html in (res._repr_html_(), single._repr_html_()):
            assert "23-PT-1101" in html
            assert "0.05" not in html and "0.95" not in html

    @pytest.mark.dsl
    def test_single_result_to_pandas(self) -> None:
        result = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE]).get(label="23-PT-1101")
        assert result is not None
        df = result.to_pandas(include_all_quantiles=True)
        assert list(df.columns) == [0.05, 0.5, 0.95]
        assert df.iloc[1][0.5] == 42.5

    @pytest.mark.dsl
    def test_to_pandas_after_slice_or_dump_load_keeps_quantile_columns(self) -> None:
        pd = local_import("pandas")
        levels = [0.05, 0.5, 0.95]
        res = ForecastResultList._load_raw_api_response([FORECAST_RESPONSE])

        sliced = res[:1].to_pandas(include_all_quantiles=True)
        assert list(sliced.columns) == [("23-PT-1101", q) for q in levels]
        assert len(sliced) == 2
        assert sliced[("23-PT-1101", 0.5)].tolist() == [42.0, 42.5]

        reloaded = ForecastResultList.load(res.dump()).to_pandas(include_all_quantiles=True)
        assert isinstance(reloaded.index, pd.DatetimeIndex)
        assert list(reloaded.columns) == [(label, q) for label in ["23-PT-1101", "24-TT-3001"] for q in levels]
        assert len(reloaded) == 2

    @pytest.mark.dsl
    @pytest.mark.parametrize("list_cls", [ForecastResultList, ImputeResultList])
    def test_to_pandas_on_empty_list_returns_empty_frame(self, list_cls: type) -> None:
        df = list_cls([]).to_pandas()
        assert df.empty


class TestImputeResultList:
    @pytest.mark.dsl
    def test_series_with_nothing_imputed_keeps_its_columns(self) -> None:
        response = {
            "items": [
                {"label": "a", "imputed": [{"timestamp": 60_000, "quantiles": {"0.5": 1.0}}]},
                {"label": "b", "imputed": []},
            ],
            "metadata": {"quantileLevels": [0.5], "cohorts": []},
        }
        res = ImputeResultList._load_raw_api_response([response])

        assert list(res.to_pandas().columns) == ["a", "b"]
        assert list(res.to_pandas(include_all_quantiles=True).columns) == [("a", 0.5), ("b", 0.5)]
        nothing_imputed = res.get(label="b")
        assert nothing_imputed is not None
        assert nothing_imputed.to_pandas().empty

    @pytest.mark.dsl
    def test_to_pandas_after_slice_or_dump_load_keeps_quantile_columns(self) -> None:
        levels = [0.05, 0.5, 0.95]
        response = {
            "items": [
                {
                    "label": "a",
                    "imputed": [
                        {"timestamp": 60_000, "quantiles": {"0.05": 1.0, "0.5": 2.0, "0.95": 3.0}},
                        {"timestamp": 120_000, "quantiles": {"0.05": 1.5, "0.5": 2.5, "0.95": 3.5}},
                    ],
                },
                {
                    "label": "b",
                    "imputed": [{"timestamp": 60_000, "quantiles": {"0.05": 4.0, "0.5": 5.0, "0.95": 6.0}}],
                },
            ],
            "metadata": {"quantileLevels": levels, "cohorts": []},
        }
        res = ImputeResultList._load_raw_api_response([response])

        sliced = res[:1].to_pandas(include_all_quantiles=True)
        assert list(sliced.columns) == [("a", q) for q in levels]
        assert len(sliced) == 2

        reloaded = ImputeResultList.load(res.dump()).to_pandas(include_all_quantiles=True)
        assert list(reloaded.columns) == [(label, q) for label in ["a", "b"] for q in levels]
        assert len(reloaded) == 2


class TestQuantileDatapoint:
    @pytest.mark.parametrize("key", ["0.5", "0.50", "5e-1"])
    def test_quantile_levels_are_parsed_as_floats(self, key: str) -> None:
        assert QuantileDatapoint._load({"timestamp": 0, "quantiles": {key: 1.0}}).quantiles == {0.5: 1.0}
