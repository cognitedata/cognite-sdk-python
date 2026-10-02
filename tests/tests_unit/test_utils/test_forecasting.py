from __future__ import annotations

import random
from typing import Any

import pytest

from cognite.client.data_classes.ai import InputTimeSeries
from cognite.client.utils._forecasting import (
    MAX_HISTORY,
    MAX_SERIES_PER_COHORT,
    MAX_SERIES_PER_REQUEST,
    SourceSeries,
    build_inputs,
    split_into_requests,
)

COHORTS = [None, "train-a", "train-b", "train-c", "train-d"]


class TestSplitIntoRequests:
    @pytest.mark.parametrize("seed", range(50))
    def test_respects_limits_and_never_splits_a_cohort(self, seed: int) -> None:
        rng = random.Random(seed)
        cohorts = [rng.choice(COHORTS) for _ in range(rng.randint(1, 700))]
        series = [InputTimeSeries(str(i), [(0, 1.0)], c) for i, c in enumerate(cohorts)]
        named = [c for c in COHORTS if c is not None]
        if any(cohorts.count(c) > MAX_SERIES_PER_COHORT for c in named):
            with pytest.raises(ValueError, match="Cohort"):
                split_into_requests(series)
            return

        requests = split_into_requests(series)

        assert sorted(s.label for r in requests for s in r) == sorted(s.label for s in series)
        assert all(len(r) <= MAX_SERIES_PER_REQUEST for r in requests)
        for c in named:
            assert sum(any(s.cohort == c for s in r) for r in requests) <= 1

    def test_oversized_cohort_is_rejected(self) -> None:
        series = [InputTimeSeries(str(i), [(0, 1.0)], "train-a") for i in range(MAX_SERIES_PER_COHORT + 1)]
        with pytest.raises(ValueError, match="Cohort 'train-a'"):
            split_into_requests(series)


def source(label: str, timestamps: list[int], values: list[float | None] | None = None) -> SourceSeries:
    return SourceSeries(label, timestamps, values if values is not None else [1.0] * len(timestamps))


def datapoints(series: InputTimeSeries) -> list[dict[str, Any]]:
    return series.dump()["datapoints"]


class TestBuildInputs:
    @pytest.mark.parametrize("seed", range(50))
    def test_regular_series_with_gaps_is_placed_on_its_grid(self, seed: int) -> None:
        rng = random.Random(seed)
        step = rng.randint(1, 3_600_000)
        n = rng.randint(3, 200)
        keep = [True, True] + [rng.random() > 0.3 for _ in range(n - 3)] + [True]  # first two adjacent fix the step
        timestamps = [i * step for i, k in enumerate(keep) if k]

        [series] = build_inputs([source("21-PT-1019", timestamps)], step_ms=None, cohort=None)

        dps = datapoints(series)
        assert [dp["timestamp"] for dp in dps] == list(range(0, timestamps[-1] + 1, step))
        assert {dp["timestamp"] for dp in dps if not dp.get("missing")} == set(timestamps)

    def test_explicit_step_marks_empty_buckets_missing(self) -> None:
        [series] = build_inputs([source("21-PT-1019", [0, 10, 30])], step_ms=10, cohort=None)
        assert [dp.get("missing", False) for dp in datapoints(series)] == [False, False, True, False]

    def test_cohort_shares_one_grid(self) -> None:
        inputs = build_inputs(
            [source("23-PT-1101", [0, 10, 20]), source("23-PT-1201", [10, 20, 30])], step_ms=None, cohort="train-a"
        )
        assert [[dp["timestamp"] for dp in datapoints(s)] for s in inputs] == [[0, 10, 20, 30], [0, 10, 20, 30]]
        assert {s.cohort for s in inputs} == {"train-a"}

    def test_mask_hides_values_and_fill_gaps_controls_whether_gaps_are_returned(self) -> None:
        sources = [source("21-PT-1019", [0, 10, 30, 40], [1.0, 2.0, 4.0, 5.0])]

        [masked_only] = build_inputs(sources, step_ms=None, cohort=None, mask=[(10, 10)], fill_gaps=False)
        assert datapoints(masked_only)[1:3] == [
            {"timestamp": 10, "value": 2.0, "missing": True},
            {"timestamp": 20, "value": None},
        ]

        [with_gaps] = build_inputs(sources, step_ms=None, cohort=None, fill_gaps=True)
        assert datapoints(with_gaps)[2] == {"timestamp": 20, "value": None, "missing": True}

    def test_nan_values_are_gaps(self) -> None:
        [series] = build_inputs(
            [source("21-PT-1019", [0, 10, 20], [1.0, float("nan"), 3.0])], step_ms=None, cohort=None, fill_gaps=True
        )
        assert datapoints(series)[1] == {"timestamp": 10, "value": None, "missing": True}

    @pytest.mark.parametrize(
        "sources, step_ms, cohort, mask, message",
        [
            pytest.param([source("a", [0])], None, None, (), "has 1 datapoints", id="one point"),
            pytest.param([source("a", [0, 10, 25])], None, None, (), "not evenly spaced", id="irregular"),
            pytest.param(
                [source("a", [0, 10]), source("b", [0, 20])], None, "c", (), "must share one spacing", id="mixed"
            ),
            pytest.param([source("a", [0, 10]), source("b", [5, 15])], 10, "c", (), "off the common grid", id="grid"),
            pytest.param([source("a", [0, MAX_HISTORY])], 1, None, (), "spans 4097 grid points", id="too long"),
            pytest.param([source("a", [0, 10])], None, None, [(100, 200)], "doesn't cover", id="mask miss"),
        ],
    )
    def test_raises_instead_of_resampling_or_truncating(
        self,
        sources: list[SourceSeries],
        step_ms: int | None,
        cohort: str | None,
        mask: list[tuple[int, int]],
        message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message):
            build_inputs(sources, step_ms, cohort, mask=mask)

    def test_hint_is_appended_for_matching_problem(self) -> None:
        with pytest.raises(ValueError, match=r"^'a' is not evenly spaced\. Use aggregates\.$"):
            build_inputs([source("a", [0, 10, 25])], None, None, hints={"irregular": "Use aggregates."})

    def test_error_without_matching_hint_has_only_the_problem(self) -> None:
        with pytest.raises(ValueError, match=r"^'a' is not evenly spaced\.$"):
            build_inputs([source("a", [0, 10, 25])], None, None, hints={"too_long": "Narrow the window."})
