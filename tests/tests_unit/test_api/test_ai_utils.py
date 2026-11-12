from __future__ import annotations

import random
from collections.abc import Sequence

import pytest

from cognite.client._api.ai.utils import MAX_REQUEST_BYTES, MAX_SERIES_PER_REQUEST, split_into_requests
from cognite.client.data_classes.ai import ForecastInputDatapoint, ForecastInputTimeSeries

COHORTS = [None] * 12 + [f"train-{i}" for i in range(12)]


def _series(label: str, cohort: str | None = None) -> ForecastInputTimeSeries:
    return ForecastInputTimeSeries(label, [ForecastInputDatapoint(0, 1.0), ForecastInputDatapoint(60_000, 1.0)], cohort)


def _one_byte_each(series: Sequence[ForecastInputTimeSeries]) -> int:
    return len(series)


class TestSplitIntoRequests:
    @pytest.mark.parametrize("seed", range(50))
    def test_respects_limits_and_never_splits_a_cohort(self, seed: int) -> None:
        rng = random.Random(seed)
        cohorts = [rng.choice(COHORTS) for _ in range(rng.randint(1, 200))]
        series = [_series(str(i), c) for i, c in enumerate(cohorts)]
        # Large series on some seeds and small on others, so that both limits decide where requests end.
        max_size = MAX_REQUEST_BYTES // rng.choice([8, 400])
        sizes = {s.label: rng.randint(1, max_size) for s in series}

        def size_of(group: Sequence[ForecastInputTimeSeries]) -> int:
            return sum(sizes[s.label] for s in group)

        requests = split_into_requests(series, size_of)

        assert sorted(s.label for r in requests for s in r) == sorted(s.label for s in series)
        assert all(len(r) <= MAX_SERIES_PER_REQUEST for r in requests)
        assert all(size_of(r) <= MAX_REQUEST_BYTES for r in requests)
        for c in set(COHORTS) - {None}:
            assert sum(any(s.cohort == c for s in r) for r in requests) <= 1
        for r in requests:
            assert [int(s.label) for s in r] == sorted(int(s.label) for s in r)

    def test_keeps_input_order_within_a_request(self) -> None:
        series = [_series("a", "train-a"), _series("b"), _series("c", "train-a")]
        assert [[s.label for s in r] for r in split_into_requests(series, _one_byte_each)] == [["a", "b", "c"]]

    def test_cohort_with_too_many_series_is_rejected(self) -> None:
        series = [_series(str(i), "train-a") for i in range(MAX_SERIES_PER_REQUEST + 1)]
        with pytest.raises(ValueError, match="Cohort 'train-a' has 33 series"):
            split_into_requests(series, _one_byte_each)

    def test_cohort_over_the_size_limit_is_rejected(self) -> None:
        series = [_series("a", "train-a"), _series("b", "train-a")]
        with pytest.raises(ValueError, match=r"Cohort 'train-a' is 6\.0 MiB as sent"):
            split_into_requests(series, lambda group: len(group) * 3 * 2**20)

    def test_series_over_the_size_limit_is_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"Series 'big' is 6\.0 MiB as sent"):
            split_into_requests([_series("big")], lambda group: 6 * 2**20)

    def test_empty_input_returns_no_requests(self) -> None:
        assert split_into_requests([], _one_byte_each) == []
