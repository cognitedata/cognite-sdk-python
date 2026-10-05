from __future__ import annotations

import random

import pytest

from cognite.client.data_classes.ai import InputTimeSeries
from cognite.client.utils._forecasting import MAX_SERIES_PER_COHORT, MAX_SERIES_PER_REQUEST, split_into_requests

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
