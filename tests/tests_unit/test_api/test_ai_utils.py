from __future__ import annotations

import random
from collections.abc import Sequence

import pytest

from cognite.client._api.ai import utils
from cognite.client._api.ai.utils import (
    MAX_REQUEST_BYTES,
    MAX_SERIES_PER_REQUEST,
    group_series_by_cohort,
    pack_series_requests,
    validate_series_groups,
)
from cognite.client.data_classes.ai import ForecastInputDatapoint, ForecastInputTimeSeries

COHORTS = [None] * 12 + [f"train-{i}" for i in range(12)]


def _series(label: str, cohort: str | None = None) -> ForecastInputTimeSeries:
    return ForecastInputTimeSeries(label, [ForecastInputDatapoint(0, 1.0), ForecastInputDatapoint(60_000, 1.0)], cohort)


def _one_byte_each(series: Sequence[ForecastInputTimeSeries]) -> int:
    return len(series)


def _split(series: list[ForecastInputTimeSeries]) -> list[list[ForecastInputTimeSeries]]:
    groups = group_series_by_cohort(series)
    validate_series_groups(series, groups)
    return pack_series_requests(series, groups)


class TestSplitIntoRequests:
    @pytest.mark.parametrize("seed", range(50))
    def test_respects_limits_and_never_splits_a_cohort(self, seed: int, monkeypatch: pytest.MonkeyPatch) -> None:
        rng = random.Random(seed)
        cohorts = [rng.choice(COHORTS) for _ in range(rng.randint(1, 200))]
        series = [_series(str(i), c) for i, c in enumerate(cohorts)]
        # Large series on some seeds and small on others, so that both limits decide where requests end.
        max_size = MAX_REQUEST_BYTES // rng.choice([8, 400])
        sizes = {s.label: rng.randint(1, max_size) for s in series}

        def size_of(group: Sequence[ForecastInputTimeSeries]) -> int:
            return sum(sizes[s.label] for s in group)

        monkeypatch.setattr(utils, "size_as_sent", size_of)

        requests = _split(series)

        assert sorted(s.label for r in requests for s in r) == sorted(s.label for s in series)
        assert all(len(r) <= MAX_SERIES_PER_REQUEST for r in requests)
        assert all(size_of(r) <= MAX_REQUEST_BYTES for r in requests)
        for c in set(COHORTS) - {None}:
            assert sum(any(s.cohort == c for s in r) for r in requests) <= 1
        for r in requests:
            assert [int(s.label) for s in r] == sorted(int(s.label) for s in r)

    def test_keeps_input_order_within_a_request(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(utils, "size_as_sent", _one_byte_each)
        series = [_series("a", "train-a"), _series("b"), _series("c", "train-a")]
        assert [[s.label for s in r] for r in _split(series)] == [["a", "b", "c"]]

    def test_cohort_with_too_many_series_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(utils, "size_as_sent", _one_byte_each)
        series = [_series(str(i), "train-a") for i in range(MAX_SERIES_PER_REQUEST + 1)]
        with pytest.raises(ValueError, match="Cohort 'train-a' has 33 series"):
            _split(series)

    def test_cohort_over_the_size_limit_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(utils, "size_as_sent", lambda group: len(group) * 3 * 2**20)
        with pytest.raises(ValueError, match=r"Cohort 'train-a' is 6\.0 MiB as sent"):
            _split([_series("a", "train-a"), _series("b", "train-a")])

    def test_series_over_the_size_limit_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(utils, "size_as_sent", lambda group: 6 * 2**20)
        with pytest.raises(ValueError, match=r"Series 'big' is 6\.0 MiB as sent"):
            _split([_series("big")])

    def test_empty_input_returns_no_requests(self) -> None:
        assert _split([]) == []
