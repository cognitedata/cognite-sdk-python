from __future__ import annotations
from datetime import datetime, timedelta, timezone

import pytest

from cognite.client.data_classes import AggregateResultItem, TimestampRange


class TestTimestampRange:
    def test_timestamprange(self) -> None:
        tsr = TimestampRange(min=1, max=2)
        assert 1 == tsr.min
        assert 2 == tsr.max
        tsr.max = 3
        assert {"max": 3, "min": 1} == tsr.dump()

    def test_one_missing(self) -> None:
        tsr = TimestampRange(min=1)
        assert {"min": 1} == tsr.dump()

    def test_empty_and_update(self) -> None:
        tsr = TimestampRange()
        assert tsr.min is None
        assert {} == tsr.dump()

        tsr.max = 4
        assert {"max": 4} == tsr.dump()

    def test_camels(self) -> None:
        ag = AggregateResultItem(child_count=23, depth=1, path=[])
        assert 23 == ag.child_count
        assert {"childCount": 23, "depth": 1, "path": []} == ag.dump(camel_case=True)

    def test_datetime(self):
        min_time = datetime.fromtimestamp(1767222000, timezone.utc)  # 2026-01-01 00:00:00 GMT+01
        max_time = datetime.fromtimestamp(1767308400, timezone.utc)  # 2026-01-02 00:00:00 GMT+01
        tsr = TimestampRange(min=min_time, max=max_time)
        assert tsr.min == 1767222000000
        assert tsr.max == 1767308400000
        assert {"min": 1767222000000, "max": 1767308400000} == tsr.dump()

    def test_time_shift_string(self):
        now_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        day_ago_time = now_time - timedelta(days=1) // timedelta(milliseconds=1)
        tsr = TimestampRange(min="1d-ago", max="now")
        # NOTE: Using approx because of the small time difference between calls
        assert tsr.min == pytest.approx(day_ago_time, 100)
        assert tsr.max == pytest.approx(now_time, 100)
