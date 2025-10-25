from __future__ import annotations

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
