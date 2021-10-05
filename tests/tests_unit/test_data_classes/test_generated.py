import json

from cognite.client.data_classes import AggregateResultItem, TimestampRange


class TestGenerated:
    def test_timestamprange(self):
        tsr = TimestampRange(min=1, max=2)
        assert 1 == tsr.min
        assert 2 == tsr.max
        assert 1 == tsr["min"]
        assert 2 == tsr["max"]
        tsr.max = 3
        assert 3 == tsr["max"]
        tsr["max"] = 4
        assert 4 == tsr["max"]
        assert {"max": 4, "min": 1} == json.loads(json.dumps(tsr))

    def test_one_missing(self):
        tsr = TimestampRange(min=1)
        assert {"min": 1} == json.loads(json.dumps(tsr))

    def test_typo(self):
        tsr = TimestampRange()
        assert tsr.min is None
        assert tsr.get("min") is None
        assert {} == json.loads(json.dumps(tsr))
        tsr["max"] = 4
        assert {"max": 4} == json.loads(json.dumps(tsr))
        tsr.mun = 4
        assert {"max": 4} == json.loads(json.dumps(tsr))

    def test_camels(self):
        ag = AggregateResultItem(child_count=23)
        assert 23 == ag.child_count
        assert 23 == ag["childCount"]
        assert ag.get("child_count") is None
        assert {"childCount": 23} == json.loads(json.dumps(ag))
