from __future__ import annotations

from cognite.client.data_classes.data_modeling.debug import DebugNotice, UnknownDebugNotice


class TestDebugNotices:
    def test_unknown_debug_notice(self) -> None:
        # Unknown code and category, but has the required keys:
        data = {"code": "very unknown", "category": "also unknown", "weIrdFiElD": 123}
        notice = DebugNotice.load(data)
        assert isinstance(notice, UnknownDebugNotice)
        assert notice.dump() == data

    def test_unknown_debug_notice__missing_required_keys(self) -> None:
        data = {"truly unknown": "A"}
        notice = DebugNotice.load(data)
        assert isinstance(notice, UnknownDebugNotice)
        assert notice.dump() == data
