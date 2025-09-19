from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.data_modeling.debug import DebugNotice, DebugParameters, UnknownDebugNotice


class TestDebugParameters:
    @pytest.mark.parametrize(
        "emit_results, timeout, profile, camel_case, expected",
        [
            (False, 1234, True, True, {"emitResults": False, "timeout": 1234, "profile": True}),
            (False, 1234, True, False, {"emit_results": False, "timeout": 1234, "profile": True}),
            (True, None, False, True, {"emitResults": True, "profile": False}),
            (True, None, False, False, {"emit_results": True, "profile": False}),
        ],
    )
    def test_debug_parameters(
        self, emit_results: bool, timeout: int | None, profile: bool, camel_case: bool, expected: dict[str, Any]
    ) -> None:
        params = DebugParameters(emit_results=emit_results, timeout=timeout, profile=profile)
        assert params.dump(camel_case=camel_case) == expected


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
