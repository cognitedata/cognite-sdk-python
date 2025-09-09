from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cognite.client.data_classes.data_modeling.debug import DebugNotice, UnknownDebugNotice
from cognite.client.utils import _json
from tests.utils import FakeCogniteResourceGenerator, all_concrete_subclasses

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TestDebugNotices:
    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "debug_notice_subclass",
        [
            pytest.param(cls, id=f"{cls.__name__} in {cls.__module__}")
            for cls in all_concrete_subclasses(DebugNotice, exclude={UnknownDebugNotice})
        ],
    )
    def test_dump_load_roundtrip(
        self, debug_notice_subclass: type[DebugNotice], cognite_mock_client_placeholder: CogniteClient
    ) -> None:
        instance_generator = FakeCogniteResourceGenerator(seed=12345, cognite_client=cognite_mock_client_placeholder)
        instance = instance_generator.create_instance(debug_notice_subclass)

        dumped = instance.dump(camel_case=True)
        roundtrip = _json.loads(_json.dumps(dumped))
        loaded = instance.load(roundtrip)

        assert loaded.dump() == instance.dump()

    def test_unknown_debug_notice(self) -> None:
        data = {"code": "very unknown", "category": "also unknown", "weIrdFiElD": 123}
        notice = DebugNotice.load(data)
        assert isinstance(notice, UnknownDebugNotice)
        assert notice.dump() == data
