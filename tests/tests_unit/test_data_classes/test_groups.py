from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes import Group


class TestGroups:
    @pytest.mark.parametrize(
        "raw",
        [
            {
                "name": "unknown group",
                "capabilities": [
                    {
                        "relationshipsAcl": {
                            "scope": {"datasetScope": {"ids": ["2332579319524236"]}},
                            "actions": ["READ", "WRITE"],
                        }
                    },
                ],
            }
        ],
    )
    def test_load_dump_unknown_group(self, raw: dict[str, Any]) -> None:
        group = Group.load(raw)
        assert group.dump(camel_case=True) == raw
