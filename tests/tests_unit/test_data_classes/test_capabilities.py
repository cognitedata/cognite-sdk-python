from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes import capabilities


class TestCapabilities:
    @pytest.mark.parametrize(
        "raw",
        [
            {
                "assetsAcl": {
                    "actions": ["Read"],
                    "scope": {"dataSetScope": {"dataSetIds": [1, 2, 3]}},
                }
            },
        ],
    )
    def test_load_dump(self, raw: dict[str, Any]) -> None:
        capability = capabilities.Capability.load(raw)
        assert capability.dump(camel_case=True) == raw
