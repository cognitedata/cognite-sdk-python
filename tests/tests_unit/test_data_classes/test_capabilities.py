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
                    "actions": ["READ"],
                    "scope": {"datasetScope": {"ids": [1, 2, 3]}},
                }
            },
            {
                "securityCategoriesAcl": {"actions": ["MEMBEROF", "LIST"], "scope": {"idScope": {"ids": [1, 2, 3]}}},
            },
            {
                "rawAcl": {
                    "actions": ["READ", "WRITE", "LIST"],
                    "scope": {"tableScope": {"dbsToTables": {"databaseName": ["my_db1", "my_db2"]}}},
                },
            },
        ],
    )
    def test_load_dump(self, raw: dict[str, Any]) -> None:
        capability = capabilities.Capability.load(raw)
        assert capability.dump(camel_case=True) == raw

    @pytest.mark.parametrize(
        "raw", [{"dataproductAcl": {"actions": ["UTILIZE"], "scope": {"components": {"ids": [1, 2, 3]}}}}]
    )
    def test_load_dump_unknown(self, raw: dict[str, Any]) -> None:
        capability = capabilities.Capability.load(raw)
        assert isinstance(capability, capabilities.UnknownAcl)
        assert capability.raw_data == raw
        assert capability.dump(camel_case=True) == raw
