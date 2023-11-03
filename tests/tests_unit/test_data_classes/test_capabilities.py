from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.capabilities import (
    AllScope,
    Capability,
    ProjectsAcl,
    UnknownAcl,
    UnknownScope,
)


@pytest.fixture
def cap_with_extra_key():
    return {
        "extra-key": {"urlNames": ["my-sandbox"]},
        "projectsAcl": {"actions": ["UPDATE", "LIST", "READ"], "scope": {"all": {}}},
    }


@pytest.fixture
def unknown_cap_with_extra_key():
    return {
        "extra-key": {"urlNames": ["my-sandbox"]},
        "veryUnknownAcl": {"actions": ["DELETE"], "scope": {"all": {}}},
    }


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
        capability = Capability.load(raw)
        assert capability.dump(camel_case=True) == raw

    def test_load_dump__extra_top_level_keys(self, cap_with_extra_key) -> None:
        capability = Capability.load(cap_with_extra_key)
        assert isinstance(capability, ProjectsAcl)
        assert isinstance(capability.scope, AllScope)
        assert all(isinstance(action, ProjectsAcl.Action) for action in capability.actions)

        assert capability.dump(camel_case=True) != cap_with_extra_key
        cap_with_extra_key.pop("extra-key")
        assert capability.dump(camel_case=True) == cap_with_extra_key

    def test_load_dump_unknown__extra_top_level_keys(self, unknown_cap_with_extra_key) -> None:
        with pytest.raises(ValueError, match="^Unable to parse capability from API-response"):
            Capability.load(unknown_cap_with_extra_key)

    @pytest.mark.parametrize(
        "raw", [{"dataproductAcl": {"actions": ["UTILIZE"], "scope": {"components": {"ids": [1, 2, 3]}}}}]
    )
    def test_load_dump_unknown(self, raw: dict[str, Any]) -> None:
        capability = Capability.load(raw)
        assert isinstance(capability, UnknownAcl)
        assert isinstance(capability.scope, UnknownScope)
        assert all(action is UnknownAcl.Action.Unknown for action in capability.actions)
        assert capability.raw_data == raw
        assert capability.dump(camel_case=True) == raw
