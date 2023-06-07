from __future__ import annotations

import pytest

from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId, load_identifier


class TestContainerReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "type": "container"}

        assert data == ContainerId.load(data).dump(camel_case=True)


class TestViewReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"}

        assert data == ViewId.load(data).dump(camel_case=True)


class TestLoadIdentifier:
    @pytest.mark.parametrize(
        "ids, expected_dict, expected_is_singleton",
        [
            (("space", "container"), [{"space": "space", "externalId": "container"}], True),
            ([("space", "container")], [{"space": "space", "externalId": "container"}], False),
        ],
    )
    def test_load(self, ids, expected_dict: list | dict, expected_is_singleton: bool):
        identifier = load_identifier(ids)

        assert identifier.as_dicts() == expected_dict
        assert (
            identifier.is_singleton() == expected_is_singleton
        ), f"Expected {expected_is_singleton} but got {identifier.is_singleton()}"
