from __future__ import annotations

from typing import Literal

import pytest

from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId, _load_identifier


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
        "ids, id_type, expected_dict, expected_is_singleton",
        [
            (("space", "container"), "container", [{"space": "space", "externalId": "container"}], True),
            ([("space", "container")], "container", [{"space": "space", "externalId": "container"}], False),
            (("space", "view", "v1"), "view", [{"space": "space", "externalId": "view", "version": "v1"}], False),
            (
                ("space", "myDataModel", "v1"),
                "data_model",
                [{"space": "space", "externalId": "myDataModel", "version": "v1"}],
                False,
            ),
        ],
    )
    def test_load(
        self,
        ids,
        id_type: Literal["container", "view", "data_model"],
        expected_dict: list | dict,
        expected_is_singleton: bool,
    ):
        identifier = _load_identifier(ids, id_type)

        assert identifier.as_dicts() == expected_dict
        assert (
            identifier.is_singleton() == expected_is_singleton
        ), f"Expected {expected_is_singleton} but got {identifier.is_singleton()}"
