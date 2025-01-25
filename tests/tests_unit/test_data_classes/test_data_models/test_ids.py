from __future__ import annotations

from typing import Literal

import pytest

from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId, _load_identifier, _load_space_identifier


class TestContainerReference:
    def test_load_dump(self) -> None:
        data = {"space": "mySpace", "externalId": "myId", "type": "container"}

        assert data == ContainerId.load(data).dump(camel_case=True)


class TestViewReference:
    def test_load_dump(self) -> None:
        data = {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"}

        assert data == ViewId.load(data).dump(camel_case=True)


class TestLoadIdentifier:
    @pytest.mark.parametrize(
        "ids, id_type, expected_dict, expected_is_singleton",
        [
            (("space", "container"), "container", [{"space": "space", "externalId": "container"}], True),
            ([("space", "container")], "container", [{"space": "space", "externalId": "container"}], False),
            (("space", "view", "v1"), "view", [{"space": "space", "externalId": "view", "version": "v1"}], False),
            ([("space", "view", "v1")], "view", [{"space": "space", "externalId": "view", "version": "v1"}], False),
            (
                ("space", "myDataModel", "v1"),
                "data_model",
                [{"space": "space", "externalId": "myDataModel", "version": "v1"}],
                False,
            ),
            (
                ("space", "externalId"),
                "node",
                [{"externalId": "externalId", "instanceType": "node", "space": "space"}],
                True,
            ),
            (
                ("space", "externalId"),
                "edge",
                [{"externalId": "externalId", "instanceType": "edge", "space": "space"}],
                True,
            ),
        ],
    )
    def test_load(
        self,
        ids: tuple[str, str] | tuple[str, str, str],
        id_type: Literal["container", "view", "data_model"],
        expected_dict: list | dict,
        expected_is_singleton: bool,
    ) -> None:
        identifier = _load_identifier(ids, id_type)

        assert identifier.as_dicts() == expected_dict
        assert identifier.is_singleton() == expected_is_singleton, (
            f"Expected {expected_is_singleton} but got {identifier.is_singleton()}"
        )

    @pytest.mark.parametrize(
        "ids, expected_dict, expected_is_singleton",
        [("spaceId", [{"space": "spaceId"}], True), (["spaceId"], [{"space": "spaceId"}], False)],
    )
    def test_load_space_identifier(
        self,
        ids: tuple[str, str] | tuple[str, str, str],
        expected_dict: list | dict,
        expected_is_singleton: bool,
    ) -> None:
        identifier = _load_space_identifier(ids)

        assert identifier.as_dicts() == expected_dict
        assert identifier.is_singleton() == expected_is_singleton, (
            f"Expected {expected_is_singleton} but got {identifier.is_singleton()}"
        )
