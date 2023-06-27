import pytest

from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    DirectRelationReference,
    PropertyType,
)


class TestDirectRelationReference:
    def test_load_dump(self) -> None:
        data = {"space": "mySpace", "externalId": "myId"}

        assert data == DirectRelationReference.load(data).dump(camel_case=True)


class TestPropertyType:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": "text", "collation": "ucs_basic", "list": False},
            {"type": "int32", "list": True},
            {"type": "timeseries", "list": False},
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = PropertyType.load(data).dump(camel_case=True)

        assert data == actual


class TestDirectRelation:
    def test_load_dump_container_direct_relation(self) -> None:
        data = {"type": "direct", "container": {"space": "mySpace", "externalId": "myId", "type": "container"}}

        assert data == DirectRelation.load(data).dump(camel_case=True)
