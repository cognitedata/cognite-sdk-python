import pytest

from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    DirectRelationReference,
    Float32,
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
            {"type": "int32", "list": True, "unit": "temperature:deg_c"},
            {"type": "timeseries", "list": False},
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = PropertyType.load(data).dump(camel_case=True)

        assert data == actual

    def test_load_ignore_unknown_properties(self) -> None:
        data = {"type": "float64", "list": True, "unit": "known", "super_unit": "unknown"}
        actual = PropertyType.load(data).dump(camel_case=True)
        data.pop("super_unit")

        assert data == actual


class TestDirectRelation:
    def test_load_dump_container_direct_relation(self) -> None:
        data = {"type": "direct", "container": {"space": "mySpace", "externalId": "myId", "type": "container"}}

        assert data == DirectRelation.load(data).dump(camel_case=True)


class TestUnitSupport:
    def test_load_dump_property_with_unit(self) -> None:
        data = {"type": "float32", "list": False, "unit": {"space": "cdf_units", "externalId": "temperature:celsius"}}
        property = Float32.load(data)
        assert isinstance(property.unit, NodeId)
        assert data == property.dump(camel_case=True)
