import pytest

from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
    PropertyType,
    PropertyTypeWithUnit,
    UnitReference,
    UnknownPropertyType,
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
            {"type": "boolean", "list": False},
            {"type": "float32", "list": False},
            {"type": "float64", "list": False},
            {"type": "int32", "list": True},
            {"type": "int64", "list": True},
            {"type": "timeseries", "list": False},
            {"type": "file", "list": False},
            {"type": "sequence", "list": False},
            {"type": "json", "list": False},
            {"type": "timestamp", "list": False},
            {
                "type": "direct",
                "container": {"space": "mySpace", "externalId": "myId", "type": "container"},
                "list": True,
            },
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = PropertyType.load(data).dump(camel_case=True)

        assert data == actual

    def test_load_ignore_unknown_properties(self) -> None:
        data = {"type": "float64", "list": True, "unit": {"externalId": "known"}, "unknownProperty": "unknown"}
        actual = PropertyType.load(data).dump(camel_case=True)
        data.pop("unknownProperty")
        assert data == actual

    def test_load_dump_unkown_property(self) -> None:
        data = {"type": "unknowngibberish", "list": True, "unit": {"externalId": "known"}}
        obj = PropertyType.load(data)
        assert isinstance(obj, UnknownPropertyType)
        actual = obj.dump(camel_case=True)
        assert data == actual


class TestUnitSupport:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": "float32", "list": False, "unit": {"externalId": "temperature:celsius"}},
            {"type": "float64", "list": False, "unit": {"externalId": "temperature:celsius"}},
            {"type": "int32", "list": True, "unit": {"externalId": "temperature:celsius"}},
            {"type": "int64", "list": True, "unit": {"externalId": "temperature:celsius"}},
        ],
    )
    def test_load_dump_property_with_unit(self, data: dict) -> None:
        property = PropertyTypeWithUnit.load(data)
        assert isinstance(property.unit, UnitReference)
        assert data == property.dump(camel_case=True)
