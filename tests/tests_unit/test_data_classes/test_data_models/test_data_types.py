import pytest

from cognite.client.data_classes._base import UnknownCogniteObject
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
    Enum,
    ListablePropertyType,
    PropertyType,
    PropertyTypeWithUnit,
    Text,
    UnitReference,
)
from tests.utils import all_concrete_subclasses


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
            {
                "type": "enum",
                "values": {
                    "string": {
                        "name": "string",
                        "description": "Time series with string data points.",
                    },
                    "numeric": {
                        "name": "numeric",
                        "description": "Time series with double floating point data points.",
                    },
                },
            },
        ],
        ids=lambda d: d.get("type", "unknown"),
    )
    def test_load_dump(self, data: dict) -> None:
        loaded = PropertyType.load(data)
        dumped = loaded.dump(camel_case=True)

        assert not isinstance(loaded, UnknownCogniteObject)
        assert data == dumped

    def test_load_ignore_unknown_properties(self) -> None:
        data = {"type": "float64", "list": True, "unit": {"externalId": "known"}, "unknownProperty": "unknown"}
        actual = PropertyType.load(data).dump(camel_case=True)
        data.pop("unknownProperty")
        assert data == actual

    def test_load_dump_unknown_property(self) -> None:
        data = {"type": "unknowngibberish", "list": True, "unit": {"externalId": "known"}}
        obj = PropertyType.load(data)
        assert isinstance(obj, UnknownCogniteObject)
        actual = obj.dump(camel_case=True)
        assert data == actual

    def test_load_text_without_collation(self) -> None:
        data = {"type": "text", "list": False}
        obj = PropertyType.load(data)
        assert isinstance(obj, Text)
        actual = obj.dump(camel_case=True)
        # Default collation is "ucs_basic"
        data["collation"] = "ucs_basic"
        assert data == actual

    def test_dump_enum_no_camel_casing_of_user_values(self) -> None:
        # Before SDK version 7.72.1, the Enum property type would camelCase the user-defined enum values
        obj = PropertyType.load(
            {
                "type": "enum",
                "values": {
                    "string_valWe ird_Case": {
                        "name": "string_valWe ird_Case",
                        "description": "Time series with string data points.",
                    },
                    "numeric_valWe ird_Case": {
                        "name": "numeric_valWe ird_Case",
                        "description": "Time series with double floating point data points.",
                    },
                },
            }
        )
        assert isinstance(obj, Enum)

        expected_values = ["numeric_valWe ird_Case", "string_valWe ird_Case"]
        assert sorted(obj.values) == expected_values
        assert sorted(obj.dump(camel_case=False)["values"]) == expected_values
        assert sorted(obj.dump(camel_case=True)["values"]) == expected_values

    @pytest.mark.parametrize("lst_cls", all_concrete_subclasses(ListablePropertyType))
    def test_is_list_property_with_max_list_size(self, lst_cls: ListablePropertyType) -> None:
        type_name = lst_cls._type
        prop = PropertyType.load({"type": type_name, "list": True})
        assert isinstance(prop, ListablePropertyType)
        assert prop.max_list_size is None

        prop = PropertyType.load({"type": type_name, "list": True, "maxListSize": 10})
        assert isinstance(prop, ListablePropertyType)
        assert prop.max_list_size == 10

        with pytest.raises(ValueError, match="^is_list must be True if max_list_size is set$"):
            PropertyType.load({"type": type_name, "list": False, "maxListSize": 10})


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
