import pytest

from cognite.client.data_classes._base import UnknownCogniteObject
from cognite.client.data_classes.data_modeling import data_types
from cognite.client.data_classes.data_modeling.containers import (
    Constraint,
    Container,
    ContainerApply,
    ContainerProperty,
    Index,
)


class TestContainer:
    def test_as_property_ref(self) -> None:
        params = dict(
            space="sp",
            externalId="ex",
            properties={},
            isGlobal=False,
            lastUpdatedTime=123,
            createdTime=12,
            usedFor="node",
        )
        cont = Container.load(params)
        cont_apply = ContainerApply.load(params)

        assert cont.as_property_ref("foo") == ("sp", "ex", "foo")
        assert cont_apply.as_property_ref("foo") == ("sp", "ex", "foo")


class TestContainerProperty:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": {"type": "direct", "list": False}, "immutable": False},
            # List is not required, but gets set and thus will be dumped
            {"type": {"type": "int32", "list": False}, "immutable": False},
            {"type": {"type": "text", "list": False, "collation": "ucs_basic"}, "immutable": False},
            {"type": {"type": "file", "list": False}, "immutable": False},
        ],
    )
    def test_load_dump__only_required(self, data: dict) -> None:
        actual = ContainerProperty.load(data).dump(camel_case=True)
        assert data == actual

    def test_dump_no_longer_camelCases_everything_when_used(self) -> None:
        cp = ContainerProperty(
            data_types.Enum(
                {
                    "Closed_I_think": data_types.EnumValue("Valve_is_closed"),
                    "Opened or not": data_types.EnumValue("Valve is opened"),
                }
            )
        )
        assert ContainerProperty._load(cp.dump()) == cp
        assert sorted(cp.dump(camel_case=True)["type"]["values"]) == ["Closed_I_think", "Opened or not"]  # type: ignore [index]


class TestConstraint:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "require": {"type": "container", "space": "mySpace", "externalId": "myExternalId"},
                "constraintType": "requires",
            },
            {"properties": ["name", "fullName"], "constraintType": "uniqueness"},
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = Constraint.load(data).dump(camel_case=True)
        assert data == actual

    def test_load_unknown_type(self) -> None:
        data = {"someUnknownConstraint": {"wawa": "wiwa"}, "constraintType": "unknown"}
        obj = Constraint.load(data)
        assert isinstance(obj, UnknownCogniteObject)
        assert obj.dump() == data


class TestIndex:
    @pytest.mark.parametrize(
        "data",
        [
            {"properties": ["name", "fullName"], "indexType": "btree", "cursorable": True},
            {"properties": ["name", "fullName"], "indexType": "inverted"},
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize(
        "data",
        [
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "indexType": "btree", "cursorable": True},
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "indexType": "inverted"},
        ],
    )
    def test_load_dump__no_fail_on_unseen_key(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=True)
        data.pop("this-key-is-new-sooo-new")
        assert data == actual

    @pytest.mark.parametrize(
        "data", [{"properties": ["name"], "indexType": "btree"}, {"properties": ["name"], "indexType": "inverted"}]
    )
    def test_load_dump__only_required(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=True)
        assert data == actual

    def test_load_unknown_type(self) -> None:
        data = {"someUnknownIndexType": {"wawa": "wiwa"}, "indexType": "someUnknownIndexType"}
        obj = Index.load(data)
        assert isinstance(obj, UnknownCogniteObject)
        assert obj.dump() == data


class TestConstraints:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "require": {"type": "container", "space": "mySpace", "externalId": "myExternalId"},
                "constraintType": "requires",
            },
            {"properties": ["name", "fullName"], "constraintType": "uniqueness"},
        ],
    )
    def test_load_dump(self, data: dict) -> None:
        actual = Constraint.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize(
        "data",
        [
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "constraintType": "uniqueness"},
            {
                "this-key-is-new-sooo-new": 42,
                "require": {"space": "hehe", "externalId": "hoho", "type": "container"},
                "constraintType": "requires",
            },
        ],
    )
    def test_load_dump__no_fail_on_unseen_key(self, data: dict) -> None:
        actual = Constraint.load(data).dump(camel_case=True)
        data.pop("this-key-is-new-sooo-new")
        assert data == actual
