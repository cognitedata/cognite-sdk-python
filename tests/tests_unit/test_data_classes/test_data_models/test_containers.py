from __future__ import annotations

import pytest

from cognite.client.data_classes._base import UnknownCogniteResource
from cognite.client.data_classes.data_modeling import data_types
from cognite.client.data_classes.data_modeling.containers import (
    Constraint,
    ConstraintApply,
    ConstraintCore,
    Container,
    ContainerApply,
    ContainerCore,
    ContainerProperty,
    ContainerPropertyApply,
    Index,
    IndexApply,
    IndexCore,
    PropertyConstraintState,
)


class TestContainer:
    @pytest.mark.parametrize("container_class", [Container, ContainerApply])
    def test_as_property_ref(self, container_class: type[ContainerCore]) -> None:
        params = dict(
            space="sp",
            externalId="ex",
            properties={},
            isGlobal=False,
            lastUpdatedTime=123,
            createdTime=12,
            usedFor="node",
        )
        cont = container_class.load(params)
        assert cont.as_property_ref("foo") == ("sp", "ex", "foo")


class TestContainerProperty:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": {"type": "direct", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
            # List is not required, but gets set and thus will be dumped
            {"type": {"type": "int32", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
            {
                "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                "immutable": False,
                "autoIncrement": False,
                "nullable": True,
            },
            {"type": {"type": "file", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
        ],
    )
    def test_load_dump__only_required_apply(self, data: dict) -> None:
        """Test ContainerPropertyApply with minimal data (no constraintState needed)"""
        actual = ContainerPropertyApply.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize("as_apply", [False, True])
    @pytest.mark.parametrize(
        "data",
        [
            {
                "type": {"type": "direct", "list": False},
                "nullable": False,
                "constraintState": {"nullability": "current"},
                "immutable": False,
                "autoIncrement": False,
            },
            {
                "type": {"type": "int32", "list": True, "maxListSize": 10},
                "immutable": False,
                "nullable": False,
                "constraintState": {"maxListSize": "pending", "nullability": "current"},
                "autoIncrement": False,
            },
            {
                "type": {"type": "text", "list": True, "collation": "ucs_basic", "maxTextSize": 10, "maxListSize": 20},
                "immutable": False,
                "nullable": True,
                "constraintState": {"maxTextSize": "failed", "maxListSize": "pending"},
                "autoIncrement": False,
            },
        ],
    )
    def test_load_dump_constraint_state(self, data: dict, as_apply: bool) -> None:
        cp = ContainerProperty.load(data)
        assert cp.constraint_state is not None
        if not cp.nullable:
            assert cp.constraint_state.nullability == "current"
        else:
            assert cp.constraint_state.nullability is None
        if isinstance(cp.type, data_types.ListablePropertyType) and cp.type.max_list_size:
            assert cp.constraint_state.max_list_size == "pending"
        else:
            assert cp.constraint_state.max_list_size is None
        if isinstance(cp.type, data_types.Text) and cp.type.max_text_size:
            assert cp.constraint_state.max_text_size == "failed"
        else:
            assert cp.constraint_state.max_text_size is None
        if as_apply:
            data.pop("constraintState")
            assert data == cp.as_apply().dump()
        else:
            assert data == cp.dump()

    def test_dump_no_longer_camelCases_everything_when_used(self) -> None:
        cp = ContainerProperty(
            type=data_types.Enum(
                {
                    "Closed_I_think": data_types.EnumValue("Valve_is_closed"),
                    "Opened or not": data_types.EnumValue("Valve is opened"),
                }
            ),
            constraint_state=PropertyConstraintState(),
        )
        assert ContainerProperty._load(cp.dump()) == cp
        assert sorted(cp.dump(camel_case=True)["type"]["values"]) == ["Closed_I_think", "Opened or not"]


class TestConstraint:
    @pytest.mark.parametrize("constraint_class", [Constraint, ConstraintApply])
    @pytest.mark.parametrize(
        "base_data",
        [
            {
                "require": {"type": "container", "space": "mySpace", "externalId": "myExternalId"},
                "constraintType": "requires",
            },
            {"properties": ["name", "fullName"], "constraintType": "uniqueness"},
        ],
    )
    def test_load_dump(self, constraint_class: type[ConstraintCore], base_data: dict) -> None:
        # Add state field only for read version (Constraint), not write version (ConstraintApply)
        data = base_data.copy()
        if constraint_class == Constraint:
            data["state"] = "current"

        actual = constraint_class.load(data).dump(camel_case=True)
        assert data == actual

    def test_load_unknown_type(self) -> None:
        data = {"someUnknownConstraint": {"wawa": "wiwa"}, "constraintType": "unknown", "state": "current"}
        obj = Constraint.load(data)
        assert isinstance(obj, UnknownCogniteResource)
        assert obj.dump() == data

    @pytest.mark.parametrize("as_apply", [False, True])
    @pytest.mark.parametrize(
        "data",
        [
            {
                "require": {"type": "container", "space": "mySpace", "externalId": "myExternalId"},
                "constraintType": "requires",
                "state": "current",
            },
            {"properties": ["name", "fullName"], "constraintType": "uniqueness", "state": "failed"},
        ],
    )
    def test_load_dump_state(self, data: dict, as_apply: bool) -> None:
        constraint = Constraint.load(data)
        assert constraint.state is not None
        assert constraint.state == data["state"]
        if as_apply:
            data.pop("state")
            assert data == constraint.as_apply().dump()
        else:
            assert data == constraint.dump()


class TestIndex:
    @pytest.mark.parametrize("index_class", [Index, IndexApply])
    @pytest.mark.parametrize(
        "base_data",
        [
            {"properties": ["name", "fullName"], "indexType": "btree", "cursorable": True},
            {"properties": ["name", "fullName"], "indexType": "inverted"},
        ],
    )
    def test_load_dump(self, index_class: type[IndexCore], base_data: dict) -> None:
        # Add state field only for read version (Index), not write version (IndexApply)
        data = base_data.copy()
        if index_class == Index:
            data["state"] = "current"

        actual = index_class.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize("index_class", [Index, IndexApply])
    @pytest.mark.parametrize(
        "base_data",
        [
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "indexType": "btree", "cursorable": True},
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "indexType": "inverted"},
        ],
    )
    def test_load_dump__no_fail_on_unseen_key(self, index_class: type[IndexCore], base_data: dict) -> None:
        # Add state field only for read version (Index), not write version (IndexApply)
        data = base_data.copy()
        if index_class == Index:
            data["state"] = "current"

        actual = index_class.load(data).dump(camel_case=True)
        data.pop("this-key-is-new-sooo-new")
        assert data == actual

    @pytest.mark.parametrize("as_apply", [False, True])
    @pytest.mark.parametrize(
        "data",
        [
            {"properties": ["name", "fullName"], "indexType": "btree", "cursorable": True, "state": "current"},
            {"properties": ["name", "fullName"], "indexType": "inverted", "state": "pending"},
        ],
    )
    def test_load_dump_state(self, data: dict, as_apply: bool) -> None:
        index = Index.load(data)
        assert index.state is not None
        assert index.state == data["state"]
        if as_apply:
            data.pop("state")
            assert data == index.as_apply().dump()
        else:
            assert data == index.dump()

    @pytest.mark.parametrize("index_class", [Index, IndexApply])
    @pytest.mark.parametrize(
        "base_data", [{"properties": ["name"], "indexType": "btree"}, {"properties": ["name"], "indexType": "inverted"}]
    )
    def test_load_dump__only_required(self, index_class: type[IndexCore], base_data: dict) -> None:
        # Add state field only for read version (Index), not write version (IndexApply)
        data = base_data.copy()
        if index_class == Index:
            data["state"] = "current"

        # Add cursorable default for btree indexes
        if base_data["indexType"] == "btree":
            data["cursorable"] = False

        actual = index_class.load(data).dump(camel_case=True)
        assert data == actual

    def test_load_unknown_type(self) -> None:
        data = {"someUnknownIndexType": {"wawa": "wiwa"}, "indexType": "someUnknownIndexType", "state": "current"}
        obj = Index.load(data)
        assert isinstance(obj, UnknownCogniteResource)
        assert obj.dump() == data


class TestConstraints:
    @pytest.mark.parametrize("constraint_class", [Constraint, ConstraintApply])
    @pytest.mark.parametrize(
        "base_data",
        [
            {
                "require": {"type": "container", "space": "mySpace", "externalId": "myExternalId"},
                "constraintType": "requires",
            },
            {"properties": ["name", "fullName"], "constraintType": "uniqueness"},
        ],
    )
    def test_load_dump(self, constraint_class: type[ConstraintCore], base_data: dict) -> None:
        # Add state field only for read version (Constraint), not write version (ConstraintApply)
        data = base_data.copy()
        if constraint_class == Constraint:
            data["state"] = "current"

        actual = constraint_class.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize("constraint_class", [Constraint, ConstraintApply])
    @pytest.mark.parametrize(
        "base_data",
        [
            {"this-key-is-new-sooo-new": 42, "properties": ["name"], "constraintType": "uniqueness"},
            {
                "this-key-is-new-sooo-new": 42,
                "require": {"space": "hehe", "externalId": "hoho", "type": "container"},
                "constraintType": "requires",
            },
        ],
    )
    def test_load_dump__no_fail_on_unseen_key(self, constraint_class: type[ConstraintCore], base_data: dict) -> None:
        # Add state field only for read version (Constraint), not write version (ConstraintApply)
        data = base_data.copy()
        if constraint_class == Constraint:
            data["state"] = "current"

        actual = constraint_class.load(data).dump(camel_case=True)
        data.pop("this-key-is-new-sooo-new")
        assert data == actual


class TestContainerPropertyApply:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": {"type": "direct", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
            {"type": {"type": "int32", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
            {
                "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                "immutable": False,
                "autoIncrement": False,
                "nullable": True,
            },
            {"type": {"type": "file", "list": False}, "immutable": False, "autoIncrement": False, "nullable": True},
        ],
    )
    def test_load_dump__only_required(self, data: dict) -> None:
        actual = ContainerPropertyApply.load(data).dump(camel_case=True)
        assert data == actual

    def test_as_apply(self) -> None:
        prop = ContainerPropertyApply(data_types.Text())
        assert prop.as_apply() is prop
