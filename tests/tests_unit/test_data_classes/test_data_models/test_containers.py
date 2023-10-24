import pytest

from cognite.client.data_classes.data_modeling.containers import Constraint, ContainerProperty, Index


class TestContainerProperty:
    @pytest.mark.parametrize(
        "data",
        [
            {"type": {"type": "direct"}},
            # List is not required, but gets set and thus will be dumped
            {"type": {"type": "int32", "list": False}},
            {"type": {"type": "text", "list": False, "collation": "ucs_basic"}},
            {"type": {"type": "file", "list": False}},
        ],
    )
    def test_load_dump__only_required(self, data: dict) -> None:
        actual = ContainerProperty.load(data).dump(camel_case=True)
        assert data == actual


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
