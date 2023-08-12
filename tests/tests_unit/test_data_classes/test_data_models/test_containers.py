import pytest

from cognite.client.data_classes.data_modeling.containers import Constraint, Index


class TestConstraintIdentifier:
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


class TestIndexIdentifier:
    @pytest.mark.parametrize("data", [{"properties": ["name", "fullName"], "indexType": "btree", "cursorable": True}])
    def test_load_dump(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=True)
        assert data == actual

    @pytest.mark.parametrize("data", [{"properties": ["name"]}])
    def test_load_dump__default_values_are_used(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=False)
        data.update(index_type="btree", cursorable=False)
        assert data == actual

    @pytest.mark.parametrize(
        "data", [{"this-key-is-new-sooo-new": 42, "properties": ["name"], "indexType": "best-tree", "cursorable": True}]
    )
    def test_load_dump__no_fail_on_unseen_key(self, data: dict) -> None:
        actual = Index.load(data).dump(camel_case=True)
        data.pop("this-key-is-new-sooo-new")
        assert data == actual
