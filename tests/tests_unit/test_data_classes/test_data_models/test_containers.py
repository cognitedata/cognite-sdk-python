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
    def test_load_dump(self, data: dict):
        actual = Constraint.load(data).dump(camel_case=True)
        assert data == actual


class TestIndexIdentifier:
    @pytest.mark.parametrize("data", [{"properties": ["name", "fullName"], "indexType": "btree"}])
    def test_load_dump(self, data: dict):
        actual = Index.load(data).dump(camel_case=True)
        assert data == actual
