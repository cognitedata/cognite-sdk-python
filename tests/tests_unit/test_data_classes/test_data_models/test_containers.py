import pytest

from cognite.client.data_classes.data_modeling.containers import ConstraintIdentifier, IndexIdentifier


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
        actual = ConstraintIdentifier.load(data).dump(camel_case=True)
        assert data == actual


class TestIndexIdentifier:
    @pytest.mark.parametrize("data", [{"properties": ["name", "fullName"], "indexType": "btree"}])
    def test_load_dump(self, data: dict):
        actual = IndexIdentifier.load(data).dump(camel_case=True)
        assert data == actual
