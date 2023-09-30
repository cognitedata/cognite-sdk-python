from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.units import (
    UnitList,
)
from cognite.client.exceptions import CogniteNotFoundError


@pytest.fixture(scope="session")
def available_units(cognite_client: CogniteClient) -> UnitList:
    units = cognite_client.units.list()
    assert len(units) >= 2, "Expected to get some units"
    return units


class TestUnits:
    def test_retrieve_unit(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        unit = available_units[0]

        retrieved_unit = cognite_client.units.retrieve(unit.external_id)

        assert retrieved_unit == unit

    def test_retrieve_multiple(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        units = available_units[:2]

        retrieved_units = cognite_client.units.retrieve(units.as_external_ids())

        assert retrieved_units == units

    def test_retrieve_raise_non_existing_unit(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.units.retrieve([available_units[0].external_id, "non-existing-unit"])

    def test_retrieve_none_for_single_non_existing_unit(
        self, cognite_client: CogniteClient, available_units: UnitList
    ) -> None:
        retrieved_unit = cognite_client.units.retrieve("non-existing-unit", ignore_unknown_ids=True)
        assert retrieved_unit is None

    def test_retrieve_ignore_unknown_unit(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        unit = available_units[0]

        retrieved_units = cognite_client.units.retrieve(
            [unit.external_id, "non-existing_unit"], ignore_unknown_ids=True
        )

        assert len(retrieved_units) == 1
        assert retrieved_units[0] == unit

    def test_list_unit_systems(self, cognite_client: CogniteClient) -> None:
        unit_systems = cognite_client.units.systems.list()

        assert len(unit_systems) >= 1, "Expected to get some unit systems"
