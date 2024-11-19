from __future__ import annotations

import random
import re

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
        unit = random.choice(available_units)

        retrieved_unit = cognite_client.units.retrieve(unit.external_id)

        assert retrieved_unit == unit

    def test_retrieve_multiple(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        unit_xids = random.choices(available_units.as_external_ids(), k=3)

        retrieved_units = cognite_client.units.retrieve(unit_xids)

        assert retrieved_units.as_external_ids() == unit_xids

    def test_retrieve_raise_non_existing_unit(self, cognite_client: CogniteClient, available_units: UnitList) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.units.retrieve([available_units[0].external_id, "non-existing-unit"])

    def test_retrieve_none_for_single_non_existing_unit(self, cognite_client: CogniteClient) -> None:
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


class TestFromAlias:
    @pytest.mark.parametrize(
        "alias, quantity, expected_xid",
        (
            ("cubic decimetre per minute", None, "volume_flow_rate:decim3-per-min"),
            ("cubic decimetre per minute", "Volume Flow Rate", "volume_flow_rate:decim3-per-min"),
            ("cubic decimetre per minute", "volume FLOW rate", "volume_flow_rate:decim3-per-min"),
            ("megavolt ampere hr", None, "energy:megav-a-hr"),
            ("megavolt ampere hr", "Energy", "energy:megav-a-hr"),
            ("megavolt ampere hr", "enerGY", "energy:megav-a-hr"),
        ),
    )
    def test_lookup_unit_from_alias(self, cognite_client, alias, quantity, expected_xid):
        unit = cognite_client.units.from_alias(alias, quantity)
        assert unit.external_id == expected_xid

    def test_lookup_unit_from_alias_unknown_quantity(self, cognite_client):
        match = re.escape("Unknown quantity='Not Energy'. Did you mean one of: ['Energy']? All known quantities: [")
        with pytest.raises(ValueError, match=match):
            cognite_client.units.from_alias("cubic decimetre per minute", "Not Energy")

    def test_lookup_ambiguous(self, cognite_client):
        match = re.escape("Ambiguous alias, matches all of: ['capacitance:farad', 'temperature:deg_f']")
        with pytest.raises(ValueError, match=match):
            cognite_client.units.from_alias("F", None)
        with pytest.raises(ValueError, match=match):
            cognite_client.units.from_alias("F", None, return_ambiguous=False)

        units = cognite_client.units.from_alias("F", None, return_ambiguous=True)
        assert units.as_external_ids() == ["capacitance:farad", "temperature:deg_f"]

    def test_lookup_closest_match__only_alias(self, cognite_client):
        # Ensure it fails without 'closest matches':
        match = re.escape("Unknown alias='c mol', did you mean one of: ['")
        with pytest.raises(ValueError, match=match):
            cognite_client.units.from_alias("c mol", return_closest_matches=False)

        units = cognite_client.units.from_alias("c mol", return_closest_matches=True)
        assert len(units) > 1
        assert "amount_of_substance:centimol" in units.as_external_ids()

    def test_lookup_closest_match_alias_and_quantity(self, cognite_client):
        # Ensure it fails without 'closest matches':
        match = re.escape("Unknown alias='imp force' for known quantity='Force'. Did you mean one of: ['")
        with pytest.raises(ValueError, match=match):
            cognite_client.units.from_alias("imp force", "Force", return_closest_matches=False)

        units = cognite_client.units.from_alias("imp force", "Force", return_closest_matches=True)
        assert len(units) > 1
        assert "force:lb_f" in units.as_external_ids()
