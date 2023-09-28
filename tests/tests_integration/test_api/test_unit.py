from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client.data_classes.unit import (
    UnitList,
)


class TestUnits:
    def test_list_units(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.units.list(limit=5)
        assert isinstance(res, UnitList)
        assert len(res) > 0
