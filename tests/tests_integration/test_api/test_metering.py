from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import MeteringData, MeteringDataList


class TestMeteringAPI:
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.metering.list()

        assert isinstance(res, MeteringDataList)
        assert len(res) > 0
        assert all(meter.meter_id is not None for meter in res)

    def test_retrieve_single(self, cognite_client: CogniteClient) -> None:
        meters = cognite_client.metering.list()
        if len(meters) == 0:
            pytest.skip("No meters found in project - skipping retrieve test")

        meter_id = meters[0].as_id()
        res = cognite_client.metering.retrieve(id=meter_id)

        assert res is not None, f"Failed to retrieve meter with id={meter_id}"
        assert isinstance(res, MeteringData)
        assert res.meter_id == meter_id

    def test_retrieve_single_not_found(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.metering.retrieve(id="nonexistent.meter.id")
        assert res is None

    def test_retrieve_multiple(self, cognite_client: CogniteClient) -> None:
        meters = cognite_client.metering.list(limit=2)
        if len(meters) == 0:
            pytest.skip("No meters found in project - skipping retrieve_multiple test")

        ids = meters.as_ids()
        res = cognite_client.metering.retrieve(id=ids)

        assert isinstance(res, MeteringDataList)
        assert len(res) == len(ids)
        assert [m.meter_id for m in res] == ids
