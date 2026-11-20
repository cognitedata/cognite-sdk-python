from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Limit, LimitList


class TestLimitsAPI:
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.limits.list()

        assert isinstance(res, LimitList)
        assert all(limit.limit_id is not None for limit in res)
        assert all(limit.value is not None for limit in res)

    def test_retrieve_single(self, cognite_client: CogniteClient) -> None:
        limits = cognite_client.limits.list()
        if len(limits) == 0:
            pytest.skip("No limits found in project - skipping retrieve test")

        limit_id = limits[0].as_id()
        res = cognite_client.limits.retrieve(id=limit_id)

        assert res is not None, f"Failed to retrieve limit with id={limit_id}"
        assert isinstance(res, Limit)
        assert res.limit_id == limit_id
        assert res.value == limits[0].value

    def test_retrieve_single_not_found(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.limits.retrieve(id="nonexistent.limit.id")
        assert res is None
