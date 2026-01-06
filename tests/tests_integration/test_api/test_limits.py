from __future__ import annotations

from cognite.client import CogniteClient


class TestLimitsAPI:
    def test_retrieve_single_not_found(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.limits.retrieve(id="nonexistent.limit.id")
        assert res is None
