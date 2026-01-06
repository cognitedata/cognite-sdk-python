from __future__ import annotations

from cognite.client import CogniteClient


class TestLimitsAPI:
    def test_retrieve_multiple_non_existing_limit(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.limits.retrieve_multiple(ids=["nonexistent.limit.id"])
        assert len(res) == 0
