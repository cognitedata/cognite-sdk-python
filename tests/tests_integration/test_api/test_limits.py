from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, patch

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import LimitValue
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


@pytest.fixture
def get_spy(async_client: AsyncCogniteClient) -> Iterator[AsyncMock]:
    with patch.object(async_client.limits, "_get", wraps=async_client.limits._get) as get_mock:
        yield get_mock


class TestLimitsAPI:
    def test_retrieve_existing_limit(self, cognite_client: CogniteClient, get_spy: AsyncMock) -> None:
        """Test retrieving a specific limit by ID."""
        limit_id = "streams.streams"
        res = cognite_client.limits.retrieve(limit_id=limit_id)
        assert isinstance(res, LimitValue)
        assert res.limit_id == limit_id
        assert res.value is not None
        assert isinstance(res.value, (int, float))
        assert get_spy.call_count == 1

    def test_retrieve_non_existing_limit(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existent limit."""
        with pytest.raises((CogniteNotFoundError, CogniteAPIError)):
            cognite_client.limits.retrieve(limit_id="nonexistent.limit.id")

    def test_retrieve_non_existing_limit_ignore_unknown(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existent limit with ignore_unknown_ids=True."""
        res = cognite_client.limits.retrieve(limit_id="nonexistent.limit.id", ignore_unknown_ids=True)
        assert res is None
