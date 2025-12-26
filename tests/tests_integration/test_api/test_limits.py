from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, patch

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import LimitValue, LimitValueFilter, LimitValueList, LimitValuePrefixFilter
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


@pytest.fixture
def post_spy(async_client: AsyncCogniteClient) -> Iterator[AsyncMock]:
    with patch.object(async_client.limits, "_post", wraps=async_client.limits._post) as post_mock:
        yield post_mock


@pytest.fixture
def get_spy(async_client: AsyncCogniteClient) -> Iterator[AsyncMock]:
    with patch.object(async_client.limits, "_get", wraps=async_client.limits._get) as get_mock:
        yield get_mock


class TestLimitsAPI:
    def test_list(self, cognite_client: CogniteClient, post_spy: AsyncMock) -> None:
        res = cognite_client.limits.list(limit=10)
        assert isinstance(res, LimitValueList)
        assert len(res) > 0
        assert all(isinstance(item, LimitValue) for item in res)
        assert all(limit_value.limit_id and isinstance(limit_value.value, (int, float)) for limit_value in res)

    def test_list_advanced_with_filter(self, cognite_client: CogniteClient, post_spy: AsyncMock) -> None:
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=100)

        assert isinstance(res, LimitValueList)
        assert all(limit_value.limit_id and limit_value.limit_id.startswith("atlas.") for limit_value in res)
        assert post_spy.call_count == 1

    def test_call_iterator(self, cognite_client: CogniteClient, get_spy: AsyncMock) -> None:
        results = list(cognite_client.limits(limit=5))
        assert len(results) <= 5
        assert all(isinstance(item, LimitValue) for item in results)
        assert get_spy.call_count >= 1

    def test_filter_different_services(self, cognite_client: CogniteClient) -> None:
        atlas_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="atlas."))
        files_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="files."))

        atlas_limits = cognite_client.limits.list_advanced(filter=atlas_filter, limit=100)
        files_limits = cognite_client.limits.list_advanced(filter=files_filter, limit=100)

        atlas_ids = {limit.limit_id for limit in atlas_limits}
        files_ids = {limit.limit_id for limit in files_limits}

        assert len(atlas_ids.intersection(files_ids)) == 0
        assert all(limit.limit_id and limit.limit_id.startswith("atlas.") for limit in atlas_limits)
        assert all(limit.limit_id and limit.limit_id.startswith("files.") for limit in files_limits)

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
