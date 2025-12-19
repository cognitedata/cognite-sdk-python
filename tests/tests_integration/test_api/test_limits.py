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
        """Test listing all limit values."""
        res = cognite_client.limits.list(limit=10)
        assert isinstance(res, LimitValueList)
        assert len(res) > 0
        assert all(isinstance(item, LimitValue) for item in res)
        for limit_value in res:
            assert limit_value.limit_id is not None
            assert limit_value.value is not None
            assert isinstance(limit_value.value, (int, float))

    def test_list_with_cursor(self, cognite_client: CogniteClient) -> None:
        """Test listing with cursor for pagination."""
        first_page = cognite_client.limits.list(limit=5)
        if first_page.next_cursor:
            second_page = cognite_client.limits.list(limit=5, cursor=first_page.next_cursor)
            assert isinstance(second_page, LimitValueList)

    def test_list_advanced_with_prefix_filter(self, cognite_client: CogniteClient, post_spy: AsyncMock) -> None:
        """Test advanced list with prefix filter for a specific service."""
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=100)

        assert isinstance(res, LimitValueList)
        for limit_value in res:
            assert limit_value.limit_id is not None
            assert limit_value.limit_id.startswith("atlas.")
        assert post_spy.call_count == 1

    def test_list_advanced_without_filter(self, cognite_client: CogniteClient) -> None:
        """Test advanced list without filter (should return all limits)."""
        res = cognite_client.limits.list_advanced(limit=10)
        assert isinstance(res, LimitValueList)
        assert len(res) > 0

    def test_retrieve_existing_limit(self, cognite_client: CogniteClient, get_spy: AsyncMock) -> None:
        """Test retrieving a specific limit by ID."""
        limits = cognite_client.limits.list(limit=1)
        if len(limits) > 0 and limits[0].limit_id is not None:
            limit_id = limits[0].limit_id
            res = cognite_client.limits.retrieve(limit_id=limit_id)
            assert isinstance(res, LimitValue)
            assert res.limit_id == limit_id
            assert res.value == limits[0].value
            assert get_spy.call_count == 2

    def test_retrieve_non_existing_limit(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existent limit."""
        with pytest.raises((CogniteNotFoundError, CogniteAPIError)):
            cognite_client.limits.retrieve(limit_id="nonexistent.limit.id")

    def test_retrieve_non_existing_limit_ignore_unknown(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existent limit with ignore_unknown_ids=True."""
        res = cognite_client.limits.retrieve(limit_id="nonexistent.limit.id", ignore_unknown_ids=True)
        assert res is None

    def test_call_iterator(self, cognite_client: CogniteClient, get_spy: AsyncMock) -> None:
        """Test the iterator pattern."""
        results = list(cognite_client.limits(limit=5))
        assert len(results) <= 5
        assert all(isinstance(item, LimitValue) for item in results)
        assert get_spy.call_count >= 1

    def test_call_iterator_with_chunk_size(self, cognite_client: CogniteClient) -> None:
        """Test iterator with chunk size."""
        results = list(cognite_client.limits(chunk_size=2, limit=5))
        assert len(results) > 0
        assert all(isinstance(item, LimitValueList) for item in results)

    def test_list_vs_list_advanced_consistency(self, cognite_client: CogniteClient) -> None:
        """Test that list and list_advanced return consistent results when no filter is applied."""
        list_res = cognite_client.limits.list(limit=10)
        advanced_res = cognite_client.limits.list_advanced(limit=10)

        assert isinstance(list_res, LimitValueList)
        assert isinstance(advanced_res, LimitValueList)
        assert len(list_res) > 0
        assert len(advanced_res) > 0

        list_ids = {limit.limit_id for limit in list_res}
        advanced_ids = {limit.limit_id for limit in advanced_res}
        assert len(list_ids.intersection(advanced_ids)) > 0

    def test_filter_different_services(self, cognite_client: CogniteClient) -> None:
        """Test filtering for different service prefixes."""
        atlas_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="atlas."))
        atlas_limits = cognite_client.limits.list_advanced(filter=atlas_filter, limit=100)

        files_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="files."))
        files_limits = cognite_client.limits.list_advanced(filter=files_filter, limit=100)

        atlas_ids = {limit.limit_id for limit in atlas_limits}
        files_ids = {limit.limit_id for limit in files_limits}

        assert len(atlas_ids.intersection(files_ids)) == 0

        for limit in atlas_limits:
            assert limit.limit_id is not None
            assert limit.limit_id.startswith("atlas.")

        for limit in files_limits:
            assert limit.limit_id is not None
            assert limit.limit_id.startswith("files.")
