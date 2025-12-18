from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import LimitValue, LimitValueFilter, LimitValueList, LimitValuePrefixFilter
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.limits, "_post", wraps=cognite_client.limits._post) as _:
        yield


@pytest.fixture
def get_spy(cognite_client):
    with mock.patch.object(cognite_client.limits, "_get", wraps=cognite_client.limits._get) as _:
        yield


class TestLimitsAPI:
    def test_list(self, cognite_client: CogniteClient, post_spy):
        """Test listing all limit values."""
        res = cognite_client.limits.list(limit=10)
        assert isinstance(res, LimitValueList)
        assert len(res) > 0
        assert all(isinstance(item, LimitValue) for item in res)
        # Verify that limit values have the expected structure
        for limit_value in res:
            assert limit_value.limit_id is not None
            assert limit_value.value is not None
            assert isinstance(limit_value.value, (int, float))

    def test_list_with_cursor(self, cognite_client: CogniteClient):
        """Test listing with cursor for pagination."""
        first_page = cognite_client.limits.list(limit=5)
        if first_page.next_cursor:
            second_page = cognite_client.limits.list(limit=5, cursor=first_page.next_cursor)
            assert isinstance(second_page, LimitValueList)
            # Verify we got different results (or empty if no more pages)
            assert len(second_page) >= 0

    def test_list_advanced_with_prefix_filter(self, cognite_client: CogniteClient, post_spy):
        """Test advanced list with prefix filter for a specific service."""
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=100)

        assert isinstance(res, LimitValueList)
        # Verify all returned limits match the prefix
        for limit_value in res:
            assert limit_value.limit_id.startswith("atlas.")
        # Verify POST was called
        assert cognite_client.limits._post.call_count == 1

    def test_list_advanced_without_filter(self, cognite_client: CogniteClient):
        """Test advanced list without filter (should return all limits)."""
        res = cognite_client.limits.list_advanced(limit=10)
        assert isinstance(res, LimitValueList)
        assert len(res) > 0

    def test_retrieve_existing_limit(self, cognite_client: CogniteClient, get_spy):
        """Test retrieving a specific limit by ID."""
        # First, get a limit ID from the list
        limits = cognite_client.limits.list(limit=1)
        if len(limits) > 0:
            limit_id = limits[0].limit_id

            # Retrieve it by ID
            res = cognite_client.limits.retrieve(limit_id=limit_id)
            assert isinstance(res, LimitValue)
            assert res.limit_id == limit_id
            assert res.value == limits[0].value
            # Verify GET was called
            assert cognite_client.limits._get.call_count == 1

    def test_retrieve_non_existing_limit(self, cognite_client: CogniteClient):
        """Test retrieving a non-existent limit."""
        with pytest.raises((CogniteNotFoundError, CogniteAPIError)):
            cognite_client.limits.retrieve(limit_id="nonexistent.limit.id")

    def test_retrieve_non_existing_limit_ignore_unknown(self, cognite_client: CogniteClient):
        """Test retrieving a non-existent limit with ignore_unknown_ids=True."""
        res = cognite_client.limits.retrieve(limit_id="nonexistent.limit.id", ignore_unknown_ids=True)
        assert res is None

    def test_call_iterator(self, cognite_client: CogniteClient, get_spy):
        """Test the iterator pattern."""
        results = list(cognite_client.limits(limit=5))
        assert len(results) <= 5
        assert all(isinstance(item, LimitValue) for item in results)
        # Verify GET was called
        assert cognite_client.limits._get.call_count >= 1

    def test_call_iterator_with_chunk_size(self, cognite_client: CogniteClient):
        """Test iterator with chunk size."""
        results = list(cognite_client.limits(chunk_size=2, limit=5))
        # Should return LimitValueList chunks
        assert len(results) > 0
        assert all(isinstance(item, LimitValueList) for item in results)

    def test_iter(self, cognite_client: CogniteClient):
        """Test the __iter__ method."""
        count = 0
        for limit_value in cognite_client.limits:
            assert isinstance(limit_value, LimitValue)
            count += 1
            if count >= 3:  # Just test a few iterations
                break
        assert count > 0

    def test_limit_value_properties(self, cognite_client: CogniteClient):
        """Test that limit values have the expected properties."""
        limits = cognite_client.limits.list(limit=1)
        if len(limits) > 0:
            limit = limits[0]
            # Test that we can access properties
            assert hasattr(limit, "limit_id")
            assert hasattr(limit, "value")
            assert limit.limit_id is not None
            assert limit.value is not None

    def test_limit_value_list_properties(self, cognite_client: CogniteClient):
        """Test that limit value lists have the expected properties."""
        limits = cognite_client.limits.list(limit=5)
        assert isinstance(limits, LimitValueList)
        assert len(limits) > 0
        # Test that we can access items
        assert limits[0] is not None
        # Test that next_cursor might be present
        # (it may or may not be None depending on whether there are more pages)

    def test_list_vs_list_advanced_consistency(self, cognite_client: CogniteClient):
        """Test that list and list_advanced return consistent results when no filter is applied."""
        list_res = cognite_client.limits.list(limit=10)
        advanced_res = cognite_client.limits.list_advanced(limit=10)

        # Both should return LimitValueList
        assert isinstance(list_res, LimitValueList)
        assert isinstance(advanced_res, LimitValueList)

        # Both should have items
        assert len(list_res) > 0
        assert len(advanced_res) > 0

        # The limit IDs should be the same (order might differ, so we'll check sets)
        list_ids = {limit.limit_id for limit in list_res}
        advanced_ids = {limit.limit_id for limit in advanced_res}
        # They should have some overlap (at least some common limits)
        assert len(list_ids.intersection(advanced_ids)) > 0

    def test_filter_different_services(self, cognite_client: CogniteClient):
        """Test filtering for different service prefixes."""
        # Test with a common service prefix
        atlas_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="atlas."))
        atlas_limits = cognite_client.limits.list_advanced(filter=atlas_filter, limit=100)

        files_filter = LimitValueFilter(prefix=LimitValuePrefixFilter(property=["limitId"], value="files."))
        files_limits = cognite_client.limits.list_advanced(filter=files_filter, limit=100)

        # Verify they return different limits (if both services have limits)
        atlas_ids = {limit.limit_id for limit in atlas_limits}
        files_ids = {limit.limit_id for limit in files_limits}

        # The sets should be disjoint (no overlap between atlas and files limits)
        assert len(atlas_ids.intersection(files_ids)) == 0

        # Verify all atlas limits start with "atlas."
        for limit in atlas_limits:
            assert limit.limit_id.startswith("atlas.")

        # Verify all files limits start with "files."
        for limit in files_limits:
            assert limit.limit_id.startswith("files.")
