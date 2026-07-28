from __future__ import annotations

import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DataPointSubscriptionWrite,
    TimeSeriesID,
    TimeSeriesIDList,
)


class TestDataPointSubscription:
    def test_raises_value_error_on_invalid_filter(self) -> None:
        f = filters
        nested_filter = f.Nested(
            scope=("some", "direct_relation", "property"), filter=f.Equals(property=["node", "name"], value="ACME")
        )
        with pytest.raises(ValueError) as e:
            DataPointSubscriptionWrite(
                external_id="MySubscription", partition_count=10, name="MySubscription", filter=nested_filter
            )
        assert "Nested" in str(e.value) and "not supported" in str(e.value)

    def test_handles_null_timeseries_count(self) -> None:
        sub = DatapointSubscription.load(
            {
                "externalId": "MySubscription",
                "partitionCount": 10,
                "name": "MySubscription",
                "createdTime": 123,
                "lastUpdatedTime": 456,
            }
        )
        assert sub.time_series_count is None


class TestTimeSeriesID:
    def test_load_with_all_fields(self) -> None:
        ts_id = TimeSeriesID._load({"id": 123, "externalId": "my_ts"})
        assert ts_id.id == 123
        assert ts_id.external_id == "my_ts"
        assert ts_id.instance_id is None
        assert ts_id.is_resolved is True

    def test_load_with_missing_id_broken_reference(self) -> None:
        """Test that TimeSeriesID can be loaded when 'id' is missing (broken reference scenario).

        This happens when a time series's external_id is changed or the time series is deleted,
        but the subscription still references the old external_id.
        """
        ts_id = TimeSeriesID._load({"externalId": "deleted_or_renamed_ts"})
        assert ts_id.id is None
        assert ts_id.external_id == "deleted_or_renamed_ts"
        assert ts_id.instance_id is None
        assert ts_id.is_resolved is False

    def test_load_with_instance_id_only(self) -> None:
        """Test loading a TimeSeriesID with only an instance_id (broken reference)."""
        ts_id = TimeSeriesID._load({"instanceId": {"space": "my_space", "externalId": "my_node"}})
        assert ts_id.id is None
        assert ts_id.external_id is None
        assert ts_id.instance_id is not None
        assert ts_id.instance_id.space == "my_space"
        assert ts_id.instance_id.external_id == "my_node"
        assert ts_id.is_resolved is False

    def test_repr_with_id(self) -> None:
        ts_id = TimeSeriesID(id=123, external_id="my_ts")
        assert repr(ts_id) == "TimeSeriesID(id=123, external_id=my_ts)"

    def test_repr_without_id(self) -> None:
        ts_id = TimeSeriesID(external_id="broken_ref")
        assert repr(ts_id) == "TimeSeriesID(external_id=broken_ref)"

    def test_dump_with_id(self) -> None:
        ts_id = TimeSeriesID(id=123, external_id="my_ts")
        dumped = ts_id.dump()
        assert dumped == {"id": 123, "externalId": "my_ts"}

    def test_dump_without_id(self) -> None:
        """Test that dump excludes 'id' when it's None."""
        ts_id = TimeSeriesID(external_id="broken_ref")
        dumped = ts_id.dump()
        assert dumped == {"externalId": "broken_ref"}
        assert "id" not in dumped

    def test_time_series_id_list_with_broken_references(self) -> None:
        """Test that TimeSeriesIDList can handle a mix of resolved and broken references."""
        items: list[dict] = [
            {"id": 123, "externalId": "ts_1"},
            {"externalId": "broken_ref"},  # No id - broken reference
            {"id": 456, "externalId": "ts_2"},
        ]
        ts_list = TimeSeriesIDList._load(items)
        assert len(ts_list) == 3
        assert ts_list[0].is_resolved is True
        assert ts_list[1].is_resolved is False
        assert ts_list[2].is_resolved is True
