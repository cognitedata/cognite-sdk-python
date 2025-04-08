import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.datapoints_subscriptions import DatapointSubscription, DataPointSubscriptionWrite


class TestDataPointSubscription:
    def test_raises_value_error_on_invalid_filter(self):
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
