import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.datapoints_subscriptions import DataPointSubscriptionWrite


class TestDataPointSubscription:
    def test_raises_value_error_on_invalid_filter(self):
        # Arrange
        f = filters
        nested_fileter = f.Nested(
            scope=("some", "direct_relation", "property"), filter=f.Equals(property=["node", "name"], value="ACME")
        )

        # Act
        with pytest.raises(ValueError) as e:
            DataPointSubscriptionWrite(
                external_id="MySubscription", partition_count=10, name="MySubscription", filter=nested_fileter
            )

        # Assert
        assert "Nested" in str(e.value) and "not supported" in str(e.value)
