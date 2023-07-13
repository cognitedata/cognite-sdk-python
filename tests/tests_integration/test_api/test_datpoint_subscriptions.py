from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DatapointSubscription, DataPointSubscriptionCreate


@pytest.fixture
def time_series_external_ids(cognite_client: CogniteClient):
    external_ids = [f"PYSDK DataPoint Subscription Test {no}" for no in range(10)]
    existing = cognite_client.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
    assert len(existing) == len(external_ids), (
        "The 10 timeseries used for testing datapoint " "subscriptions must exist in the test environment"
    )
    return external_ids


class TestDatapointSubscriptions:
    def test_list_subscriptions(self, cognite_client: CogniteClient):
        subscriptions = cognite_client.time_series.subscriptions.list(limit=5)

        assert len(subscriptions) > 0, "Add at least one subscription to the test environment to run this test"

    def test_create_retrieve_delete_subscription(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionCreateRetrieveDeleteTest",
            name="PYSDKDataPointSubscriptionCreateRetrieveDeleteTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )
        created_subscription: DatapointSubscription | None = None

        # Act
        try:
            created_subscription = cognite_client.time_series.subscriptions.create(new_subscription)
            retrieved_subscription = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)

            # Assert
            assert created_subscription.created_time
            assert created_subscription.last_updated_time
            assert created_subscription.time_series_count == len(new_subscription.time_series_ids)
            assert (
                retrieved_subscription.external_id == new_subscription.external_id == created_subscription.external_id
            )

            # Act
            cognite_client.time_series.subscriptions.delete(new_subscription.external_id)
            retrieved_deleted = cognite_client.time_series.subscriptions.retrieve(
                new_subscription.external_id, ignore_unknown_ids=True
            )

            # Assert
            assert retrieved_deleted is None
        finally:
            if created_subscription:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)
