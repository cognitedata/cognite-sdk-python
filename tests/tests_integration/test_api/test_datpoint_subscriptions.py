from __future__ import annotations

import time

import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries, filters
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DataPointSubscriptionCreate,
    DataPointSubscriptionFilterProperties,
    DataPointSubscriptionUpdate,
)


@pytest.fixture(scope="session")
def time_series_external_ids(cognite_client: CogniteClient) -> list[str]:
    external_ids = [f"PYSDK DataPoint Subscription Test {no}" for no in range(10)]
    existing = cognite_client.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
    assert len(existing) == len(external_ids), (
        "The 10 timeseries used for testing datapoint " "subscriptions must exist in the test environment"
    )
    return external_ids


@pytest.fixture(scope="session")
def subscription_one_timeseries(cognite_client: CogniteClient) -> DatapointSubscription:
    sub1 = cognite_client.time_series.subscriptions.retrieve("PYSDKDataPointSubscriptionTest1", ignore_unknown_ids=True)
    assert (
        sub1 is not None
    ), "The subscription used for testing datapoint subscriptions must exist in the test environment"
    return sub1


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

    def test_update_subscription(self, cognite_client: CogniteClient, time_series_external_ids: list[str]):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionUpdateTest",
            name="PYSDKDataPointSubscriptionUpdateTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )
        created: DatapointSubscription | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            update = (
                DataPointSubscriptionUpdate(new_subscription.external_id)
                .name.set("New Name")
                .time_series_ids.remove([time_series_external_ids[0]])
            )

            # Act
            updated = cognite_client.time_series.subscriptions.update(update)

            # Assert
            assert updated.name == "New Name"
            assert updated.time_series_count == len(time_series_external_ids) - 1
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_update_filter_defined_subscription(self, cognite_client: CogniteClient):
        # Arrange
        f = filters
        p = DataPointSubscriptionFilterProperties
        numerical_timeseries = f.And(
            f.Equals(p.is_string, False),
        )

        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKFilterDefinedDataPointSubscriptionUpdateTest",
            name="PYSDKFilterDefinedDataPointSubscriptionUpdateTest",
            filter=numerical_timeseries,
            partition_count=1,
        )
        created: DatapointSubscription | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            new_filter = f.And(
                f.Equals(p.is_string, False), f.Prefix(p.external_id, "PYSDK DataPoint Subscription Test")
            )
            new_update = DataPointSubscriptionUpdate(new_subscription.external_id).filter.set(new_filter)

            # Act
            _ = cognite_client.time_series.subscriptions.update(new_update)
            retrieved = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)

            # Assert
            assert retrieved.filter.dump() == new_filter.dump()
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_list_data_subscription_no_change(
        self, cognite_client: CogniteClient, subscription_one_timeseries: DatapointSubscription
    ):
        # Act
        batch = cognite_client.time_series.subscriptions.list_data(subscription_one_timeseries.external_id, [0])

        # Assert
        assert batch.has_next is False
        assert batch.partitions[0].cursor is not None

    def test_list_data_subscription_changed_time_series(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            name="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            time_series_ids=[time_series_external_ids[0]],
            partition_count=1,
        )
        created: DatapointSubscription | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            # Act
            first_batch = cognite_client.time_series.subscriptions.list_data(new_subscription.external_id, [0])

            # Assert
            assert first_batch.has_next is False
            assert first_batch.partitions[0].cursor is not None

            # Arrange
            update = (
                DataPointSubscriptionUpdate(new_subscription.external_id)
                .time_series_ids.add([time_series_external_ids[1]])
                .time_series_ids.remove([time_series_external_ids[0]])
            )

            # Act
            cognite_client.time_series.subscriptions.update(update)
            second_batch = cognite_client.time_series.subscriptions.list_data(
                new_subscription.external_id, first_batch.partitions
            )

            # Assert
            assert second_batch.subscription_changes
            subscription_changes = second_batch.subscription_changes
            assert {a.external_id for a in subscription_changes.added} == {time_series_external_ids[1]}
            assert {a.external_id for a in subscription_changes.removed} == {time_series_external_ids[0]}
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_list_data_subscription_datapoints_added(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            name="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            time_series_ids=[time_series_external_ids[0]],
            partition_count=1,
        )
        created: DatapointSubscription | None = None
        new_data: pd.DataFrame | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            # Act
            first_batch = cognite_client.time_series.subscriptions.list_data(new_subscription.external_id, [0])

            # Assert
            assert first_batch.has_next is False
            assert first_batch.partitions[0].cursor is not None

            # Arrange
            existing_data = cognite_client.time_series.data.retrieve_dataframe(external_id=time_series_external_ids[0])
            new_data = pd.DataFrame(
                index=pd.date_range(start=existing_data.index[-1] + pd.Timedelta("1d"), periods=2, freq="1d"),
                data=[[42], [43]],
                columns=[time_series_external_ids[0]],
            )

            # Act
            cognite_client.time_series.data.insert_dataframe(new_data)
            second_batch = cognite_client.time_series.subscriptions.list_data(
                new_subscription.external_id, first_batch.partitions
            )

            # Assert
            assert second_batch.updates
            assert (
                sum(
                    abs(actual.value - expected)
                    for actual, expected in zip(
                        second_batch.updates[0].upserts, new_data[time_series_external_ids[0]].values
                    )
                )
                < 1e-6
            )
            assert all(
                (actual.timestamp == expected)
                for actual, expected in zip(second_batch.updates[0].upserts, new_data.index.astype("int64") // 10**6)
            )
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)
            if new_data is not None:
                cognite_client.time_series.data.delete_range(
                    new_data.index[0], new_data.index[-1] + pd.Timedelta("1d"), external_id=time_series_external_ids[0]
                )

    @pytest.mark.skip(reason="This test is flaky")
    def test_update_filter_subscription_added_times_series(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        f = filters
        p = DataPointSubscriptionFilterProperties
        numerical_timeseries = f.And(
            f.Equals(p.is_string, False), f.Prefix(p.external_id, "PYSDK DataPoint Subscription Test")
        )

        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionUpdateFilterTest",
            name="PYSDKDataPointSubscriptionUpdateFilterTest",
            filter=numerical_timeseries,
            partition_count=1,
        )

        created: DatapointSubscription | None = None
        created_timeseries: TimeSeries | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            # Act
            first_batch = cognite_client.time_series.subscriptions.list_data(new_subscription.external_id, [0])

            # Assert
            assert first_batch.has_next is False
            assert first_batch.partitions[0].cursor is not None

            # Arrange
            new_numerical_timeseries = TimeSeries(
                external_id="PYSDK DataPoint Subscription Test 42",
                name="PYSDK DataPoint Subscription Test 42",
                is_string=False,
            )
            created_timeseries = cognite_client.time_series.create(new_numerical_timeseries)
            cognite_client.time_series.data.insert_dataframe(
                pd.DataFrame(index=[pd.Timestamp.now()], data=[[42]], columns=[new_numerical_timeseries.external_id])
            )
            # Ensure that the subscription has been updated
            time.sleep(2)

            # Act
            second_batch = cognite_client.time_series.subscriptions.list_data(
                new_subscription.external_id, first_batch.partitions
            )

            # Assert
            assert second_batch.subscription_changes
            subscription_changes = second_batch.subscription_changes
            assert {a.external_id for a in subscription_changes.added} == {new_numerical_timeseries.external_id}
            assert {a.external_id for a in subscription_changes.removed} == set()
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)
            if created_timeseries:
                cognite_client.time_series.delete(created_timeseries.id)
