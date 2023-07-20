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
def subscription_3(cognite_client: CogniteClient) -> DatapointSubscription:
    sub3 = cognite_client.time_series.subscriptions.retrieve("PYSDKDataPointSubscriptionTest3", ignore_unknown_ids=True)
    assert (
        sub3 is not None
    ), "The subscription used for testing datapoint subscriptions must exist in the test environment"
    return sub3


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
            cognite_client.time_series.subscriptions.update(new_update)
            retrieved = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)

            # Assert
            assert retrieved.filter.dump() == new_filter.dump()
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_delete_subscription(self, cognite_client: CogniteClient, time_series_external_ids: list[str]):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionDeleteTest",
            name="PYSDKDataPointSubscriptionDeleteTest",
            time_series_ids=[time_series_external_ids[0]],
            partition_count=1,
        )

        created: DatapointSubscription | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)

            # Act
            cognite_client.time_series.subscriptions.delete(new_subscription.external_id)

            retrieved = cognite_client.time_series.subscriptions.retrieve(
                new_subscription.external_id, ignore_unknown_ids=True
            )

            # Assert
            assert retrieved is None
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_iterate_data_subscription_initial_call(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionListDataTest",
            name="PYSDKDataPointSubscriptionListDataTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )

        try:
            cognite_client.time_series.subscriptions.create(new_subscription)
            subscription = iter(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))

            # Act
            _, time_series = next(subscription)

            # Assert
            assert (
                len(time_series.added) > 0
            ), "The subscription used for testing datapoint subscriptions must have at least one time series"

            # Act
            for next_data, next_timeseries in subscription:
                # Assert
                assert len(next_timeseries.added) == 0, "There should be no more timeseries in the subsequent batches"
        finally:
            cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_iterate_data_subscription_changed_time_series(
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
            subscription = iter(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))
            data, timeseries = next(subscription)

            # Assert
            assert timeseries.added[0].external_id == time_series_external_ids[0]
            assert len(data) == 0

            # Arrange
            update = (
                DataPointSubscriptionUpdate(new_subscription.external_id)
                .time_series_ids.add([time_series_external_ids[1]])
                .time_series_ids.remove([time_series_external_ids[0]])
            )

            # Act
            cognite_client.time_series.subscriptions.update(update)
            data, timeseries = next(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))

            # Assert
            assert {a.external_id for a in timeseries.added} == {
                time_series_external_ids[1],
                time_series_external_ids[0],
            }
            assert {a.external_id for a in timeseries.removed} == {time_series_external_ids[0]}
            assert len(data) == 0
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_iterate_data_subscription_datapoints_added(
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
            assert created.created_time

            # Act
            _, timeseries = next(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))

            # Assert
            assert timeseries.added[0].external_id == time_series_external_ids[0]

            # Arrange
            existing_data = cognite_client.time_series.data.retrieve_dataframe(external_id=time_series_external_ids[0])
            new_data = pd.DataFrame(
                index=pd.date_range(start=existing_data.index[-1] + pd.Timedelta("1d"), periods=2, freq="1d"),
                data=[[42], [43]],
                columns=[time_series_external_ids[0]],
            )

            # Act
            cognite_client.time_series.data.insert_dataframe(new_data)
            retrieved_data, timeseries = next(
                cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id)
            )

            # Assert
            assert timeseries
            assert (
                sum(
                    abs(actual.value - expected)
                    for actual, expected in zip(retrieved_data[0].upserts, new_data[time_series_external_ids[0]].values)
                )
                < 1e-6
            ), "The values of the retrieved data should be the same as the inserted data"
            assert all(
                (actual.timestamp == expected)
                for actual, expected in zip(retrieved_data[0].upserts, new_data.index.astype("int64") // 10**6)
            ), "The timestamps of the retrieved data should be the same as the inserted data"
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)
            if new_data is not None:
                cognite_client.time_series.data.delete_range(
                    new_data.index[0], new_data.index[-1] + pd.Timedelta("1d"), external_id=time_series_external_ids[0]
                )

    def test_iterate_data_subscription_jump_to_end(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        # Arrange
        new_subscription = DataPointSubscriptionCreate(
            external_id="PYSDKDataPointSubscriptionJumpToEndTest",
            name="PYSDKDataPointSubscriptionJumpToEndTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )

        created: DatapointSubscription | None = None
        try:
            created = cognite_client.time_series.subscriptions.create(new_subscription)
            assert created.created_time

            # Act
            for changed_data, timeseries in cognite_client.time_series.subscriptions.iterate_data(
                new_subscription.external_id, start="now"
            ):
                # Assert
                assert len(changed_data) == 0
                assert len(timeseries.added) == 0
                assert len(timeseries.removed) == 0

        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)

    def test_iterate_data_subscription_start_1m_ago(
        self, cognite_client: CogniteClient, subscription_3: DatapointSubscription
    ):
        # Arrange
        added_count = 0
        for changed_data, timeseries in cognite_client.time_series.subscriptions.iterate_data(
            subscription_3.external_id
        ):
            added_count += len(timeseries.added)
        assert added_count > 0, "There should be at least one timeseries added"

        # Act
        added_last_minute = 0
        for changed_data, timeseries in cognite_client.time_series.subscriptions.iterate_data(
            subscription_3.external_id, start="1m-ago"
        ):
            added_last_minute += len(timeseries.added)

        # Assert
        assert added_last_minute == 0, "There should be no timeseries added in the last minute"

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
            assert created.created_time

            # Act
            initial_added_count = 0
            for _, timeseries in cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id):
                initial_added_count += len(timeseries.added)

            # Assert
            assert initial_added_count > 0, "There should be at least one numerical timeseries added"

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
            time.sleep(10)

            # Act
            updated_added_count = 0
            for _, timeseries in cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id):
                updated_added_count += len(timeseries.added)

            # Assert
            assert (
                initial_added_count + 1 == updated_added_count
            ), "The new timeseries should be added. Note this test is flaky and may fail."
        finally:
            if created:
                cognite_client.time_series.subscriptions.delete(new_subscription.external_id, ignore_unknown_ids=True)
            if created_timeseries:
                cognite_client.time_series.delete(created_timeseries.id)
