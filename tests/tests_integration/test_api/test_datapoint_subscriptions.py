from __future__ import annotations

import math
import platform
import random
import time
from contextlib import contextmanager
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries, filters
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DatapointSubscriptionProperty,
    DataPointSubscriptionUpdate,
    DataPointSubscriptionWrite,
)
from cognite.client.utils._text import random_string

TIMESERIES_EXTERNAL_IDS = [f"PYSDK DataPoint Subscription Test {no}" for no in range(10)]


@contextmanager
def create_subscription_with_cleanup(
    client: CogniteClient, sub_to_create: DataPointSubscriptionWrite
) -> DatapointSubscription:
    sub = None
    try:
        yield (sub := client.time_series.subscriptions.create(sub_to_create))
    finally:
        if sub:
            client.time_series.subscriptions.delete(external_id=sub.external_id, ignore_unknown_ids=True)


@pytest.fixture(scope="session")
def all_time_series_external_ids(cognite_client: CogniteClient) -> list[str]:
    existing_xids = cognite_client.time_series.retrieve_multiple(
        external_ids=TIMESERIES_EXTERNAL_IDS, ignore_unknown_ids=True
    ).as_external_ids()

    if len(existing_xids) == len(TIMESERIES_EXTERNAL_IDS):
        return existing_xids

    return cognite_client.time_series.upsert(
        [
            TimeSeries(external_id=external_id, name=external_id, is_string=False)
            for external_id in TIMESERIES_EXTERNAL_IDS
        ],
        mode="overwrite",
    ).as_external_ids()


@pytest.fixture
def time_series_external_ids(all_time_series_external_ids):
    # Spread the load to avoid API errors like 'a ts can't be part of too many subscriptions':
    ts_xids = all_time_series_external_ids[:]
    return random.sample(ts_xids, k=4)


@pytest.fixture(scope="session")
def subscription(cognite_client: CogniteClient, all_time_series_external_ids: list[str]) -> DatapointSubscription:
    external_id = f"PYSDKDataPointSubscriptionTest-{platform.system()}"
    sub = cognite_client.time_series.subscriptions.retrieve(external_id)
    if sub is not None:
        return sub
    new_sub = DataPointSubscriptionWrite(
        external_id=external_id,
        name=f"{external_id}_3ts",
        time_series_ids=all_time_series_external_ids[:3],
        partition_count=1,
    )
    return cognite_client.time_series.subscriptions.create(new_sub)


@pytest.fixture
def sub_for_status_codes(cognite_client: CogniteClient, time_series_external_ids: list[str]) -> DatapointSubscription:
    external_id = f"PYSDKDataPointSubscriptionTestWithStatusCodes-{random_string(5)}"
    new_sub = DataPointSubscriptionWrite(
        external_id=external_id,
        name=f"{external_id}_1ts",
        time_series_ids=time_series_external_ids[:1],
        partition_count=1,
    )
    with create_subscription_with_cleanup(cognite_client, new_sub) as created:
        yield created


class TestDatapointSubscriptions:
    def test_list_subscriptions(self, cognite_client: CogniteClient, subscription: DatapointSubscription) -> None:
        subscriptions = cognite_client.time_series.subscriptions.list(limit=5)
        assert len(subscriptions) > 0, "Add at least one subscription to the test environment to run this test"

    def test_create_retrieve_delete_subscription(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        data_set = cognite_client.data_sets.list(limit=1)[0]
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionCreateRetrieveDeleteTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionCreateRetrieveDeleteTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
            data_set_id=data_set.id,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription) as created:
            retrieved_subscription = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)

            assert created.created_time
            assert created.last_updated_time
            assert created.time_series_count == len(new_subscription.time_series_ids)
            assert retrieved_subscription.external_id == new_subscription.external_id == created.external_id
            assert retrieved_subscription.name == new_subscription.name == created.name
            assert retrieved_subscription.description == new_subscription.description == created.description
            assert retrieved_subscription.data_set_id == new_subscription.data_set_id == created.data_set_id

            time_series_in_subscription = cognite_client.time_series.subscriptions.list_member_time_series(
                new_subscription.external_id, limit=10
            )
            retrieved_time_series_external_ids = [ts.external_id for ts in time_series_in_subscription]
            assert sorted(new_subscription.time_series_ids) == sorted(retrieved_time_series_external_ids)

            cognite_client.time_series.subscriptions.delete(new_subscription.external_id)
            retrieved_deleted = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)
            assert retrieved_deleted is None

    def test_update_subscription(self, cognite_client: CogniteClient, time_series_external_ids: list[str]):
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionUpdateTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionUpdateTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )
        data_set = cognite_client.data_sets.list(limit=1)[0]
        with create_subscription_with_cleanup(cognite_client, new_subscription):
            update = (
                DataPointSubscriptionUpdate(new_subscription.external_id)
                .name.set("New Name")
                .time_series_ids.remove([time_series_external_ids[0]])
                .data_set_id.set(data_set.id)
            )
            updated = cognite_client.time_series.subscriptions.update(update)

            assert updated.name == "New Name"
            assert updated.time_series_count == len(time_series_external_ids) - 1
            assert updated.data_set_id == data_set.id

    def test_update_filter_defined_subscription(self, cognite_client: CogniteClient):
        f = filters
        p = DatapointSubscriptionProperty
        numerical_timeseries = f.And(f.Equals(p.is_string, False))

        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKFilterDefinedDataPointSubscriptionUpdateTest-{random_string(10)}",
            name="PYSDKFilterDefinedDataPointSubscriptionUpdateTest",
            filter=numerical_timeseries,
            partition_count=1,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription):
            new_filter = f.And(
                f.Equals(p.is_string, False), f.Prefix(p.external_id, "PYSDK DataPoint Subscription Test")
            )
            new_update = DataPointSubscriptionUpdate(new_subscription.external_id).filter.set(new_filter)

            # There is a bug in the API that causes the updated filter not to be returned when calling update.
            cognite_client.time_series.subscriptions.update(new_update)
            retrieved = cognite_client.time_series.subscriptions.retrieve(new_subscription.external_id)

            assert retrieved.filter.dump() == new_filter.dump()

    def test_iterate_data_subscription_initial_call(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionListDataTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionListDataTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription):
            subscription_changes = cognite_client.time_series.subscriptions.iterate_data(
                new_subscription.external_id,
                poll_timeout=0,
            )
            batch = next(subscription_changes)

            assert (
                len(batch.subscription_changes.added) > 0
            ), "The subscription used for testing datapoint subscriptions must have at least one time series"

            batch = next(subscription_changes)
            assert (
                len(batch.subscription_changes.added) == 0
            ), "There should be no more timeseries in the subsequent batches"

    def test_iterate_data_subscription_changed_time_series(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        first_ts, second_ts = time_series_external_ids[:2]
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionChangedTimeSeriesTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            time_series_ids=[first_ts],
            partition_count=1,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription):
            subscription_changes = cognite_client.time_series.subscriptions.iterate_data(
                new_subscription.external_id,
                poll_timeout=0,
            )
            batch = next(subscription_changes)

            assert batch.subscription_changes.added[0].external_id == first_ts
            assert len(batch.updates) == 0

            update = (
                DataPointSubscriptionUpdate(new_subscription.external_id)
                .time_series_ids.add([second_ts])
                .time_series_ids.remove([first_ts])
            )
            cognite_client.time_series.subscriptions.update(update)
            batch = next(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))

            assert {a.external_id for a in batch.subscription_changes.added} == {second_ts, first_ts}
            assert {a.external_id for a in batch.subscription_changes.removed} == {first_ts}
            assert len(batch.updates) == 0

    def test_iterate_data_subscription_datapoints_added(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionChangedTimeSeriesTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionChangedTimeSeriesTest",
            time_series_ids=[time_series_external_ids[0]],
            partition_count=1,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription) as created:
            assert created.created_time
            batch = next(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))
            assert batch.subscription_changes.added[0].external_id == time_series_external_ids[0]

            existing_data = cognite_client.time_series.data.retrieve_dataframe(external_id=time_series_external_ids[0])
            new_values = [42, 43]
            new_data = pd.DataFrame(
                {time_series_external_ids[0]: new_values},
                index=pd.date_range(start=existing_data.index[-1] + pd.Timedelta("1d"), periods=2, freq="1d"),
            )
            new_timestamps = new_data.index.asi8 // 10**6
            try:
                cognite_client.time_series.data.insert_dataframe(new_data)
                batch = next(cognite_client.time_series.subscriptions.iterate_data(new_subscription.external_id))

                assert batch.updates
                np.testing.assert_allclose(
                    new_values,
                    [dp.value for dp in batch.updates[0].upserts],
                    err_msg="The values of the retrieved data should be the same as the inserted data (to float precision)",
                )
                np.testing.assert_equal(
                    new_timestamps,
                    [dp.timestamp for dp in batch.updates[0].upserts],
                    err_msg="The timestamps of the retrieved data should be exactly equal to the inserted ones",
                )
            finally:
                cognite_client.time_series.data.delete_range(
                    new_timestamps[0], new_timestamps[-1] + 1, external_id=time_series_external_ids[0]
                )

    def test_iterate_data_subscription_jump_to_end(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionJumpToEndTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionJumpToEndTest",
            time_series_ids=time_series_external_ids,
            partition_count=1,
        )
        with create_subscription_with_cleanup(cognite_client, new_subscription) as created:
            assert created.created_time

            for batch in cognite_client.time_series.subscriptions.iterate_data(
                new_subscription.external_id,
                start="now",
                poll_timeout=0,
            ):
                assert len(batch.updates) == 0
                assert len(batch.subscription_changes.added) == 0
                assert len(batch.subscription_changes.removed) == 0
                if not batch.has_next:
                    break

    def test_iterate_data_subscription_start_1m_ago(
        self, cognite_client: CogniteClient, subscription: DatapointSubscription
    ):
        added_last_minute = 0
        for batch in cognite_client.time_series.subscriptions.iterate_data(
            subscription.external_id,
            start="1m-ago",
            poll_timeout=0,
        ):
            added_last_minute += len(batch.subscription_changes.added)
            if not batch.has_next:
                break
        assert added_last_minute == 0, "There should be no timeseries added in the last minute"

    def test_iterate_data__using_status_codes(
        self, cognite_client: CogniteClient, sub_for_status_codes: DatapointSubscription
    ):
        no_bad_iter = cognite_client.time_series.subscriptions.iterate_data(
            sub_for_status_codes.external_id,
            poll_timeout=0,
            include_status=True,
            treat_uncertain_as_bad=False,
            ignore_bad_datapoints=True,
        )
        has_bad_iter = cognite_client.time_series.subscriptions.iterate_data(
            sub_for_status_codes.external_id,
            poll_timeout=0,
            include_status=True,
            ignore_bad_datapoints=False,
        )
        ts, *_ = cognite_client.time_series.subscriptions.list_member_time_series(sub_for_status_codes.external_id)
        cognite_client.time_series.data.insert(
            external_id=ts.external_id,
            datapoints=[
                {"timestamp": -99, "value": None, "status": {"symbol": "Bad"}},
                {"timestamp": -98, "value": math.nan, "status": {"symbol": "Bad"}},
                {"timestamp": -97, "value": math.inf, "status": {"symbol": "Bad"}},
                {"timestamp": -96, "value": -math.inf, "status": {"symbol": "Bad"}},
                {"timestamp": -95, "value": -95, "status": {"symbol": "Uncertain"}},
                {"timestamp": -94, "value": -94, "status": {"code": 1024}},
                {"timestamp": -93, "value": -93, "status": {"symbol": "Good"}},
                {"timestamp": -92, "value": -92},
            ],
        )
        no_bad = next(no_bad_iter).updates
        has_bad = next(has_bad_iter).updates

        assert len(no_bad) == 1
        assert len(has_bad) == 1
        assert ts.id == no_bad[0].time_series.id == no_bad[0].time_series.id

        assert no_bad[0].upserts.is_string is False
        assert has_bad[0].upserts.is_string is False
        assert no_bad[0].upserts.timestamp == list(range(-95, -91))
        assert has_bad[0].upserts.timestamp == list(range(-99, -91))
        assert has_bad[0].upserts.value[0] is None
        assert all(isinstance(v, float) for v in has_bad[0].upserts.value[1:])
        assert no_bad[0].upserts.status_symbol == ["Uncertain", "Good", "Good", "Good"]

    @pytest.mark.skip(reason="Using a filter (as opposed to specific identifiers) is eventually consistent")
    def test_update_filter_subscription_added_times_series(
        self, cognite_client: CogniteClient, time_series_external_ids: list[str]
    ):
        f = filters
        p = DatapointSubscriptionProperty
        numerical_timeseries = f.And(
            f.Equals(p.is_string, False), f.Prefix(p.external_id, "PYSDK DataPoint Subscription Test")
        )
        new_subscription = DataPointSubscriptionWrite(
            external_id=f"PYSDKDataPointSubscriptionUpdateFilterTest-{random_string(10)}",
            name="PYSDKDataPointSubscriptionUpdateFilterTest",
            filter=numerical_timeseries,
            partition_count=1,
        )
        created_timeseries: TimeSeries | None = None
        with create_subscription_with_cleanup(cognite_client, new_subscription) as created:
            assert created.created_time

            initial_added_count = 0
            for batch in cognite_client.time_series.subscriptions.iterate_data(
                new_subscription.external_id,
                poll_timeout=0,
            ):
                initial_added_count += len(batch.subscription_changes.added)
                if not batch.has_next:
                    break

            assert initial_added_count > 0, "There should be at least one numerical timeseries added"

            new_numerical_timeseries = TimeSeries(
                external_id=f"PYSDK DataPoint Subscription Test 42 ({random_string(10)})",
                name="PYSDK DataPoint Subscription Test 42",
                is_string=False,
            )
            try:
                created_timeseries = cognite_client.time_series.create(new_numerical_timeseries)
                cognite_client.time_series.data.insert(
                    [(datetime.now(), 42)], external_id=new_numerical_timeseries.external_id
                )
                # Ensure that the subscription has been updated
                time.sleep(10)

                updated_added_count = 0
                for batch in cognite_client.time_series.subscriptions.iterate_data(
                    new_subscription.external_id,
                    poll_timeout=0,
                ):
                    updated_added_count += len(batch.subscription_changes.added)
                    if not batch.has_next:
                        break

                assert initial_added_count + 1 == updated_added_count, (
                    "The new timeseries should be added. This is most likely because using a filter with "
                    "datapoint subscriptions is eventually consistent."
                )
            finally:
                if created_timeseries:
                    cognite_client.time_series.delete(created_timeseries.id)
