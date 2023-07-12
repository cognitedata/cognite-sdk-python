from datetime import datetime

import pandas as pd

from cognite.client import CogniteClient
from cognite.client.data_classes import DataPointSubscriptionCreate, TimeSeries

TIMESERIES_EXTERNAL_IDS = [f"PYSDK DataPoint Subscription Test {no}" for no in range(10)]


def main(client: CogniteClient):
    # Create timeseries
    timeseries = [
        TimeSeries(
            external_id=external_id,
            name=external_id,
            is_string=False,
        )
        for external_id in TIMESERIES_EXTERNAL_IDS
    ]
    existing = client.time_series.retrieve_multiple(external_ids=TIMESERIES_EXTERNAL_IDS, ignore_unknown_ids=True)
    existing_ids = {ts.external_id for ts in existing}
    new_timeseries = client.time_series.create(
        time_series=[ts for ts in timeseries if ts.external_id not in existing_ids]
    )
    print(f"Created {len(new_timeseries)} new timeseries")

    # Create datapoint datapoints
    for no, external_id in enumerate(TIMESERIES_EXTERNAL_IDS, 1):
        datapoints = pd.DataFrame(
            index=pd.date_range(start=datetime(2023, 7, no), periods=10, freq="1D"), data={external_id: list(range(10))}
        )
        client.time_series.data.insert_dataframe(datapoints, external_id_headers=True)
        print(f"Inserted datapoints for {external_id}")

    # Create datapoint subscriptions
    sub1 = DataPointSubscriptionCreate(
        external_id="PYSDKDataPointSubscriptionTest1",
        name="PYSDKDataPointSubscriptionTest1_1ts",
        time_series_ids=[TIMESERIES_EXTERNAL_IDS[0]],
        partition_count=1,
    )
    client.time_series.subscriptions.create(sub1)
    print("Created datapoint subscriptions sub1")
    sub2 = DataPointSubscriptionCreate(
        external_id="PYSDKDataPointSubscriptionTest2",
        name="PYSDKDataPointSubscriptionTest2_3ts",
        time_series_ids=TIMESERIES_EXTERNAL_IDS[:3],
        partition_count=1,
    )
    client.time_series.subscriptions.create(sub2)
    print("Created datapoint subscriptions sub2")
    sub3 = DataPointSubscriptionCreate(
        external_id="PYSDKDataPointSubscriptionTest3",
        name="PYSDKDataPointSubscriptionTest3_10ts",
        time_series_ids=TIMESERIES_EXTERNAL_IDS,
        partition_count=2,
    )

    client.time_series.subscriptions.create(sub3)
    print("Created datapoint subscriptions sub3")
    print("Done")


if __name__ == "__main__":
    # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    main(local_client.get_interactive())
