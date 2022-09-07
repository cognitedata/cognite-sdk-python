from __future__ import annotations

import os
import pathlib
import random
from dataclasses import dataclass
from itertools import product
from typing import Iterable, Literal

import numpy as np
import pandas as pd

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthInteractive
from cognite.client.data_classes import TimeSeries

TIME_SERIES_COUNT = {"single": 1, "few": 100, "some": 1_000, "many": 10_000}
THIS_FILE = pathlib.Path(__file__)


def get_client() -> CogniteClient:
    credentials = OAuthInteractive(
        authority_url=os.environ["COGNITE_AUTHORITY_URL"],
        client_id=os.environ["COGNITE_CLIENT_ID"],
        scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
        redirect_port=random.randint(53000, 60000),  # random port so we can run the test suite in parallel
    )
    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=credentials,
        )
    )


@dataclass
class Case:
    count: Literal["single", "few", "some", "many"]
    length: Literal["shallow", "mixed", "deep"]
    dtype: Literal["string", "long_string", "number"] = "number"
    resolution: Literal["dense", "sparse", "mixed"] = "dense"
    aggregation: Literal["raw", "mean", "mixed"] = "raw"
    window: Literal["limited", "unlimited"] = "unlimited"
    out_format: Literal["dataframe", "datapoints"] = "dataframe"

    @property
    def external_id_prefix(self) -> str:
        return f"benchmark:{self.count}:{self.length}:{self.dtype}:{self.resolution}"

    @property
    def test_name(self) -> str:
        return f"test_{self.count}_{self.length}_{self.dtype}_{self.resolution}_{self.aggregation}_{self.window}_{self.out_format}"


def create_test_cases() -> Iterable[Case]:
    test_series = list(product(["single", "few", "some", "many"], ["shallow", "mixed", "deep"]))
    test_series.remove(("single", "mixed"))
    test_series.remove(("some", "mixed"))
    test_series.remove(("many", "mixed"))
    test_series.remove(("few", "deep"))
    test_series.remove(("some", "deep"))
    test_series.remove(("many", "deep"))
    for count, size in test_series:
        yield Case(count, size)


def create_dataframe(case: Case) -> pd.DataFrame:
    time_series_count = TIME_SERIES_COUNT[case.count]
    time_series = []
    if case.length == "deep":
        lengths = ["deep"] * time_series_count
    elif case.length == "mixed":
        lengths = ["shallow"] * (time_series_count - 10) + ["deep"] * 10
        random.shuffle(lengths)
    else:
        lengths = ["shallow"] * time_series_count

    for no, length in enumerate(lengths):
        value_count = int(1e6) if length == "deep" else 100
        values = range(1, value_count + 1)
        if case.dtype != "number":
            raise NotImplementedError("Only data type number supported")
        timestamps = create_timestamps(length, case.resolution, value_count)
        series = pd.Series(values, timestamps, name=f"{case.external_id_prefix}:{no}")
        time_series.append(series)
    return pd.concat(time_series, axis=1)


def create_timestamps(
    length: Literal["shallow", "deep"],
    resolution: Literal["dense", "sparse"],
    value_count: int,
) -> pd.DatetimeIndex:
    if resolution == "dense":
        frequency = {"shallow": "1d", "deep": "1s"}[length]
        return pd.date_range(start="2020-01-01", periods=value_count, freq=frequency)
    increase = (np.random.randn(value_count) * 1_000).astype(np.int64)
    increase[0] = np.int64(pd.Timestamp("2020-01-01").timestamp() * 1_000)
    return pd.to_datetime(increase.cumsum(), unit="ms")


def try_create_time_series(client: CogniteClient, cases: list[Case]) -> list[str]:
    created = []
    for case in cases:
        found = client.time_series.list(external_id_prefix=case.external_id_prefix)
        if found:
            continue
        time_series = [
            TimeSeries(
                external_id=external_id,
                name=external_id.replace(":", " ").title(),
            )
            for no in range(TIME_SERIES_COUNT[case.count])
            if (external_id := f"{case.external_id_prefix}:{no}")
        ]
        created.append(case.external_id_prefix)
        client.time_series.create(time_series)
    return created


def create_test_file_header() -> str:
    return f"""\"\"\"
This file is automatically generate from {THIS_FILE.name!r}. DO NOT MANUALLY CHANGE IT!
\"\"\"
from cognite.client import CogniteClient

"""


def create_test(case: Case) -> str:
    count = TIME_SERIES_COUNT[case.count]
    external_id = 'f"%s:{no}"' % case.external_id_prefix
    return f"""
def {case.test_name}(
    cognite_client: CogniteClient, benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[{external_id} for no in range({count})],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )

"""


def upload_data_to_cdf(test_cases: list[Case]):
    client = get_client()
    created = set(try_create_time_series(client, test_cases))
    print(f"Created {', '.join(created)}")
    for case in test_cases:
        df = create_dataframe(case)
        memory = df.memory_usage().sum()
        print(f"{case.external_id_prefix}: {memory*1e-6:,.3f} MB")
        if case.external_id_prefix in created:
            client.time_series.data.insert_dataframe(df)
            print(f"Inserted {len(df)*len(df.columns):,} datapoints into {case.external_id_prefix}")


def create_benchmark_test_file(test_cases: list[Case]):
    test_file = [create_test_file_header()]
    test_file.extend(create_test(case) for case in test_cases)
    test_file = "".join(test_file)
    with open(THIS_FILE.parent / "test_datapoints_benchmarks.py", "w") as file:
        file.write(test_file)


def main():
    test_cases = list(create_test_cases())
    # upload_data_to_cdf(test_cases)
    create_benchmark_test_file(test_cases)


if __name__ == "__main__":
    main()
