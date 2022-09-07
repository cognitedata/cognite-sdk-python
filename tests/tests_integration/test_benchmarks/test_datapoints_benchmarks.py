"""
This file is automatically generate from 'create_datapoints.py'. DO NOT MANUALLY CHANGE IT!
"""
from cognite.client import CogniteClient


def test_single_shallow_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:single:shallow:number:dense:{no}" for no in range(1)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )


def test_single_deep_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:single:deep:number:dense:{no}" for no in range(1)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )


def test_few_shallow_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:few:shallow:number:dense:{no}" for no in range(100)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )


def test_few_mixed_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:few:mixed:number:dense:{no}" for no in range(100)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )


def test_some_shallow_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:some:shallow:number:dense:{no}" for no in range(1000)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )


def test_many_shallow_number_dense_raw_unlimited_dataframe(
    cognite_client: CogniteClient,
    benchmark,
):
    result = benchmark(
        cognite_client.time_series.data.retrieve_dataframe,
        external_id=[f"benchmark:many:shallow:number:dense:{no}" for no in range(10000)],
    )

    assert all(
        series.sum() == len(series) * (len(series) + 1) / 2
        for col in result
        if not (series := result[col].dropna()).empty
    )
