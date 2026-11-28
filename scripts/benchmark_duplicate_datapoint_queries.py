"""Measure duplicate datapoint query work offline with a mock CDF transport.

Run from the repository root: uv run python scripts/benchmark_duplicate_datapoint_queries.py
The median includes SDK processing and mock responses without simulated network delay.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import warnings
from dataclasses import dataclass
from statistics import median
from time import perf_counter
from unittest.mock import patch

import httpx2

from cognite.client import AsyncCogniteClient, ClientConfig, global_config
from cognite.client._proto.data_point_list_response_pb2 import (
    TIMESERIES_TYPE_NUMERIC,
    DataPointListItem,
    DataPointListResponse,
)
from cognite.client._proto.data_points_pb2 import NumericDatapoint
from cognite.client.credentials import Token
from cognite.client.data_classes import DatapointsList

SERIES_IDS = list(range(1, 21))
POINTS_PER_SERIES = 256
END_MS = POINTS_PER_SERIES * 1_000
TIMESTAMPS = list(range(0, END_MS, 1_000))


@dataclass
class Traffic:
    requests: int = 0
    items: int = 0
    request_bytes: int = 0
    response_bytes: int = 0
    response_points: int = 0


class MockDatapointsEndpoint:
    def __init__(self) -> None:
        self.traffic = Traffic()
        self.series = {
            series_id: DataPointListItem(
                id=series_id,
                externalId=f"sensor-{series_id}",
                type=TIMESERIES_TYPE_NUMERIC,
                numericDatapoints={
                    "datapoints": [
                        NumericDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(TIMESTAMPS, values_for(series_id), strict=True)
                    ]
                },
            )
            for series_id in SERIES_IDS
        }

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.method == "POST" and request.url.path.endswith("/timeseries/data/list"), request.url
        body = request.content
        items = json.loads(gzip.decompress(body) if request.headers.get("Content-Encoding") == "gzip" else body)[
            "items"
        ]
        response = DataPointListResponse()
        for item in items:
            series = self.series[item["id"]]
            # The workload uses one complete page per series. Check that every request can accept it.
            assert item["start"] == 0 and item["end"] == END_MS and item["limit"] >= POINTS_PER_SERIES, item
            response.items.append(series)

        payload = response.SerializeToString()
        self.traffic.requests += 1
        self.traffic.items += len(items)
        self.traffic.request_bytes += len(body)
        self.traffic.response_bytes += len(payload)
        self.traffic.response_points += len(items) * POINTS_PER_SERIES
        return httpx2.Response(200, content=payload)


def values_for(series_id: int) -> list[int]:
    return [series_id * POINTS_PER_SERIES + index for index in range(POINTS_PER_SERIES)]


def check_results(result: DatapointsList, ids: list[int]) -> None:
    assert len(result) == len(ids)
    for series, series_id in zip(result, ids, strict=True):
        assert series.id == series_id and series.external_id == f"sensor-{series_id}"
        assert series.timestamp == TIMESTAMPS and series.value == values_for(series_id)


async def benchmark(samples: int) -> None:
    global_config.disable_pypi_version_check = True
    endpoint = MockDatapointsEndpoint()
    config = ClientConfig(
        client_name="offline-datapoints-benchmark",
        project="benchmark",
        credentials=Token("offline"),
        base_url="https://benchmark.invalid",
    )
    client = AsyncCogniteClient(config)
    print("Local mock transport. Times include SDK and mock processing with no network latency.")
    print(f"{len(SERIES_IDS)} unique IDs, {POINTS_PER_SERIES} points per result, median of {samples} runs.")
    print(
        "copies  input  HTTP calls  sent items  sent bytes  received bytes  received points  output points  median ms"
    )
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(endpoint)) as http_client:
        with patch("cognite.client._http_client.get_global_async_httpx_client", return_value=http_client):
            for copies in (1, 2, 10, 30):
                ids = SERIES_IDS * copies
                timings = []
                traffic = None
                for _ in range(samples + 1):
                    endpoint.traffic = Traffic()
                    start = perf_counter()
                    result = await client.time_series.data.retrieve(
                        id=ids, start=0, end=END_MS, limit=POINTS_PER_SERIES
                    )
                    timings.append(perf_counter() - start)
                    check_results(result, ids)
                    if traffic is None:
                        traffic = endpoint.traffic
                    else:
                        assert traffic == endpoint.traffic, (traffic, endpoint.traffic)

                assert traffic is not None
                print(
                    f"{copies:>6} {len(ids):>6} {traffic.requests:>11} {traffic.items:>11} {traffic.request_bytes:>11} "
                    f"{traffic.response_bytes:>15} {traffic.response_points:>16} {len(ids) * POINTS_PER_SERIES:>14} "
                    f"{median(timings[1:]) * 1_000:>10.2f}"
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=9, help="Timed calls per workload after a warmup call")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be at least 1")
    with warnings.catch_warnings():
        # Internal IDs reproduce the workload described in #2838.
        warnings.filterwarnings("ignore", message="Identifying time series by", category=DeprecationWarning)
        asyncio.run(benchmark(args.samples))
