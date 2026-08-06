from __future__ import annotations

import contextlib
import datetime
import os
import sys
import uuid
from collections.abc import Iterator

import pytest

from cognite.client import AsyncCogniteClient, ClientConfig, CogniteClient
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, SpaceApply, ViewId
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.utils._experimental import FeaturePreviewWarning

# Kept short: data modeling space ids are capped at 43 chars and the per-process
# shard (os/python/worker/random) appended below consumes most of the budget.
SPACE_PREFIX = "sp_sdk_state_ts"
STATE_SET_VIEW = ViewId("cdf_cdm", "CogniteStateSet", "v1")
TIME_SERIES_VIEW = ViewId("cdf_cdm", "CogniteTimeSeries", "v1")

# Fixed timestamps so aggregate assertions stay deterministic.
TS_T0 = 1_609_459_200_000  # 2021-01-01 00:00:00 UTC
TS_T1 = 1_609_462_800_000  # +1h
TS_T2 = 1_609_466_400_000  # +2h
QUERY_END = 1_609_545_600_000  # 2021-01-02 00:00:00 UTC


def _shard_id(worker_id: str) -> str:
    """Return a unique-per-process suffix, including OS / Python version / xdist worker / random."""
    py = f"py{sys.version_info.major}{sys.version_info.minor}"
    osid = "win" if os.name == "nt" else "nix"
    return f"{osid}_{py}_{worker_id}_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def beta_cognite_client(cognite_client: CogniteClient) -> CogniteClient:
    cfg = cognite_client.config
    return CogniteClient(
        ClientConfig(
            client_name=cfg.client_name,
            project=cfg.project,
            base_url=cfg.base_url,
            credentials=cfg.credentials,
            api_subversion="beta",
        )
    )


@pytest.fixture(scope="session")
def beta_async_client(async_client: AsyncCogniteClient) -> AsyncCogniteClient:
    cfg = async_client.config
    return AsyncCogniteClient(
        ClientConfig(
            client_name=cfg.client_name,
            project=cfg.project,
            base_url=cfg.base_url,
            credentials=cfg.credentials,
            api_subversion="beta",
        )
    )


@pytest.fixture(scope="class")
def state_ts_resources(beta_cognite_client: CogniteClient, worker_id: str) -> Iterator[tuple[str, str, str, NodeId]]:
    shard = _shard_id(worker_id)
    space = f"{SPACE_PREFIX}_{shard}"
    state_set_xid = f"valve_states_{shard}"
    ts_xid = f"valve_001_state_{shard}"

    beta_cognite_client.data_modeling.spaces.apply(SpaceApply(space=space))
    beta_cognite_client.data_modeling.instances.apply(
        nodes=[
            NodeApply(
                space=space,
                external_id=state_set_xid,
                sources=[
                    NodeOrEdgeData(
                        source=STATE_SET_VIEW,
                        properties={
                            "name": "Valve Position States",
                            "description": "Standard position states for industrial valves",
                            "states": [
                                {"numericValue": 0, "stringValue": "CLOSED"},
                                {"numericValue": 1, "stringValue": "OPEN"},
                                {"numericValue": 2, "stringValue": "TRANSITIONING"},
                            ],
                        },
                    )
                ],
            ),
            NodeApply(
                space=space,
                external_id=ts_xid,
                sources=[
                    NodeOrEdgeData(
                        source=TIME_SERIES_VIEW,
                        properties={
                            "name": "Valve 001 Position",
                            "description": "Integration test state time series",
                            "type": "state",
                            "stateSet": {"space": space, "externalId": state_set_xid},
                        },
                    )
                ],
            ),
        ],
        replace=True,
    )

    ts_node_id = NodeId(space, ts_xid)
    try:
        yield space, state_set_xid, ts_xid, ts_node_id
    finally:
        with contextlib.suppress(Exception):
            beta_cognite_client.data_modeling.instances.delete(
                nodes=[NodeId(space, ts_xid), NodeId(space, state_set_xid)]
            )
        with contextlib.suppress(Exception):
            beta_cognite_client.data_modeling.spaces.delete(spaces=[space])


def _state_datapoints() -> list[dict[str, int | float | str | datetime.datetime]]:
    return [
        {"timestamp": TS_T0, "numericValue": 0, "stringValue": "CLOSED"},
        {"timestamp": TS_T1, "numericValue": 1, "stringValue": "OPEN"},
        {"timestamp": TS_T2, "numericValue": 0, "stringValue": "CLOSED"},
    ]


class TestStateTimeSeries:
    @pytest.mark.allow_no_semaphore(
        "State datapoint insert goes through DatapointsAPI._insert_datapoints, which holds the "
        "semaphore via outer 'async with' and passes None to _post to avoid double-acquiring."
    )
    def test_insert_and_retrieve_raw(
        self,
        beta_cognite_client: CogniteClient,
        state_ts_resources: tuple[str, str, str, NodeId],
    ) -> None:
        _, _, _, ts_node_id = state_ts_resources

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            beta_cognite_client.time_series.data.insert(_state_datapoints(), instance_id=ts_node_id)

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            dps = beta_cognite_client.time_series.data.retrieve(instance_id=ts_node_id, start=TS_T0, end=QUERY_END)

        assert dps is not None
        assert dps.type == "state"
        assert list(dps.timestamp) == [TS_T0, TS_T1, TS_T2]
        assert dps.numeric_value == [0, 1, 0]
        assert dps.string_value == ["CLOSED", "OPEN", "CLOSED"]

    def test_retrieve_arrays_state_raw(
        self,
        beta_cognite_client: CogniteClient,
        state_ts_resources: tuple[str, str, str, NodeId],
    ) -> None:
        import numpy as np

        _, _, _, ts_node_id = state_ts_resources

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            arr = beta_cognite_client.time_series.data.retrieve_arrays(
                instance_id=ts_node_id, start=TS_T0, end=QUERY_END
            )

        assert arr is not None
        assert arr.type == "state"
        assert arr.numeric_value is not None and arr.string_value is not None
        assert arr.numeric_value.dtype == np.object_
        assert arr.string_value.dtype == np.object_
        assert arr.numeric_value.tolist() == [0, 1, 0]
        assert arr.string_value.tolist() == ["CLOSED", "OPEN", "CLOSED"]

    def test_retrieve_state_aggregates(
        self,
        beta_cognite_client: CogniteClient,
        state_ts_resources: tuple[str, str, str, NodeId],
    ) -> None:
        _, _, _, ts_node_id = state_ts_resources

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            dps = beta_cognite_client.time_series.data.retrieve(
                instance_id=ts_node_id,
                start=TS_T0,
                end=QUERY_END,
                aggregates=["state_count", "state_transitions", "state_duration"],
                granularity="1d",
            )

        assert dps is not None
        assert dps.state_aggregates is not None and len(dps.state_aggregates) >= 1
        rows = dps.state_aggregates[0]
        by_value = {row.numeric_value: row for row in rows}

        closed, open_ = by_value[0], by_value[1]
        assert closed.string_value == "CLOSED"
        assert open_.string_value == "OPEN"
        assert closed.state_count == 2
        assert open_.state_count == 1
        assert closed.state_transitions == 2
        assert open_.state_transitions == 1
        assert open_.state_duration == 3_600_000
        assert closed.state_duration is not None and closed.state_duration > 0


class TestStateTimeSeriesAsync:
    @pytest.mark.allow_no_semaphore(
        "State datapoint insert goes through DatapointsAPI._insert_datapoints, which holds the "
        "semaphore via outer 'async with' and passes None to _post to avoid double-acquiring."
    )
    async def test_insert_and_retrieve_raw_async(
        self,
        beta_async_client: AsyncCogniteClient,
        state_ts_resources: tuple[str, str, str, NodeId],
    ) -> None:
        _, _, _, ts_node_id = state_ts_resources

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            await beta_async_client.time_series.data.insert(_state_datapoints(), instance_id=ts_node_id)

        with pytest.warns(FeaturePreviewWarning, match="State time series"):
            dps = await beta_async_client.time_series.data.retrieve(instance_id=ts_node_id, start=TS_T0, end=QUERY_END)

        assert dps is not None
        assert dps.type == "state"
        assert dps.numeric_value == [0, 1, 0]
        assert dps.string_value == ["CLOSED", "OPEN", "CLOSED"]
