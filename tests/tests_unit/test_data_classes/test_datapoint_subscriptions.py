from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes import Datapoint, filters
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DataPointSubscriptionWrite,
    DatapointsUpdate,
    TimeSeriesID,
    TimeSeriesIDList,
)


class TestDataPointSubscription:
    def test_raises_value_error_on_invalid_filter(self) -> None:
        f = filters
        nested_filter = f.Nested(
            scope=("some", "direct_relation", "property"), filter=f.Equals(property=["node", "name"], value="ACME")
        )
        with pytest.raises(ValueError) as e:
            DataPointSubscriptionWrite(
                external_id="MySubscription", partition_count=10, name="MySubscription", filter=nested_filter
            )
        assert "Nested" in str(e.value) and "not supported" in str(e.value)

    def test_handles_null_timeseries_count(self) -> None:
        sub = DatapointSubscription.load(
            {
                "externalId": "MySubscription",
                "partitionCount": 10,
                "name": "MySubscription",
                "createdTime": 123,
                "lastUpdatedTime": 456,
            }
        )
        assert sub.time_series_count is None


class TestTimeSeriesID:
    def test_load_with_all_fields(self) -> None:
        ts_id = TimeSeriesID._load({"id": 123, "externalId": "my_ts"})
        assert ts_id.id == 123
        assert ts_id.external_id == "my_ts"
        assert ts_id.instance_id is None
        assert ts_id.is_resolved is True

    def test_load_with_missing_id_broken_reference(self) -> None:
        """Test that TimeSeriesID can be loaded when 'id' is missing (broken reference scenario).

        This happens when a time series's external_id is changed or the time series is deleted,
        but the subscription still references the old external_id.
        """
        ts_id = TimeSeriesID._load({"externalId": "deleted_or_renamed_ts"})
        assert ts_id.id is None
        assert ts_id.external_id == "deleted_or_renamed_ts"
        assert ts_id.instance_id is None
        assert ts_id.is_resolved is False

    def test_load_with_instance_id_only(self) -> None:
        """Test loading a TimeSeriesID with only an instance_id (broken reference)."""
        ts_id = TimeSeriesID._load({"instanceId": {"space": "my_space", "externalId": "my_node"}})
        assert ts_id.id is None
        assert ts_id.external_id is None
        assert ts_id.instance_id is not None
        assert ts_id.instance_id.space == "my_space"
        assert ts_id.instance_id.external_id == "my_node"
        assert ts_id.is_resolved is False

    def test_repr_with_id(self) -> None:
        ts_id = TimeSeriesID(id=123, external_id="my_ts")
        assert repr(ts_id) == "TimeSeriesID(id=123, external_id=my_ts)"

    def test_repr_without_id(self) -> None:
        ts_id = TimeSeriesID(external_id="broken_ref")
        assert repr(ts_id) == "TimeSeriesID(external_id=broken_ref)"

    def test_dump_with_id(self) -> None:
        ts_id = TimeSeriesID(id=123, external_id="my_ts")
        dumped = ts_id.dump()
        assert dumped == {"id": 123, "externalId": "my_ts"}

    def test_dump_without_id(self) -> None:
        """Test that dump excludes 'id' when it's None."""
        ts_id = TimeSeriesID(external_id="broken_ref")
        dumped = ts_id.dump()
        assert dumped == {"externalId": "broken_ref"}
        assert "id" not in dumped

    def test_time_series_id_list_with_broken_references(self) -> None:
        """Test that TimeSeriesIDList can handle a mix of resolved and broken references."""
        items: list[dict] = [
            {"id": 123, "externalId": "ts_1"},
            {"externalId": "broken_ref"},  # No id - broken reference
            {"id": 456, "externalId": "ts_2"},
        ]
        ts_list = TimeSeriesIDList._load(items)
        assert len(ts_list) == 3
        assert ts_list[0].is_resolved is True
        assert ts_list[1].is_resolved is False
        assert ts_list[2].is_resolved is True


class TestSubscriptionDatapoints:
    @pytest.fixture
    def state_update(self) -> dict[str, Any]:
        return {
            "timeSeries": {
                "id": 1,
                "instanceId": {"space": "sp", "externalId": "xid"},
                "type": "state",
                "isString": False,
            },
            "upserts": [
                {"timestamp": 1000, "numericValue": 1, "stringValue": "ON"},
                {"timestamp": 2000, "numericValue": 0},  # state no longer part of the state set
                {"timestamp": 3000, "status": {"code": 0x80000000, "symbol": "Bad"}},  # bad status, no state
            ],
            "deletes": [],
        }

    @pytest.mark.parametrize("include_status", [False, True])
    def test_load_state_datapoints(self, state_update: dict[str, Any], include_status: bool) -> None:
        update = DatapointsUpdate.load(state_update, include_status=include_status, ignore_bad_datapoints=False)
        dps = update.upserts
        assert dps.type == "state"
        assert dps.value is None
        assert dps.timestamp == [1000, 2000, 3000]
        assert dps.numeric_states == [1, 0, None]
        assert dps.string_states == ["ON", None, None]
        if include_status:
            assert dps.status_code == [0, 0, 0x80000000]
            assert dps.status_symbol == ["Good", "Good", "Bad"]
        else:
            assert dps.status_code is None and dps.status_symbol is None

    def test_iterate_state_datapoints(self, state_update: dict[str, Any]) -> None:
        dps = DatapointsUpdate.load(state_update, include_status=True, ignore_bad_datapoints=False).upserts
        first, _, last = list(dps)
        assert isinstance(first, Datapoint)
        assert (first.timestamp, first.value, first.numeric_state, first.string_state) == (1000, None, 1, "ON")
        assert (last.numeric_state, last.string_state, last.status_symbol) == (None, None, "Bad")

    def test_load_numeric_datapoints_unchanged(self) -> None:
        update = DatapointsUpdate.load(
            {
                "timeSeries": {"id": 1, "externalId": "xid", "type": "numeric", "isString": False},
                "upserts": [{"timestamp": 1000, "value": 1.5}],
                "deletes": [],
            }
        )
        dps = update.upserts
        assert dps.value == [1.5]
        assert dps.numeric_states is None and dps.string_states is None
        (dp,) = list(dps)
        assert (dp.timestamp, dp.value, dp.numeric_state) == (1000, 1.5, None)

    @pytest.mark.dsl
    def test_state_datapoints_to_pandas(self, state_update: dict[str, Any]) -> None:
        dps = DatapointsUpdate.load(state_update, include_status=False, ignore_bad_datapoints=False).upserts
        df = dps.to_pandas()
        node_id = NodeId("sp", "xid")
        assert list(df.columns) == [(node_id, "numeric"), (node_id, "string")]
        assert df[(node_id, "numeric")].tolist()[:2] == [1, 0]
        assert df[(node_id, "string")].tolist()[0] == "ON"
