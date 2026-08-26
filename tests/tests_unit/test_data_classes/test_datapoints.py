from __future__ import annotations

import math
from datetime import timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from cognite.client.data_classes import Datapoint, DatapointsArray, StateDatapointsInsert, StateDatapointWrite
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import DatapointsArrayList, DatapointsList
from tests.utils import PANDAS_TS_UNIT


class TestDatapoint:
    def test_display_str_no_timezone(self) -> None:
        dp = Datapoint(timestamp=1716589737000, value="foo", average=123)
        assert "timezone" not in str(dp)
        assert '"timestamp": "2024-05-24 22:28:57.000+00:00"' in str(dp)
        dp.timezone = None
        assert "timezone" not in str(dp)
        assert '"timestamp": "2024-05-24 22:28:57.000+00:00"' in str(dp)

    def test_display_str_with_builtin_timezone(self) -> None:
        epoch_ms = 1716589737000
        dp = Datapoint(timestamp=epoch_ms, value="foo", average=123)
        dp.timezone = timezone(timedelta(hours=2))
        assert "timezone" in str(dp)
        assert '"timestamp": "2024-05-25 00:28:57.000+02:00"' in str(dp)

        # Timezone is only a setting for how to display the timestamp:
        dp.timezone = timezone(timedelta(hours=-2))
        assert '"timestamp": "2024-05-24 20:28:57.000-02:00"' in str(dp)
        assert dp.timestamp == epoch_ms

    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "epoch_ms, offset_hours, zone, expected",
        (
            (1716589737000, 2, "Europe/Oslo", "2024-05-25 00:28:57.000+02:00"),
            (1616589737000, 1, "Europe/Oslo", "2021-03-24 13:42:17.000+01:00"),
        ),
    )
    def test_display_str_and_to_pandas_with_timezone_and_zoneinfo(
        self, epoch_ms: int, offset_hours: int, zone: str, expected: str
    ) -> None:
        import pandas as pd

        dp1 = Datapoint(timestamp=epoch_ms, value="foo", average=123)
        dp2 = Datapoint(timestamp=epoch_ms, value="foo", average=123)
        dp1.timezone = ZoneInfo(zone)
        dp2.timezone = timezone(timedelta(hours=offset_hours))
        sdp1, sdp2 = str(dp1), str(dp2)

        assert sdp1 != sdp2
        assert sdp1.replace("Europe/Oslo", "") == sdp2.replace(f"UTC+0{offset_hours}:00", "")
        assert f'"timestamp": "{expected}"' in sdp1

        df1, df2 = dp1.to_pandas(), dp2.to_pandas()
        assert 1 == len(df1.index) == len(df2.index)
        assert pd.Timestamp(expected) == df1.index[0] == df2.index[0]


@pytest.mark.dsl
class TestDatapointsArray:
    def test_dump_converts_missing_values_to_none(self) -> None:
        # Easy to forget that we can have bad data (missing) without any status codes on the object
        import numpy as np

        params: dict = dict(
            id=123,
            is_string=False,
            is_step=False,
            type="numeric",
            timestamp=np.array([1, 2, 3], dtype=np.int64),
            value=np.array([-1, None, 2.5], dtype=np.float64),
        )
        dps1 = DatapointsArray(**params).dump()
        dps2 = DatapointsArray(**params, null_timestamps={2}).dump()
        assert dps1 != dps2
        assert math.isnan(dps1["datapoints"][1]["value"])
        assert dps2["datapoints"][1]["value"] is None


@pytest.mark.dsl
class TestToPandas:
    @pytest.mark.parametrize("dps_lst_cls", [DatapointsList, DatapointsArrayList])
    def test_identifier_priority(self, dps_lst_cls: type[CogniteResourceList]) -> None:
        import numpy as np
        import pandas as pd

        ts = [1234] if dps_lst_cls is DatapointsList else np.array([1234 * 1_000_000], dtype="datetime64[ns]")
        dps_cls = dps_lst_cls._RESOURCE
        df = dps_lst_cls(
            [
                dps_cls(timestamp=ts, value=[2.0], id=123, is_string=False, is_step=False, type="numeric"),
                dps_cls(
                    timestamp=ts, value=[4.0], id=456, external_id="foo", is_string=False, is_step=False, type="numeric"
                ),
                dps_cls(
                    timestamp=ts,
                    value=[6.0],
                    id=789,
                    instance_id=NodeId("s", "x"),
                    is_string=False,
                    is_step=False,
                    type="numeric",
                ),
            ]
        ).to_pandas()

        exp_ts = 1234 * 1_000_000 if PANDAS_TS_UNIT == "ns" else 1234
        exp_df = pd.DataFrame(
            {1: 2.0, 2: 4.0, 3: 6.0},
            index=np.array([exp_ts], dtype=f"datetime64[{PANDAS_TS_UNIT}]"),
        )
        exp_df.columns = pd.Index([123, "foo", NodeId(space="s", external_id="x")], name="identifier")
        pd.testing.assert_frame_equal(df, exp_df)


class TestStateDatapointWrite:
    @pytest.mark.parametrize(
        "kwargs, expected",
        [
            ({"numeric_value": 5}, {"timestamp": 1, "numericValue": 5}),  # numeric only
            ({"string_value": "on"}, {"timestamp": 1, "stringValue": "on"}),  # string only
            (  # both value fields:
                {"numeric_value": -1, "string_value": "off"},
                {"timestamp": 1, "numericValue": -1, "stringValue": "off"},
            ),
            ({"status_code": 0x80000000}, {"timestamp": 1, "status": {"code": 0x80000000}}),  # code only
            ({"status_symbol": "Bad"}, {"timestamp": 1, "status": {"symbol": "Bad"}}),  # symbol only
            (  # code + symbol:
                {"status_code": 0x80000000, "status_symbol": "Bad"},
                {"timestamp": 1, "status": {"code": 0x80000000, "symbol": "Bad"}},
            ),
        ],
    )
    def test_dump(self, kwargs: dict[str, Any], expected: dict[str, Any]) -> None:
        assert StateDatapointWrite(timestamp=1, **kwargs).dump() == expected

    def test_dump_snake_case(self) -> None:
        dp = StateDatapointWrite(1000, numeric_value=-1, string_value="off", status_code=0, status_symbol="Good")
        assert dp.dump(camel_case=False) == {
            "timestamp": 1000,
            "numeric_value": -1,
            "string_value": "off",
            "status": {"symbol": "Good", "code": 0},
        }

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"numeric_value": 5},
            {"string_value": "idle"},
            {"numeric_value": 0, "string_value": "off"},
            {"status_symbol": "Bad"},
            {"status_code": 0x80000000},
        ],
    )
    def test_load_dump_roundtrip(self, kwargs: dict[str, Any]) -> None:
        obj = StateDatapointWrite(timestamp=1000, **kwargs)
        assert StateDatapointWrite.load(obj.dump(camel_case=False)) == obj

    def test_load_returns_self_if_instance(self) -> None:
        obj = StateDatapointWrite(1000, numeric_value=0)
        assert StateDatapointWrite.load(obj) is obj

    def test_load_raises_on_invalid_type(self) -> None:
        with pytest.raises(TypeError, match="StateDatapointWrite"):
            StateDatapointWrite.load([1000, 0])  # type: ignore[arg-type]

    def test_no_value_and_no_status_raises(self) -> None:
        with pytest.raises(ValueError, match="bad status"):
            StateDatapointWrite(timestamp=1000)

    @pytest.mark.parametrize("kwargs", [{"status_code": 0x80000000}, {"status_symbol": "Bad"}])
    def test_status_only_without_value_is_valid(self, kwargs: dict[str, Any]) -> None:
        dp = StateDatapointWrite(timestamp=1000, **kwargs)
        assert dp.numeric_value is None and dp.string_value is None


@pytest.fixture
def state_dps_insert() -> StateDatapointsInsert:
    return StateDatapointsInsert(
        instance_id=NodeId("my-space", "my-ts"),
        datapoints=[StateDatapointWrite(1000, numeric_value=0)],
    )


class TestStateDatapointsInsert:
    def test_dump_camel_case(self, state_dps_insert: StateDatapointsInsert) -> None:
        assert state_dps_insert.dump() == {
            "instanceId": {"space": "my-space", "externalId": "my-ts"},
            "datapoints": [{"timestamp": 1000, "numericValue": 0}],
        }

    def test_dump_snake_case(self, state_dps_insert: StateDatapointsInsert) -> None:
        assert state_dps_insert.dump(camel_case=False) == {
            "instance_id": {"space": "my-space", "external_id": "my-ts"},
            "datapoints": [{"timestamp": 1000, "numeric_value": 0}],
        }

    def test_load_dump_roundtrip(self, state_dps_insert: StateDatapointsInsert) -> None:
        back = StateDatapointsInsert.load(state_dps_insert.dump())
        assert NodeId.load(back.instance_id) == NodeId("my-space", "my-ts")
        assert len(back.datapoints) == 1

    def test_load_returns_self_if_instance(self, state_dps_insert: StateDatapointsInsert) -> None:
        assert StateDatapointsInsert.load(state_dps_insert) is state_dps_insert

    def test_load_raises_on_invalid_type(self) -> None:
        with pytest.raises(TypeError, match="StateDatapointsInsert"):
            StateDatapointsInsert.load("not-a-dict")  # type: ignore[arg-type]

    def test_to_proto_dict(self, state_dps_insert: StateDatapointsInsert) -> None:
        assert state_dps_insert._to_proto_dict() == {
            "instanceId": {"space": "my-space", "externalId": "my-ts"},
            "stateDatapoints": {"datapoints": [{"timestamp": 1000, "numericValue": 0}]},
        }

    def test_non_sequence_datapoints_raises(self) -> None:
        with pytest.raises(TypeError, match="sequence"):
            StateDatapointsInsert(instance_id=NodeId("sp", "xid"), datapoints="bad")  # type: ignore[arg-type]
