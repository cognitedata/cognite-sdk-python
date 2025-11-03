from __future__ import annotations

import math
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from cognite.client.data_classes import Datapoint, DatapointsArray
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import DatapointsArrayList, DatapointsList


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
                dps_cls(timestamp=ts, value=[2.0], id=123, is_string=False),
                dps_cls(timestamp=ts, value=[4.0], external_id="foo", is_string=False),
                dps_cls(timestamp=ts, value=[6.0], instance_id=NodeId("s", "x"), is_string=False),
            ]
        ).to_pandas()

        exp_df = pd.DataFrame(
            {1: 2.0, 2: 4.0, 3: 6.0},
            index=np.array([1234 * 1_000_000], dtype="datetime64[ns]"),
        )
        exp_df.columns = pd.MultiIndex.from_tuples(
            [(123,), ("foo",), (NodeId(space="s", external_id="x"),)], names=["identifier"]
        )
        pd.testing.assert_frame_equal(df, exp_df)
