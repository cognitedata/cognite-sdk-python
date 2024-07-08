from __future__ import annotations

import math
from datetime import timedelta, timezone

import pytest

from cognite.client.data_classes import Datapoint, DatapointsArray
from cognite.client.utils._time import ZoneInfo


class TestDatapoint:
    def test_display_str_no_timezone(self):
        dp = Datapoint(timestamp=1716589737000, value="foo", average=123)
        assert "timezone" not in str(dp)
        assert '"timestamp": "2024-05-24 22:28:57.000+00:00"' in str(dp)
        dp.timezone = None
        assert "timezone" not in str(dp)
        assert '"timestamp": "2024-05-24 22:28:57.000+00:00"' in str(dp)

    def test_display_str_with_builtin_timezone(self):
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
    def test_display_str_and_to_pandas_with_timezone_and_zoneinfo(self, epoch_ms, offset_hours, zone, expected):
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


def factory_method_from_array_data():
    try:
        import numpy as np
        import pandas as pd
    except ImportError:
        return []

    index = pd.date_range("2023-01-01", periods=4, freq="1h", tz="UTC").values
    arr1 = DatapointsArray(id=123, average=np.array([1.0, 2.0], dtype=np.float64), timestamp=index[:2])
    arr2 = DatapointsArray(id=123, average=np.array([3.0, 4.0], dtype=np.float64), timestamp=index[2:])
    expected = DatapointsArray(id=123, average=np.array([1, 2, 3, 4], dtype=np.float64), timestamp=index)
    yield pytest.param([arr1, arr2], expected, id="Construct from two arrays")


@pytest.mark.dsl
class TestDatapointsArray:
    @staticmethod
    @pytest.mark.parametrize("arrays, expected_array", list(factory_method_from_array_data()))
    def test_factory_method_from_array(arrays: list[DatapointsArray], expected_array: DatapointsArray):
        import numpy as np

        actual_array = DatapointsArray.create_from_arrays(*arrays)

        np.testing.assert_allclose(actual_array.average, expected_array.average)
        np.testing.assert_equal(actual_array.timestamp, expected_array.timestamp)

    def test_dump_converts_missing_values_to_none(self):
        # Easy to forget that we can have bad data (missing) without any status codes on the object
        import numpy as np

        params = dict(
            id=123,
            timestamp=np.array([1, 2, 3], dtype=np.int64),
            value=np.array([-1, None, 2.5], dtype=np.float64),
        )
        dps1 = DatapointsArray(**params).dump()
        dps2 = DatapointsArray(**params, null_timestamps={2}).dump()
        assert dps1 != dps2
        assert math.isnan(dps1["datapoints"][1]["value"])
        assert dps2["datapoints"][1]["value"] is None
