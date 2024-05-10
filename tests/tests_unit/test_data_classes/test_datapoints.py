from __future__ import annotations

import math

import pytest

from cognite.client.data_classes import DatapointsArray


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
