from __future__ import annotations

import pytest

from cognite.client.data_classes import DatapointsArray


def factory_method_from_array_data():
    try:
        import numpy as np
        import pandas as pd
    except ModuleNotFoundError:
        return []
    index = pd.date_range("2023-01-01", periods=4, freq="1H", tz="UTC").values
    arr1 = DatapointsArray(id=123, average=np.array([1.0, 2.0], dtype=np.float64), timestamp=index[:2])
    arr2 = DatapointsArray(id=123, average=np.array([3.0, 4.0], dtype=np.float64), timestamp=index[2:])
    expected = DatapointsArray(id=123, average=np.array([1, 2, 3, 4], dtype=np.float64), timestamp=index)
    yield pytest.param([arr1, arr2], expected, id="Construct from two arrays")


class TestDatapointsArray:
    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize("arrays, expected_array", list(factory_method_from_array_data()))
    def test_factory_method_from_array(arrays: list[DatapointsArray], expected_array: DatapointsArray):
        import numpy as np

        actual_array = DatapointsArray.create_from_arrays(*arrays)

        np.testing.assert_allclose(actual_array.average, expected_array.average)
        # Comparing timestamps directly leads to an incorrect assertion fail.
        np.testing.assert_allclose(actual_array.timestamp.astype("long"), expected_array.timestamp.astype("long"))
