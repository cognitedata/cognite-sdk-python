from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils import _pandas_helpers as pdh

ForcePandasMajorVersion = Callable[[int], None]


class _ResourceWithoutListClass(CogniteResource):
    """A minimal resource with no `_LIST_CLASS` counterpart, so `to_pandas()` is forced through
    `base_resource_to_pandas_fallback` instead of delegating to a list class. Note: we need at least
    one non-timestamp attribute too, or the resulting Series/DataFrame ends up purely `datetime64`-typed
    and pandas silently reverts to its own default resolution instead of respecting our chosen unit
    (this doesn't happen for real SDK resources, which always have several non-timestamp attributes)."""

    def __init__(self, id: int | None = None, created_time: int | None = None) -> None:
        self.id = id
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(id=resource.get("id"), created_time=resource.get("createdTime"))


@pytest.fixture
def force_pandas_major_version(monkeypatch: MonkeyPatch) -> ForcePandasMajorVersion:
    """Lets a test pretend to run on an arbitrary pandas major version, regardless of what's actually
    installed. This is what lets us exercise both the v2 (nanosecond) and v3 (millisecond) branches of
    the SDK's timestamp-resolution logic in the same test run/environment.
    """

    def _force(major_version: int) -> None:
        monkeypatch.setattr(pdh, "pandas_major_version", lambda: major_version)

    return _force


@pytest.mark.dsl
class TestTimestampResolution:
    @pytest.mark.parametrize("major_version, expected_unit", [(1, "ns"), (2, "ns"), (3, "ms"), (4, "ms")])
    def test_is_pandas_v2_or_lower_and_timestamp_dtype_unit(
        self, force_pandas_major_version: ForcePandasMajorVersion, major_version: int, expected_unit: str
    ) -> None:
        force_pandas_major_version(major_version)
        assert pdh.is_pandas_v2_or_lower() is (major_version < 3)
        assert pdh.timestamp_dtype_unit() == expected_unit

    @pytest.mark.parametrize("major_version, expected_unit", [(2, "ns"), (3, "ms")])
    def test_to_pandas_timestamp(
        self, force_pandas_major_version: ForcePandasMajorVersion, major_version: int, expected_unit: str
    ) -> None:
        force_pandas_major_version(major_version)

        ts = pdh.to_pandas_timestamp(1234)
        assert ts.unit == expected_unit
        assert ts.tz is None

        ts_tz = pdh.to_pandas_timestamp(1234, tz="UTC")
        assert ts_tz.unit == expected_unit
        assert str(ts_tz.tz) == "UTC"

    @pytest.mark.parametrize("major_version, expected_dtype", [(2, "datetime64[ns]"), (3, "datetime64[ms]")])
    def test_convert_timestamp_columns_to_datetime(
        self, force_pandas_major_version: ForcePandasMajorVersion, major_version: int, expected_dtype: str
    ) -> None:
        import pandas as pd

        force_pandas_major_version(major_version)

        df = pd.DataFrame({"created_time": [0, 60_000], "last_updated_time": [0, 60_000], "untouched": [1, 2]})
        out = pdh.convert_timestamp_columns_to_datetime(df)

        assert str(out["created_time"].dtype) == expected_dtype
        assert str(out["last_updated_time"].dtype) == expected_dtype
        assert out["untouched"].dtype != "datetime64"
        # Regardless of resolution, the represented instant must be correct:
        assert out["created_time"].iloc[1] == pd.Timestamp(60_000, unit="ms")

    @pytest.mark.parametrize("major_version, expected_dtype", [(2, "datetime64[ns]"), (3, "datetime64[ms]")])
    def test_create_timestamp_index(
        self, force_pandas_major_version: ForcePandasMajorVersion, major_version: int, expected_dtype: str
    ) -> None:
        force_pandas_major_version(major_version)

        idx = pdh._create_timestamp_index([0, 1_000, 2_000], timezone=None)
        assert str(idx.dtype) == expected_dtype

    @pytest.mark.parametrize("major_version, expected_unit", [(2, "ns"), (3, "ms")])
    def test_base_resource_to_pandas_fallback(
        self, force_pandas_major_version: ForcePandasMajorVersion, major_version: int, expected_unit: str
    ) -> None:
        force_pandas_major_version(major_version)

        # No _LIST_CLASS is set, so this exercises `base_resource_to_pandas_fallback` specifically
        # (as opposed to the more common "squeeze a single-row list dataframe" path):
        assert _ResourceWithoutListClass._LIST_CLASS is None
        resource = _ResourceWithoutListClass(id=1, created_time=60_000)
        df = resource.to_pandas(convert_timestamps=True)

        assert df.at["created_time", "value"].unit == expected_unit

    def test_changing_the_constant_changes_to_pandas_output(
        self, force_pandas_major_version: ForcePandasMajorVersion
    ) -> None:
        """End-to-end check that flipping `pandas_major_version` actually changes what a real
        `to_pandas()` call produces, not just the internal helper functions."""
        from cognite.client.data_classes import Asset, AssetList

        assets = AssetList(
            [Asset(id=1, external_id="foo", root_id=1, name="foo", created_time=0, last_updated_time=60_000)]
        )

        force_pandas_major_version(2)
        df_v2 = assets.to_pandas()
        force_pandas_major_version(3)
        df_v3 = assets.to_pandas()

        assert str(df_v2["last_updated_time"].dtype) == "datetime64[ns]"
        assert str(df_v3["last_updated_time"].dtype) == "datetime64[ms]"
        assert df_v2["last_updated_time"].dtype != df_v3["last_updated_time"].dtype
