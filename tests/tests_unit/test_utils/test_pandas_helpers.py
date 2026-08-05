from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils import _pandas_helpers as pdh

PANDAS_V2_V3_UNITS = [(2, "ns"), (3, "ms")]
PANDAS_V2_V3_DTYPES = [(2, "datetime64[ns]"), (3, "datetime64[ms]")]


class _ResourceWithoutListClass(CogniteResource):
    """Minimal resource that forces the fallback resource-to-pandas path."""

    def __init__(self, id: int | None = None, created_time: int | None = None) -> None:
        self.id = id
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(id=resource.get("id"), created_time=resource.get("createdTime"))


@pytest.fixture
def force_pandas_major_version(monkeypatch: MonkeyPatch) -> Callable[[int], None]:
    """Override pandas major version for a test and reset helper caches."""

    def _force(major_version: int) -> None:
        monkeypatch.setattr(pdh, "pandas_major_version", lambda: major_version)
        pdh.is_pandas_v2_or_lower.cache_clear()  # type: ignore [attr-defined]
        pdh.timestamp_dtype_unit.cache_clear()  # type: ignore [attr-defined]

    return _force


@pytest.mark.dsl
class TestTimestampResolution:
    @pytest.mark.parametrize(
        "major_version, expected_unit, is_v2_or_lower",
        [(1, "ns", True), (2, "ns", True), (3, "ms", False), (42, "ms", False)],
    )
    def test_is_pandas_v2_or_lower_and_timestamp_dtype_unit(
        self,
        force_pandas_major_version: Callable[[int], None],
        major_version: int,
        expected_unit: str,
        is_v2_or_lower: bool,
    ) -> None:
        force_pandas_major_version(major_version)
        assert pdh.is_pandas_v2_or_lower() is is_v2_or_lower
        assert pdh.timestamp_dtype_unit() == expected_unit

    @pytest.mark.parametrize("major_version, expected_unit", PANDAS_V2_V3_UNITS)
    @pytest.mark.parametrize("tz, expected_tz", [(None, None), ("UTC", "UTC")])
    def test_to_pandas_timestamp(
        self,
        force_pandas_major_version: Callable[[int], None],
        major_version: int,
        expected_unit: str,
        tz: str | None,
        expected_tz: str | None,
    ) -> None:
        force_pandas_major_version(major_version)

        ts = pdh.to_pandas_timestamp(1234, tz=tz)
        assert ts.unit == expected_unit
        assert (None if ts.tz is None else str(ts.tz)) == expected_tz

    @pytest.mark.parametrize("major_version, expected_dtype", PANDAS_V2_V3_DTYPES)
    def test_convert_timestamp_columns_to_datetime(
        self, force_pandas_major_version: Callable[[int], None], major_version: int, expected_dtype: str
    ) -> None:
        import pandas as pd

        force_pandas_major_version(major_version)

        df = pd.DataFrame({"created_time": [0, 60_000], "last_updated_time": [0, 60_000], "untouched": [1, 2]})
        out = pdh.convert_timestamp_columns_to_datetime(df)

        assert str(out["created_time"].dtype) == expected_dtype
        assert str(out["last_updated_time"].dtype) == expected_dtype
        assert out["created_time"].iloc[1] == pd.Timestamp(60_000, unit="ms")

    @pytest.mark.parametrize("major_version, expected_dtype", PANDAS_V2_V3_DTYPES)
    def test_create_timestamp_index(
        self, force_pandas_major_version: Callable[[int], None], major_version: int, expected_dtype: str
    ) -> None:
        force_pandas_major_version(major_version)

        idx = pdh._create_timestamp_index([0, 1_000, 2_000], timezone=None)
        assert str(idx.dtype) == expected_dtype

    @pytest.mark.parametrize("major_version, expected_unit", PANDAS_V2_V3_UNITS)
    def test_base_resource_to_pandas_fallback(
        self, force_pandas_major_version: Callable[[int], None], major_version: int, expected_unit: str
    ) -> None:
        force_pandas_major_version(major_version)

        assert _ResourceWithoutListClass._LIST_CLASS is None
        resource = _ResourceWithoutListClass(id=1, created_time=60_000)
        df = resource.to_pandas(convert_timestamps=True)

        assert df.at["created_time", "value"].unit == expected_unit

    def test_changing_the_constant_changes_to_pandas_output(
        self, force_pandas_major_version: Callable[[int], None]
    ) -> None:
        """Changing pandas major version should change timestamp dtype in to_pandas output."""
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
