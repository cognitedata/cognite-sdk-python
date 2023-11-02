from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes import Group, GroupList


def raw_groups():
    yield {
        "name": "entitymatching",
        "sourceId": "3c5dd595-1234-1234-1234-8db813d2c232",
        "capabilities": [{"entitymatchingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}}],
        "id": 1234,
        "isDeleted": False,
        "deletedTime": -1,
        "metadata": {"k": "v"},
    }
    yield {
        "name": "a deleted group without capabilities",
        "id": 2345,
        "isDeleted": True,
        "deletedTime": 123456789,
    }
    yield {
        "name": "unknown group",
        "id": 3456,
        "isDeleted": False,
        "capabilities": [
            {
                "relationshipsAcl": {
                    "scope": {"datasetScope": {"ids": ["2332579319524236"]}},
                    "actions": ["READ", "WRITE"],
                }
            },
        ],
    }


class TestGroups:
    @pytest.mark.parametrize("raw", list(raw_groups()))
    def test_load_dump_unknown_group(self, raw: dict[str, Any]) -> None:
        group = Group.load(raw)
        assert group.dump(camel_case=True) == raw

    @pytest.mark.dsl
    def test_to_pandas__deleted_time(self):
        import pandas as pd

        group = Group.load(next(raw_groups()))
        assert group.to_pandas().at["deleted_time", "value"] is pd.NaT
        assert group.to_pandas(convert_timestamps=False).at["deleted_time", "value"] == -1


class TestGroupsList:
    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "convert_timestamps, expected",
        (
            (True, dict(data=[None, "1970-01-02 10:17:36.789", None], dtype="datetime64[ns]", name="deleted_time")),
            (False, dict(data=[-1, 123456789, None], dtype="Int64", name="deleted_time")),
        ),
    )
    def test_to_pandas__deleted_time(self, expected, convert_timestamps):
        import pandas as pd

        groups = GroupList.load(list(raw_groups()))
        df_ts = groups.to_pandas(convert_timestamps=convert_timestamps)
        expected = pd.Series(**expected)
        pd.testing.assert_series_equal(df_ts["deleted_time"], expected)
