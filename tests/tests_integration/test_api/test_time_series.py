import time
from datetime import datetime
from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesUpdate
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def new_ts():
    ts = COGNITE_CLIENT.time_series.create(TimeSeries(name="any"))
    yield ts
    COGNITE_CLIENT.time_series.delete(id=ts.id)
    assert COGNITE_CLIENT.time_series.retrieve(ts.id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.time_series, "_post", wraps=COGNITE_CLIENT.time_series._post) as _:
        yield


class TestTimeSeriesAPI:
    def test_retrieve(self):
        listed_asset = COGNITE_CLIENT.time_series.list(limit=1)[0]
        retrieved_asset = COGNITE_CLIENT.time_series.retrieve(listed_asset.id)
        retrieved_asset.external_id = listed_asset.external_id
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self):
        res = COGNITE_CLIENT.time_series.list(limit=2)
        retrieved_assets = COGNITE_CLIENT.time_series.retrieve_multiple([t.id for t in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_retrieve_multiple_ignore_unknown(self):
        res = COGNITE_CLIENT.time_series.list(limit=2)
        retrieved_assets = COGNITE_CLIENT.time_series.retrieve_multiple(
            [t.id for t in res] + [0, 1, 2, 3], ignore_unknown_ids=True
        )
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.time_series, 10):
            res = COGNITE_CLIENT.time_series.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.time_series._post.call_count

    def test_list_with_filters(self, post_spy):
        res = COGNITE_CLIENT.time_series.list(
            is_step=True,
            is_string=False,
            metadata={"a": "b"},
            last_updated_time={"min": 45},
            created_time={"max": 123},
            asset_ids=[1, 2],
            root_asset_ids=[1231],
            include_metadata=False,
        )
        assert 0 == len(res)
        assert 1 == COGNITE_CLIENT.time_series._post.call_count

    def test_partitioned_list(self, post_spy):
        mintime = datetime(2019, 1, 1).timestamp() * 1000
        maxtime = datetime(2019, 5, 15).timestamp() * 1000
        res_flat = COGNITE_CLIENT.time_series.list(limit=None, created_time={"min": mintime, "max": maxtime})
        res_part = COGNITE_CLIENT.time_series.list(
            partitions=8, limit=None, created_time={"min": mintime, "max": maxtime}
        )
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_aggregate(self, new_ts):
        res = COGNITE_CLIENT.time_series.aggregate(filter=TimeSeriesFilter(name="any"))
        assert res[0].count > 0

    def test_search(self):
        res = COGNITE_CLIENT.time_series.search(
            name="test__timestamp_multiplied", filter=TimeSeriesFilter(created_time={"min": 0})
        )
        assert len(res) > 0

    def test_update(self, new_ts):
        update_ts = TimeSeriesUpdate(new_ts.id).name.set("newname")
        res = COGNITE_CLIENT.time_series.update(update_ts)
        assert "newname" == res.name

    def test_delete_with_nonexisting(self):
        a = COGNITE_CLIENT.time_series.create(TimeSeries(name="any"))
        COGNITE_CLIENT.assets.delete(id=a.id, external_id="this ts does not exist", ignore_unknown_ids=True)
        assert COGNITE_CLIENT.assets.retrieve(id=a.id) is None
