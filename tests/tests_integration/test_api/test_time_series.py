from datetime import datetime
from unittest import mock

import pytest

from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesUpdate
from tests.utils import set_request_limit


@pytest.fixture(scope="class")
def new_ts(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries(name="any", metadata={"a": "b"}))
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.time_series, "_post", wraps=cognite_client.time_series._post) as _:
        yield


class TestTimeSeriesAPI:
    def test_retrieve(self, cognite_client):
        listed_asset = cognite_client.time_series.list(limit=1)[0]
        retrieved_asset = cognite_client.time_series.retrieve(listed_asset.id)
        retrieved_asset.external_id = listed_asset.external_id
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self, cognite_client):
        res = cognite_client.time_series.list(limit=2)
        retrieved_assets = cognite_client.time_series.retrieve_multiple([t.id for t in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_retrieve_multiple_ignore_unknown(self, cognite_client):
        res = cognite_client.time_series.list(limit=2)
        retrieved_assets = cognite_client.time_series.retrieve_multiple(
            [t.id for t in res] + [1, 2, 3], ignore_unknown_ids=True
        )
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.time_series, 10):
            res = cognite_client.time_series.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.time_series._post.call_count

    def test_list_with_filters(self, cognite_client, post_spy):
        res = cognite_client.time_series.list(
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
        assert 1 == cognite_client.time_series._post.call_count

    def test_partitioned_list(self, cognite_client, post_spy):
        mintime = datetime(2019, 1, 1).timestamp() * 1000
        maxtime = datetime(2019, 5, 15).timestamp() * 1000
        res_flat = cognite_client.time_series.list(limit=None, created_time={"min": mintime, "max": maxtime})
        res_part = cognite_client.time_series.list(
            partitions=8, limit=None, created_time={"min": mintime, "max": maxtime}
        )
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_aggregate(self, cognite_client, new_ts):
        res = cognite_client.time_series.aggregate(filter=TimeSeriesFilter(name="any"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.time_series.search(
            name="test__timestamp_multiplied", filter=TimeSeriesFilter(created_time={"min": 0})
        )
        assert len(res) > 0

    def test_update(self, cognite_client, new_ts):
        assert new_ts.metadata == {"a": "b"}
        update_ts = TimeSeriesUpdate(new_ts.id).name.set("newname").metadata.set({})
        res = cognite_client.time_series.update(update_ts)
        assert "newname" == res.name
        assert res.metadata == {}

    def test_delete_with_nonexisting(self, cognite_client):
        a = cognite_client.time_series.create(TimeSeries(name="any"))
        cognite_client.assets.delete(id=a.id, external_id="this ts does not exist", ignore_unknown_ids=True)
        assert cognite_client.assets.retrieve(id=a.id) is None
