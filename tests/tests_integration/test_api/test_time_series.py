import time
from unittest import mock

import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesUpdate
from cognite.client.exceptions import CogniteAPIError
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def new_ts():
    ts = COGNITE_CLIENT.time_series.create(TimeSeries(name="any"))
    yield ts
    COGNITE_CLIENT.time_series.delete(id=ts.id)
    assert COGNITE_CLIENT.time_series.retrieve(ts.id) is None


@pytest.fixture
def get_spy():
    with mock.patch.object(COGNITE_CLIENT.time_series, "_get", wraps=COGNITE_CLIENT.time_series._get) as _:
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

    def test_list(self, get_spy):
        with set_request_limit(COGNITE_CLIENT.time_series, 10):
            res = COGNITE_CLIENT.time_series.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.time_series._get.call_count

    def test_search(self):
        res = COGNITE_CLIENT.time_series.search(
            name="test__timestamp_multiplied", filter=TimeSeriesFilter(created_time={"min": 0})
        )
        assert len(res) > 0

    def test_update(self, new_ts):
        update_ts = TimeSeriesUpdate(new_ts.id).name.set("newname")
        res = COGNITE_CLIENT.time_series.update(update_ts)
        assert "newname" == res.name
