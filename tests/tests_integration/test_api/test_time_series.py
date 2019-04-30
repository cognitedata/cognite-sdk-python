import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesUpdate
from cognite.client.exceptions import CogniteAPIError

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(autouse=True, scope="module")
def set_limit():
    limit_tmp = COGNITE_CLIENT.time_series._LIMIT
    COGNITE_CLIENT.time_series._LIMIT = 10
    yield set_limit
    COGNITE_CLIENT.time_series._LIMIT = limit_tmp


@pytest.fixture
def new_ts():
    ts = COGNITE_CLIENT.time_series.create(TimeSeries(name="any"))
    yield ts
    COGNITE_CLIENT.time_series.delete(id=ts.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.time_series.get(ts.id)
    assert 422 == e.value.code


class TestTimeSeriesAPI:
    def test_get(self):
        res = COGNITE_CLIENT.time_series.list(limit=1)
        assert res[0] == COGNITE_CLIENT.time_series.get(res[0].id)

    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.time_series, "_get")

        res = COGNITE_CLIENT.time_series.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.time_series._get.call_count

    def test_search(self):
        res = COGNITE_CLIENT.time_series.search(
            name="multiplied_by_2", filter=TimeSeriesFilter(created_time={"min": 0})
        )
        assert len(res) > 0

    def test_update(self, new_ts):
        update_ts = TimeSeriesUpdate(new_ts.id).name.set("newname")
        res = COGNITE_CLIENT.time_series.update(update_ts)
        assert "newname" == res.name
