import json
import math
from contextlib import contextmanager
from datetime import datetime
from random import choice, random
from unittest import mock
from unittest.mock import PropertyMock

import pytest

from cognite.client import CogniteClient
from cognite.client._api.datapoints import _DatapointsFetcher, _DPQuery, _DPWindow
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsList, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import _utils as utils
from tests.utils import jsgz_load, set_request_limit

COGNITE_CLIENT = CogniteClient()
DPS_CLIENT = COGNITE_CLIENT.datapoints


def generate_datapoints(start: int, end: int, aggregates=None, granularity=None):
    dps = []
    granularity = utils.granularity_to_ms(granularity) if granularity else 1000
    for i in range(start, end, granularity):
        dp = {}
        if aggregates:
            if aggregates == ["count"]:
                dp["count"] = int(math.ceil((end - start) / 1000))
            else:
                for agg in aggregates:
                    dp[agg] = random()
        else:
            dp["value"] = random()
        dp["timestamp"] = i
        dps.append(dp)
    return dps


@pytest.fixture
def mock_get_datapoints(rsps):
    def request_callback(request):
        payload = jsgz_load(request.body)

        items = []
        for dps_query in payload["items"]:
            aggregates = []

            if "aggregates" in dps_query:
                aggregates = dps_query["aggregates"]
            elif "aggregates" in payload:
                aggregates = payload["aggregates"]

            granularity = None
            if "granularity" in dps_query:
                granularity = dps_query["granularity"]
            elif "granularity" in payload:
                granularity = payload["granularity"]

            if (granularity and not aggregates) or (not granularity and aggregates):
                return (
                    400,
                    {},
                    json.dumps({"error": {"code": 400, "message": "You must specify both aggregates AND granularity"}}),
                )

            if "start" in dps_query and "end" in dps_query:
                start, end = dps_query["start"], dps_query["end"]
            else:
                start, end = payload["start"], payload["end"]

            limit = 100000
            if "limit" in dps_query:
                limit = dps_query["limit"]
            elif "limit" in payload:
                limit = payload["limit"]

            dps = generate_datapoints(start, end, aggregates, granularity)
            dps = dps[:limit]
            id_to_return = dps_query.get("id", int(dps_query.get("externalId", "-1")))
            external_id_to_return = dps_query.get("externalId", str(dps_query.get("id", -1)))
            items.append({"id": id_to_return, "externalId": external_id_to_return, "datapoints": dps})
        response = {"items": items}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_empty(rsps):
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        status=200,
        json={"items": [{"id": 1, "externalId": "1", "datapoints": []}]},
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_one_ts_empty(rsps):
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        status=200,
        json={"items": [{"id": 1, "externalId": "1", "datapoints": [{"timestamp": 1, "value": 1}]}]},
    )
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        status=200,
        json={"items": [{"id": 2, "externalId": "2", "datapoints": []}]},
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_one_ts_has_missing_aggregates(rsps):
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "abc",
                    "datapoints": [
                        {"timestamp": 0, "average": 0},
                        {"timestamp": 1, "average": 1},
                        {"timestamp": 2, "average": 2},
                        {"timestamp": 3, "average": 3},
                        {"timestamp": 4, "average": 4},
                    ],
                }
            ]
        },
    )
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 2,
                    "externalId": "def",
                    "datapoints": [
                        {"timestamp": 0},
                        {"timestamp": 1, "interpolation": 1},
                        {"timestamp": 2},
                        {"timestamp": 3, "interpolation": 3},
                        {"timestamp": 4},
                    ],
                }
            ]
        },
    )
    yield rsps


@pytest.fixture
def set_dps_workers():
    def set_workers(limit):
        DPS_CLIENT._max_workers = limit

    workers_tmp = DPS_CLIENT._max_workers
    yield set_workers
    DPS_CLIENT._max_workers = workers_tmp


def assert_dps_response_is_correct(calls, dps_object):
    datapoints = []
    for call in calls:
        if jsgz_load(call.request.body)["limit"] > 1 and jsgz_load(call.request.body).get("aggregates") != ["count"]:
            dps_response = call.response.json()["items"][0]
            if dps_response["id"] == dps_object.id and dps_response["externalId"] == dps_object.external_id:
                datapoints.extend(dps_response["datapoints"])
                id = dps_response["id"]
                external_id = dps_response["externalId"]

    expected_dps = sorted(datapoints, key=lambda x: x["timestamp"])
    assert id == dps_object.id
    assert external_id == dps_object.external_id
    assert expected_dps == dps_object.dump(camel_case=True)["datapoints"]


class TestGetDatapoints:
    def test_retrieve_datapoints_by_id(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.retrieve(id=123, start=1000000, end=1100000)
        assert isinstance(dps_res, Datapoints)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_500(self, rsps):
        rsps.add(
            rsps.POST,
            DPS_CLIENT._base_url + "/timeseries/data/list",
            json={"error": {"code": 500, "message": "Internal Server Error"}},
            status=500,
        )
        with pytest.raises(CogniteAPIError):
            DPS_CLIENT.retrieve(id=123, start=1000000, end=1100000)

    def test_retrieve_datapoints_by_external_id(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.retrieve(external_id="123", start=1000000, end=1100000)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_aggregates(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.retrieve(
            id=123, start=1000000, end=1100000, aggregates=["average", "stepInterpolation"], granularity="10s"
        )
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_local_aggregates(self, mock_get_datapoints):
        dps_res_list = DPS_CLIENT.retrieve(
            external_id={"externalId": "123", "aggregates": ["average"]},
            id={"id": 234},
            start=1000000,
            end=1100000,
            aggregates=["max"],
            granularity="10s",
        )
        for dps_res in dps_res_list:
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_some_aggregates_omitted(self, mock_get_datapoints_one_ts_has_missing_aggregates):
        dps_res_list = DPS_CLIENT.retrieve(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            aggregates=[],
            granularity="1s",
        )
        for dps in dps_res_list:
            if dps.id == 1:
                assert dps.average == [0, 1, 2, 3, 4]
            elif dps.id == 2:
                assert dps.interpolation == [None, 1, None, 3, None]

    def test_datapoints_paging(self, mock_get_datapoints, set_dps_workers):
        set_dps_workers(1)
        with set_request_limit(DPS_CLIENT, 2):
            dps_res = DPS_CLIENT.retrieve(id=123, start=0, end=10000, aggregates=["average"], granularity="1s")
        assert 6 == len(mock_get_datapoints.calls)
        assert 10 == len(dps_res)

    def test_datapoints_concurrent(self, mock_get_datapoints):
        DPS_CLIENT._DPS_LIMIT_AGG = 20
        dps_res = DPS_CLIENT.retrieve(id=123, start=0, end=100000, aggregates=["average"], granularity="1s")
        requested_windows = sorted(
            [
                (jsgz_load(call.request.body)["start"], jsgz_load(call.request.body)["end"])
                for call in mock_get_datapoints.calls
            ],
            key=lambda x: x[0],
        )
        assert (0, 100000) == requested_windows[0]
        assert [(20000, 100000), (40000, 100000), (60000, 100000), (80000, 100000)] == requested_windows[2:]
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_datapoints_paging_with_limit(self, mock_get_datapoints):
        with set_request_limit(DPS_CLIENT, 3):
            dps_res = DPS_CLIENT.retrieve(id=123, start=0, end=10000, aggregates=["average"], granularity="1s", limit=4)
        assert 4 == len(dps_res)

    def test_retrieve_datapoints_multiple_time_series(self, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = DPS_CLIENT.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
        assert isinstance(dps_res_list, DatapointsList), type(dps_res_list)
        for dps_res in dps_res_list:
            if dps_res.id in ids:
                ids.remove(dps_res.id)
            if dps_res.external_id in external_ids:
                external_ids.remove(dps_res.external_id)
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)
        assert 0 == len(ids)
        assert 0 == len(external_ids)

    def test_retrieve_datapoints_empty(self, mock_get_datapoints_empty):
        res = DPS_CLIENT.retrieve(id=1, start=0, end=10000)
        assert 0 == len(res)


class TestQueryDatapoints:
    def test_query_single(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.query(query=DatapointsQuery(id=1, start=0, end=10000))
        assert isinstance(dps_res, Datapoints)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_query_multiple(self, mock_get_datapoints):
        dps_res_list = DPS_CLIENT.query(
            query=[
                DatapointsQuery(id=1, start=0, end=10000),
                DatapointsQuery(external_id="2", start=10000, end=20000, aggregates=["average"], granularity="2s"),
            ]
        )
        assert isinstance(dps_res_list, DatapointsList)
        for dps_res in dps_res_list:
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_query_empty(self, mock_get_datapoints_empty):
        dps_res = DPS_CLIENT.query(query=DatapointsQuery(id=1, start=0, end=10000))
        assert 0 == len(dps_res)


@pytest.fixture
def mock_retrieve_latest(rsps):
    def request_callback(request):
        payload = jsgz_load(request.body)

        items = []
        for latest_query in payload["items"]:
            id = latest_query.get("id", -1)
            external_id = latest_query.get("externalId", "-1")
            before = latest_query.get("before", 10001)
            items.append(
                {"id": id, "externalId": external_id, "datapoints": [{"timestamp": before - 1, "value": random()}]}
            )
        return 200, {}, json.dumps({"items": items})

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/latest",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_retrieve_latest_empty(rsps):
    rsps.add(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/latest",
        status=200,
        json={
            "items": [{"id": 1, "externalId": "1", "datapoints": []}, {"id": 2, "externalId": "2", "datapoints": []}]
        },
    )
    yield rsps


class TestGetLatest:
    def test_retrieve_latest(self, mock_retrieve_latest):
        res = DPS_CLIENT.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 10000 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts(self, mock_retrieve_latest):
        res = DPS_CLIENT.retrieve_latest(id=1, external_id="2")
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 10000 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_with_before(self, mock_retrieve_latest):
        res = DPS_CLIENT.retrieve_latest(id=1, before=10)
        assert isinstance(res, Datapoints)
        assert 9 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts_with_before(self, mock_retrieve_latest):
        res = DPS_CLIENT.retrieve_latest(id=[1, 2], external_id=["1", "2"], before=10)
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 9 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_empty(self, mock_retrieve_latest_empty):
        res = DPS_CLIENT.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 0 == len(res)

    def test_retrieve_latest_multiple_ts_empty(self, mock_retrieve_latest_empty):
        res_list = DPS_CLIENT.retrieve_latest(id=[1, 2])
        assert isinstance(res_list, DatapointsList)
        assert 2 == len(res_list)
        for res in res_list:
            assert 0 == len(res)


@pytest.fixture
def mock_post_datapoints(rsps):
    rsps.add(rsps.POST, DPS_CLIENT._base_url + "/timeseries/data", status=200, json={})
    yield rsps


class TestInsertDatapoints:
    def test_insert_tuples(self, mock_post_datapoints):
        dps = [(i * 1e10, i) for i in range(1, 11)]
        res = DPS_CLIENT.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_insert_dicts(self, mock_post_datapoints):
        dps = [{"timestamp": i * 1e10, "value": i} for i in range(1, 11)]
        res = DPS_CLIENT.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_by_external_id(self, mock_post_datapoints):
        dps = [(i * 1e10, i) for i in range(1, 11)]
        DPS_CLIENT.insert(dps, external_id="1")
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 11)]}
            ]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_insert_datapoints_in_jan_1970(self):
        dps = [{"timestamp": i, "value": i} for i in range(1, 11)]
        with pytest.raises(AssertionError):
            DPS_CLIENT.insert(dps, id=1)

    @pytest.mark.parametrize("ts_key, value_key", [("timestamp", "values"), ("timstamp", "value")])
    def test_invalid_datapoints_keys(self, ts_key, value_key):
        dps = [{ts_key: i * 1e10, value_key: i} for i in range(1, 11)]
        with pytest.raises(AssertionError, match="is missing the"):
            DPS_CLIENT.insert(dps, id=1)

    def test_insert_datapoints_over_limit(self, mock_post_datapoints):
        dps = [(i * 1e10, i) for i in range(1, 11)]
        with set_request_limit(DPS_CLIENT, 5):
            res = DPS_CLIENT.insert(dps, id=1)
        assert res is None
        request_bodies = [jsgz_load(call.request.body) for call in mock_post_datapoints.calls]
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 6)]}]
        } in request_bodies
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(6, 11)]}]
        } in request_bodies

    def test_insert_datapoints_no_data(self):
        with pytest.raises(AssertionError, match="No datapoints provided"):
            DPS_CLIENT.insert(id=1, datapoints=[])

    def test_insert_datapoints_in_multiple_time_series(self, mock_post_datapoints):
        dps = [{"timestamp": i * 1e10, "value": i} for i in range(1, 11)]
        dps_objects = [{"externalId": "1", "datapoints": dps}, {"id": 1, "datapoints": dps}]
        res = DPS_CLIENT.insert_multiple(dps_objects)
        assert res is None
        request_bodies = [jsgz_load(call.request.body) for call in mock_post_datapoints.calls]
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 11)]}]
        } in request_bodies
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e10), "value": i} for i in range(1, 11)]}
            ]
        } in request_bodies

    def test_insert_datapoints_in_multiple_time_series_invalid_key(self):
        dps = [{"timestamp": i * 1e10, "value": i} for i in range(1, 11)]
        dps_objects = [{"extId": "1", "datapoints": dps}]
        with pytest.raises(AssertionError, match="Invalid key 'extId'"):
            DPS_CLIENT.insert_multiple(dps_objects)


@pytest.fixture
def mock_delete_datapoints(rsps):
    rsps.add(rsps.POST, DPS_CLIENT._base_url + "/timeseries/data/delete", status=200, json={})
    yield rsps


class TestDeleteDatapoints:
    def test_delete_range(self, mock_delete_datapoints):
        res = DPS_CLIENT.delete_range(start=datetime(2018, 1, 1), end=datetime(2018, 1, 2), id=1)
        assert res is None
        assert {"items": [{"id": 1, "inclusiveBegin": 1514764800000, "exclusiveEnd": 1514851200000}]} == jsgz_load(
            mock_delete_datapoints.calls[0].request.body
        )

    @pytest.mark.parametrize(
        "id, external_id, exception",
        [(None, None, AssertionError), (1, "1", AssertionError), ("1", None, TypeError), (None, 1, TypeError)],
    )
    def test_delete_range_invalid_id(self, id, external_id, exception):
        with pytest.raises(exception):
            DPS_CLIENT.delete_range("1d-ago", "now", id, external_id)

    def test_delete_range_start_after_end(self):
        with pytest.raises(AssertionError, match="must be"):
            DPS_CLIENT.delete_range(1, 0, 1)

    def test_delete_ranges(self, mock_delete_datapoints):
        ranges = [{"id": 1, "start": 0, "end": 1}, {"externalId": "1", "start": 0, "end": 1}]
        DPS_CLIENT.delete_ranges(ranges)
        assert {
            "items": [
                {"id": 1, "inclusiveBegin": 0, "exclusiveEnd": 1},
                {"externalId": "1", "inclusiveBegin": 0, "exclusiveEnd": 1},
            ]
        } == jsgz_load(mock_delete_datapoints.calls[0].request.body)

    def test_delete_ranges_invalid_ids(self):
        ranges = [{"idz": 1, "start": 0, "end": 1}]
        with pytest.raises(AssertionError, match="Invalid key 'idz'"):
            DPS_CLIENT.delete_ranges(ranges)
        ranges = [{"start": 0, "end": 1}]
        with pytest.raises(AssertionError, match="Exactly one of id and external id must be specified"):
            DPS_CLIENT.delete_ranges(ranges)


class TestDatapointsObject:
    def test_len(self):
        assert 3 == len(Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3]))

    def test_get_non_empty_data_fields(self):
        assert sorted([("timestamp", [1, 2, 3]), ("value", [1, 2, 3])]) == sorted(
            Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])._get_non_empty_data_fields()
        )
        assert sorted([("timestamp", [1, 2, 3]), ("max", [1, 2, 3]), ("sum", [1, 2, 3])]) == sorted(
            Datapoints(id=1, timestamp=[1, 2, 3], sum=[1, 2, 3], max=[1, 2, 3])._get_non_empty_data_fields()
        )
        assert sorted([("timestamp", [1, 2, 3]), ("max", [1, 2, 3])]) == sorted(
            Datapoints(id=1, timestamp=[1, 2, 3], sum=[], max=[1, 2, 3])._get_non_empty_data_fields()
        )
        assert sorted([("timestamp", [1, 2, 3]), ("max", [1, 2, 3]), ("sum", [])]) == sorted(
            Datapoints(id=1, timestamp=[1, 2, 3], sum=[], max=[1, 2, 3])._get_non_empty_data_fields(
                get_empty_lists=True
            )
        )
        assert [("timestamp", [])] == list(Datapoints(id=1)._get_non_empty_data_fields())

    def test_iter(self):
        for dp in Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3]):
            assert dp.timestamp in [1, 2, 3]
            assert dp.value in [1, 2, 3]

    def test_eq(self):
        assert Datapoints(1) == Datapoints(1)
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) == Datapoints(
            1, timestamp=[1, 2, 3], value=[1, 2, 3]
        )
        assert Datapoints(1) != Datapoints(0)
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) != Datapoints(1, timestamp=[1, 2, 3], max=[1, 2, 3])
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) != Datapoints(
            1, timestamp=[1, 2, 3], value=[1, 2, 4]
        )

    def test_get_item(self):
        dps = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])

        assert Datapoint(timestamp=1, value=1) == dps[0]
        assert Datapoint(timestamp=2, value=2) == dps[1]
        assert Datapoint(timestamp=3, value=3) == dps[2]
        assert Datapoints(id=1, timestamp=[1, 2], value=[1, 2]) == dps[:2]

    def test_load(self):
        res = Datapoints._load(
            {"id": 1, "externalId": "1", "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}]}
        )
        assert 1 == res.id
        assert "1" == res.external_id
        assert [1, 2] == res.timestamp
        assert [1, 2] == res.value

    def test_slice(self):
        res = Datapoints(id=1, timestamp=[1, 2, 3])._slice(slice(None, 1))
        assert [1] == res.timestamp

    def test_insert(self):
        d0 = Datapoints()
        d1 = Datapoints(id=1, external_id="1", timestamp=[7, 8, 9], value=[7, 8, 9])
        d2 = Datapoints(id=1, external_id="1", timestamp=[1, 2, 3], value=[1, 2, 3])
        d3 = Datapoints(id=1, external_id="1", timestamp=[4, 5, 6], value=[4, 5, 6])

        d0._insert(d1)
        assert [7, 8, 9] == d0.timestamp
        assert [7, 8, 9] == d0.value
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum == None

        d0._insert(d2)
        assert [1, 2, 3, 7, 8, 9] == d0.timestamp
        assert [1, 2, 3, 7, 8, 9] == d0.value
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum == None

        d0._insert(d3)
        assert [1, 2, 3, 4, 5, 6, 7, 8, 9] == d0.timestamp
        assert [1, 2, 3, 4, 5, 6, 7, 8, 9] == d0.value
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum == None


@pytest.mark.dsl
class TestPlotDatapoints:
    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.plot")
    def test_plot_datapoints(self, pandas_plot_mock, plt_show_mock):
        d = Datapoints(id=1, timestamp=[1, 2, 3, 4, 5], value=[1, 2, 3, 4, 5])
        d.plot()
        assert 1 == pandas_plot_mock.call_count
        assert 1 == plt_show_mock.call_count

    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.plot")
    def test_plot_datapoints_list(self, pandas_plot_mock, plt_show_mock):
        d1 = Datapoints(id=1, timestamp=[1, 2, 3, 4, 5], value=[1, 2, 3, 4, 5])
        d2 = Datapoints(id=2, timestamp=[1, 2, 3, 4, 5], value=[6, 7, 8, 9, 10])
        d = DatapointsList([d1, d2])
        d.plot()
        assert 1 == pandas_plot_mock.call_count
        assert 1 == plt_show_mock.call_count


@pytest.mark.dsl
class TestPandasIntegration:
    def test_datapoint(self):
        import pandas as pd

        d = Datapoint(timestamp=0, value=2, max=3)
        expected_df = pd.DataFrame({"value": [2], "max": [3]}, index=[utils.ms_to_datetime(0)])
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(), check_like=True)

    def test_datapoints(self):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"1|average": [2, 3, 4], "1|stepInterpolation": [3, 4, 5]},
            index=[utils.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_id_and_external_id_set_gives_external_id_columns(self):
        import pandas as pd

        d = Datapoints(id=0, external_id="abc", timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"abc|average": [2, 3, 4], "abc|stepInterpolation": [3, 4, 5]},
            index=[utils.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_empty(self):
        d = Datapoints(external_id="1", timestamp=[], value=[])
        assert d.to_pandas().empty

    def test_datapoints_list(self):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        d2 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4], step_interpolation=[3, 4, 5])
        d3 = Datapoints(id=3, timestamp=[1, 3], value=[1, 3])
        dps_list = DatapointsList([d1, d2, d3])
        expected_df = pd.DataFrame(
            {
                "1|average": [2, 3, 4],
                "1|stepInterpolation": [3, 4, 5],
                "2|max": [2, 3, 4],
                "2|stepInterpolation": [3, 4, 5],
                "3": [1, None, 3],
            },
            index=[utils.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())

    def test_datapoints_list_non_aligned(self):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])
        d2 = Datapoints(id=2, timestamp=[3, 4, 5], value=[3, 4, 5])

        dps_list = DatapointsList([d1, d2])

        expected_df = pd.DataFrame(
            {"1": [1, 2, 3, None, None], "2": [None, None, 3, 4, 5]},
            index=[utils.ms_to_datetime(ms) for ms in [1, 2, 3, 4, 5]],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())

    def test_datapoints_list_empty(self):
        dps_list = DatapointsList([])
        assert dps_list.to_pandas().empty

    def test_retrieve_dataframe(self, mock_get_datapoints):
        df = DPS_CLIENT.retrieve_dataframe(
            id=[1, {"id": 2, "aggregates": ["max"]}],
            external_id=["123"],
            start=1000000,
            end=1100000,
            aggregates=["average"],
            granularity="10s",
        )

        assert {"1|average", "2|max", "123|average"} == set(df.columns)
        assert df.shape[0] > 0

    def test_retrieve_datapoints_some_aggregates_omitted(self, mock_get_datapoints_one_ts_has_missing_aggregates):
        import pandas as pd

        df = DPS_CLIENT.retrieve_dataframe(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            aggregates=[],
            granularity="1s",
        )

        expected_df = pd.DataFrame(
            {"1|average": [0, 1, 2, 3, 4], "def|interpolation": [None, 1, None, 3, None]},
            index=[utils.ms_to_datetime(i) for i in range(5)],
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_dataframe_id_and_external_id_requested(self, rsps):
        rsps.add(
            rsps.POST,
            DPS_CLIENT._base_url + "/timeseries/data/list",
            status=200,
            json={"items": [{"id": 1, "externalId": "abc", "datapoints": [{"timestamp": 0, "average": 1}]}]},
        )
        rsps.add(
            rsps.POST,
            DPS_CLIENT._base_url + "/timeseries/data/list",
            status=200,
            json={"items": [{"id": 2, "externalId": "def", "datapoints": [{"timestamp": 0, "average": 1}]}]},
        )
        res = DPS_CLIENT.retrieve_dataframe(
            start=0, end="now", id=1, external_id=["def"], aggregates=["average"], granularity="1m"
        )
        assert {"1|average", "def|average"} == set(res.columns)

    def test_insert_dataframe(self, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, 3, 4], "456": [5.0, 6.0, 7.0, 8.0]}, index=[utils.ms_to_datetime(ms) for ms in timestamps]
        )
        res = DPS_CLIENT.insert_dataframe(df)
        assert res is None
        request_bodies = [jsgz_load(call.request.body) for call in mock_post_datapoints.calls]
        assert {
            "items": [
                {"id": 123, "datapoints": [{"timestamp": ts, "value": val} for ts, val in zip(timestamps, range(1, 5))]}
            ]
        } in request_bodies
        assert {
            "items": [
                {
                    "id": 456,
                    "datapoints": [{"timestamp": ts, "value": float(val)} for ts, val in zip(timestamps, range(5, 9))],
                }
            ]
        } in request_bodies

    def test_insert_dataframe_with_nans(self):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, None, 4], "456": [5.0, 6.0, 7.0, 8.0]}, index=[utils.ms_to_datetime(ms) for ms in timestamps]
        )
        with pytest.raises(AssertionError, match="contains NaNs"):
            DPS_CLIENT.insert_dataframe(df)

    def test_retrieve_datapoints_multiple_time_series_correct_ordering(self, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = DPS_CLIENT.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
        assert list(dps_res_list.to_pandas().columns) == ["1", "2", "3", "4", "5", "6"], "Incorrect column ordering"

    def test_retrieve_datapoints_one_ts_empty_correct_number_of_columns(self, mock_get_datapoints_one_ts_empty):
        res = DPS_CLIENT.retrieve(id=[1, 2], start=0, end=10000)
        assert 2 == len(res.to_pandas().columns)


@pytest.fixture
def mock_get_dps_count(rsps):
    def request_callback(request):
        payload = jsgz_load(request.body)
        granularity = payload["granularity"]
        aggregates = payload["aggregates"]
        start = payload["start"]
        end = payload["end"]

        assert payload["aggregates"] == ["count"]
        assert utils.granularity_to_ms(payload["granularity"]) >= utils.granularity_to_ms("1d")

        dps = [{"timestamp": i, "count": 1000} for i in range(start, end, utils.granularity_to_ms(granularity))]
        response = {"items": [{"id": 0, "externalId": "bla", "datapoints": dps}]}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/list",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


gms = lambda s: utils.granularity_to_ms(s)


class TestDataFetcher:
    @pytest.mark.parametrize(
        "q, expected_q",
        [
            ([_DPQuery(1, 2, None, None, None, None, None)], [_DPQuery(1, 2, None, None, None, None, None)]),
            (
                [_DPQuery(datetime(2018, 1, 1), datetime(2019, 1, 1), None, None, None, None, None)],
                [_DPQuery(1514764800000, 1546300800000, None, None, None, None, None)],
            ),
            (
                [_DPQuery(gms("1h"), gms(("25h")), None, ["average"], "1d", None, None)],
                [_DPQuery(gms("1d"), gms("2d"), None, ["average"], "1d", None, None)],
            ),
        ],
    )
    def test_preprocess_queries(self, q, expected_q):
        _DatapointsFetcher(DPS_CLIENT)._preprocess_queries(q)
        for actual, expected in zip(q, expected_q):
            assert expected.start == actual.start
            assert expected.end == actual.end

    @pytest.mark.parametrize(
        "ts, granularity, expected_output",
        [
            (gms("1h"), "1d", gms("1d")),
            (gms("23h"), "10d", gms("1d")),
            (gms("24h"), "5d", gms("1d")),
            (gms("25h"), "3d", gms("2d")),
            (gms("1m"), "1h", gms("1h")),
            (gms("90s"), "10m", gms("2m")),
            (gms("90s"), "1s", gms("90s")),
        ],
    )
    def test_align_with_granularity_unit(self, ts, granularity, expected_output):
        assert expected_output == _DatapointsFetcher._align_with_granularity_unit(ts, granularity)

    @pytest.mark.parametrize(
        "start, end, granularity, request_limit, user_limit, expected_output",
        [
            (0, gms("20d"), "10d", 2, None, [_DPWindow(start=0, end=1728000000)]),
            (
                0,
                gms("20d"),
                "10d",
                1,
                None,
                [_DPWindow(start=0, end=864000000), _DPWindow(start=864000000, end=1728000000)],
            ),
            (
                0,
                gms("6d"),
                "1s",
                2000,
                None,
                [_DPWindow(0, gms("2d")), _DPWindow(gms("2d"), gms("4d")), _DPWindow(gms("4d"), gms("6d"))],
            ),
            (
                0,
                gms("3d"),
                None,
                1000,
                None,
                [_DPWindow(0, gms("1d")), _DPWindow(gms("1d"), gms("2d")), _DPWindow(gms("2d"), gms("3d"))],
            ),
            (0, gms("1h"), None, 2000, None, [_DPWindow(start=0, end=3600000)]),
            (0, gms("1s"), None, 1, None, [_DPWindow(start=0, end=1000)]),
            (0, gms("1s"), None, 1, None, [_DPWindow(start=0, end=1000)]),
            (0, gms("3d"), None, 1000, 500, [_DPWindow(0, gms("1d"))]),
        ],
    )
    def test_get_datapoints_windows(
        self, start, end, granularity, request_limit, user_limit, expected_output, mock_get_dps_count
    ):
        res = _DatapointsFetcher(DPS_CLIENT)._get_windows(
            id=0, start=start, end=end, granularity=granularity, request_limit=request_limit, user_limit=user_limit
        )
        assert expected_output == res

    @pytest.mark.parametrize(
        "start, end, granularity, expected_output",
        [
            (0, 10001, "1s", 10000),
            (0, 10000, "1m", 0),
            (0, 110000, "1m", gms("1m")),
            (0, gms("10d") - 1, "2d", gms("8d")),
            (0, gms("10d") - 1, "1m", gms("10d") - gms("1m")),
            (0, 10000, "1s", 10000),
        ],
    )
    def test_align_window_end(self, start, end, granularity, expected_output):
        assert expected_output == _DatapointsFetcher._align_window_end(start, end, granularity)

    def test_remove_duplicates_from_datapoints(self):
        d = Datapoints(
            id=1,
            timestamp=[1, 1, 2, 3, 3, 4, 5, 5, 6, 7, 7],
            value=[0, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1],
            max=[0, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1],
        )
        d_no_dupes = _DatapointsFetcher._remove_duplicates(d)
        assert [1, 2, 3, 4, 5, 6, 7] == d_no_dupes.timestamp
        assert [0, 1, 0, 1, 1, 0, 1] == d_no_dupes.value
        assert [0, 1, 0, 1, 1, 0, 1] == d_no_dupes.max


class TestHelpers:
    @pytest.mark.parametrize(
        "ids, external_ids, expected_output",
        [
            (1, None, ([{"id": 1}], True)),
            (None, "1", ([{"externalId": "1"}], True)),
            (1, "1", ([{"id": 1}, {"externalId": "1"}], False)),
            ([1], ["1"], ([{"id": 1}, {"externalId": "1"}], False)),
            ([1], None, ([{"id": 1}], False)),
            ({"id": 1, "aggregates": ["average"]}, None, ([{"id": 1, "aggregates": ["average"]}], True)),
            ({"id": 1}, {"externalId": "1"}, ([{"id": 1}, {"externalId": "1"}], False)),
            (
                [{"id": 1, "aggregates": ["average"]}],
                [{"externalId": "1", "aggregates": ["average", "sum"]}],
                ([{"id": 1, "aggregates": ["average"]}, {"externalId": "1", "aggregates": ["average", "sum"]}], False),
            ),
        ],
    )
    def test_process_time_series_input_ok(self, ids, external_ids, expected_output):
        assert expected_output == DPS_CLIENT._process_ts_identifiers(ids, external_ids)

    @pytest.mark.parametrize(
        "ids, external_ids, exception, match",
        [
            (1.0, None, TypeError, "Invalid type '<class 'float'>'"),
            ([1.0], None, TypeError, "Invalid type '<class 'float'>'"),
            (None, 1, TypeError, "Invalid type '<class 'int'>'"),
            (None, [1], TypeError, "Invalid type '<class 'int'>'"),
            ({"wrong": 1, "aggregates": ["average"]}, None, ValueError, "Unknown key 'wrong'"),
            (None, [{"externalId": 1, "wrong": ["average"]}], ValueError, "Unknown key 'wrong'"),
            (None, {"id": 1, "aggregates": ["average"]}, ValueError, "Unknown key 'id'"),
            ({"externalId": 1}, None, ValueError, "Unknown key 'externalId'"),
        ],
    )
    def test_process_time_series_input_fail(self, ids, external_ids, exception, match):
        with pytest.raises(exception, match=match):
            DPS_CLIENT._process_ts_identifiers(ids, external_ids)
