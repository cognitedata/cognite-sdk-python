import json
from random import random

import pytest

from cognite.client import CogniteClient
from cognite.client._utils import utils
from cognite.client.api.datapoints import Datapoints, DatapointsList, _DPWindow
from tests.utils import jsgz_load

DPS_CLIENT = CogniteClient(debug=True, max_workers=20).datapoints


def generate_datapoints(start: int, end: int, aggregates=None, granularity=None):
    dps = []
    granularity = utils.granularity_to_ms(granularity) if granularity else 1000
    for i in range(start, end, granularity):
        dp = {}
        if aggregates:
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
            id_to_return = dps_query.get("id", -1)
            external_id_to_return = dps_query.get("externalId", "-1")
            items.append({"id": id_to_return, "externalId": external_id_to_return, "datapoints": dps})
        response = {"data": {"items": items}}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/get",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def set_dps_limits():
    def set_limit(limit):
        DPS_CLIENT._LIMIT_AGG = limit
        DPS_CLIENT._LIMIT = limit

    limit_agg_tmp = DPS_CLIENT._LIMIT_AGG
    limit_tmp = DPS_CLIENT._LIMIT
    yield set_limit
    DPS_CLIENT._LIMIT_AGG = limit_agg_tmp
    DPS_CLIENT._LIMIT = limit_tmp


@pytest.fixture
def set_dps_workers():
    def set_limit(limit):
        DPS_CLIENT._max_workers = limit

    workers_tmp = DPS_CLIENT._max_workers
    yield set_limit
    DPS_CLIENT._max_workers = workers_tmp


class TestGetDatapoints:
    def assert_dps_response_is_correct(self, calls, dps_object):
        datapoints = []
        for call in calls:
            dps_response = call.response.json()["data"]["items"][0]
            dps = {}
            if dps_response["id"] == dps_object.id and dps_response["externalId"] == dps_object.external_id:
                datapoints.extend(dps_response["datapoints"])
                id = dps_response["id"]
                external_id = dps_response["externalId"]

        assert {
            "id": id,
            "externalId": external_id,
            "datapoints": sorted(datapoints, key=lambda x: x["timestamp"]),
        } == dps_object.dump(camel_case=True)

    def test_get_datapoints_by_id(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.get(id=123, start=1000000, end=1100000)
        assert isinstance(dps_res, Datapoints)
        print()
        print(dps_res.dump(camel_case=True))
        print()

        self.assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_get_datapoints_by_external_id(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.get(external_id="123", start=1000000, end=1100000)
        self.assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_get_datapoints_aggregates(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.get(
            id=123, start=1000000, end=1100000, aggregates=["average", "stepInterpolation"], granularity="10s"
        )
        self.assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_datapoints_paging(self, mock_get_datapoints, set_dps_limits, set_dps_workers):
        set_dps_workers(1)
        set_dps_limits(1)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=10000, aggregates=["average"], granularity="1s")
        assert 10 == len(dps_res)

    def test_datapoints_concurrent(self, mock_get_datapoints, set_dps_workers, set_dps_limits):
        set_dps_workers(5)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=20000, aggregates=["average"], granularity="1s")
        requested_windows = sorted(
            [
                (jsgz_load(call.request.body)["start"], jsgz_load(call.request.body)["end"])
                for call in mock_get_datapoints.calls
            ],
            key=lambda x: x[0],
        )

        assert [(0, 4000), (5000, 9000), (10000, 14000), (15000, 19000)] == requested_windows
        self.assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    @pytest.mark.parametrize(
        "max_workers, aggregates, granularity, actual_windows_req",
        [
            (1, None, None, [(0, 20000), (9001, 20000), (18002, 20000)]),
            (2, None, None, [(0, 10000), (9001, 10000), (10001, 20000), (19002, 20000)]),
            (3, None, None, [(0, 6666), (6667, 13333), (13334, 20000)]),
            (4, ["average"], "1s", [(0, 5000), (6000, 11000), (12000, 17000), (18000, 20000)]),
            (2, ["average"], "5s", [(0, 10000), (15000, 20000)]),
            (4, ["average"], "5s", [(0, 5000), (10000, 15000)]),
        ],
    )
    def test_request_dps_spacing_correct(
        self,
        mock_get_datapoints,
        set_dps_workers,
        set_dps_limits,
        max_workers,
        aggregates,
        granularity,
        actual_windows_req,
    ):
        set_dps_limits(10)
        set_dps_workers(max_workers)
        DPS_CLIENT.get(id=123, start=0, end=20000, aggregates=aggregates, granularity=granularity)
        requested_windows = sorted(
            [
                (jsgz_load(call.request.body)["start"], jsgz_load(call.request.body)["end"])
                for call in mock_get_datapoints.calls
            ],
            key=lambda x: x[0],
        )
        assert actual_windows_req == requested_windows

    def test_datapoints_paging_with_limit(self, mock_get_datapoints, set_dps_limits):
        set_dps_limits(3)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=10000, aggregates=["average"], granularity="1s", limit=4)
        assert 4 == len(dps_res)

    def test_get_datapoints_multiple_time_series(self, mock_get_datapoints, set_dps_limits):
        set_dps_limits(10)
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = DPS_CLIENT.get(id=ids, external_id=external_ids, start=0, end=100000)
        assert isinstance(dps_res_list, DatapointsList)
        for dps_res in dps_res_list:
            if dps_res.id in ids:
                ids.remove(dps_res.id)
            if dps_res.external_id in external_ids:
                external_ids.remove(dps_res.external_id)
            self.assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)
        assert 0 == len(ids)
        assert 0 == len(external_ids)


@pytest.fixture
def mock_get_latest(rsps):
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
        return 200, {}, json.dumps({"data": {"items": items}})

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/latest",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


class TestGetLatest:
    def test_get_latest(self, mock_get_latest):
        res = DPS_CLIENT.get_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 10000 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_get_latest_multiple_ts(self, mock_get_latest):
        res = DPS_CLIENT.get_latest(id=1, external_id="2")
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 10000 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_get_latest_with_before(self, mock_get_latest):
        res = DPS_CLIENT.get_latest(id=1, before=10)
        assert isinstance(res, Datapoints)
        assert 9 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_get_latest_multiple_ts_with_before(self, mock_get_latest):
        res = DPS_CLIENT.get_latest(id=[1, 2], external_id=["1", "2"], before=10)
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 9 == dps[0].timestamp
            assert isinstance(dps[0].value, float)


class TestDatapointsObject:
    def test_len(self):
        assert 3 == len(Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3]))

    def test_get_operative_attr_names(self):
        assert ["timestamp", "value"] == Datapoints(
            id=1, timestamp=[1, 2, 3], value=[1, 2, 3]
        )._get_operative_attr_names()
        assert ["timestamp", "max", "sum"] == Datapoints(
            id=1, timestamp=[1, 2, 3], sum=[1, 2, 3], max=[1, 2, 3]
        )._get_operative_attr_names()
        assert [] == Datapoints(id=1)._get_operative_attr_names()

    def test_load(self):
        res = Datapoints._load(
            {"id": 1, "externalId": "1", "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}]}
        )
        assert 1 == res.id
        assert "1" == res.external_id
        assert [1, 2] == res.timestamp
        assert [1, 2] == res.value

    def test_truncate(self):
        res = Datapoints(id=1, timestamp=[1, 2, 3])._truncate(limit=1)
        assert [1] == res.timestamp


class TestHelpers:
    @pytest.mark.parametrize(
        "start, end, granularity, num_of_workers, expected_output",
        [
            (1550241236999, 1550244237001, "1d", 1, [_DPWindow(1550241236999, 1550244237001)]),
            (0, 10000, "1s", 10, [_DPWindow(i, i + 1000) for i in range(0, 10000, 2000)]),
            (0, 2500, "1s", 3, [_DPWindow(0, 1250), _DPWindow(2250, 2500)]),
            (0, 2500, None, 3, [_DPWindow(0, 833), _DPWindow(834, 1667), _DPWindow(1668, 2500)]),
        ],
    )
    def test_get_datapoints_windows(self, start, end, granularity, num_of_workers, expected_output):
        res = DPS_CLIENT._get_windows(start=start, end=end, granularity=granularity, max_windows=num_of_workers)
        assert expected_output == res

    def test_concatenate_datapoints(self):
        d1 = Datapoints(id=1, external_id="1", timestamp=[1, 2, 3], value=[1, 2, 3])
        d2 = Datapoints(id=1, external_id="1", timestamp=[4, 5, 6], value=[4, 5, 6])
        concatenated = DPS_CLIENT._concatenate_datapoints(d1, d2)
        assert [1, 2, 3, 4, 5, 6] == concatenated.timestamp
        assert [1, 2, 3, 4, 5, 6] == concatenated.value
        assert 1 == concatenated.id
        assert "1" == concatenated.external_id
        assert concatenated.sum is None
