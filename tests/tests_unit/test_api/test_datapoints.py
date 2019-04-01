import json
import math
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
def get_datapoints_mock(rsps):
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


class TestDatapointsAPI:
    def assert_dps_response_is_correct(self, calls, dps_object):
        datapoints = []
        for call in calls:
            dps_response = call.response.json()["data"]["items"][0]
            if dps_response["id"] == dps_object.id and dps_response["externalId"] == dps_object.external_id:
                datapoints.extend(dps_response["datapoints"])
                id = dps_response["id"]
                external_id = dps_response["externalId"]

        assert {
            "id": id,
            "externalId": external_id,
            "datapoints": sorted(datapoints, key=lambda x: x["timestamp"]),
        } == dps_object.dump(camel_case=True)

    def test_get_datapoints_by_id(self, get_datapoints_mock):
        dps_res = DPS_CLIENT.get(id=123, start=1000000, end=1100000)
        assert isinstance(dps_res, Datapoints)
        self.assert_dps_response_is_correct(get_datapoints_mock.calls, dps_res)

    def test_get_datapoints_by_external_id(self, get_datapoints_mock):
        dps_res = DPS_CLIENT.get(external_id="123", start=1000000, end=1100000)
        self.assert_dps_response_is_correct(get_datapoints_mock.calls, dps_res)

    def test_get_datapoints_aggregates(self, get_datapoints_mock):
        dps_res = DPS_CLIENT.get(id=123, start=1000000, end=1100000, aggregates=["avg", "step"], granularity="10s")
        self.assert_dps_response_is_correct(get_datapoints_mock.calls, dps_res)

    def test_datapoints_paging(self, get_datapoints_mock, set_dps_limits, set_dps_workers):
        set_dps_workers(1)
        set_dps_limits(1)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=10000, aggregates=["avg"], granularity="1s")
        assert 10 == len(dps_res.datapoints)

    def test_datapoints_concurrent(self, get_datapoints_mock, set_dps_workers, set_dps_limits):
        set_dps_workers(5)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=20000, aggregates=["avg"], granularity="1s")
        requested_windows = sorted(
            [
                (jsgz_load(call.request.body)["start"], jsgz_load(call.request.body)["end"])
                for call in get_datapoints_mock.calls
            ],
            key=lambda x: x[0],
        )

        assert [(0, 4000), (5000, 9000), (10000, 14000), (15000, 19000)] == requested_windows
        self.assert_dps_response_is_correct(get_datapoints_mock.calls, dps_res)

    @pytest.mark.parametrize(
        "max_workers, aggregates, granularity, actual_windows_req",
        [
            (1, None, None, [(0, 20000), (9001, 20000), (18002, 20000)]),
            (2, None, None, [(0, 10000), (9001, 10000), (10001, 20000), (19002, 20000)]),
            (3, None, None, [(0, 6666), (6667, 13333), (13334, 20000)]),
            (4, ["avg"], "1s", [(0, 5000), (6000, 11000), (12000, 17000), (18000, 20000)]),
            (2, ["avg"], "5s", [(0, 10000), (15000, 20000)]),
            (4, ["avg"], "5s", [(0, 5000), (10000, 15000)]),
        ],
    )
    def test_request_dps_spacing_correct(
        self,
        get_datapoints_mock,
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
                for call in get_datapoints_mock.calls
            ],
            key=lambda x: x[0],
        )
        assert actual_windows_req == requested_windows

    def test_datapoints_paging_with_limit(self, get_datapoints_mock, set_dps_limits):
        set_dps_limits(3)
        dps_res = DPS_CLIENT.get(id=123, start=0, end=10000, aggregates=["avg"], granularity="1s", limit=4)
        assert 4 == len(dps_res.datapoints)

    def test_get_datapoints_multiple_time_series(self, get_datapoints_mock, set_dps_limits):
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
            self.assert_dps_response_is_correct(get_datapoints_mock.calls, dps_res)
            print(dps_res)
        assert 0 == len(ids)
        assert 0 == len(external_ids)


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
