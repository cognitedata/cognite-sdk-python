import json
from random import random

import pytest

from cognite.client import CogniteClient
from tests.utils import jsgz_load

DPS_CLIENT = CogniteClient().datapoints


def generate_datapoints(start: int, end: int, aggregates=None):
    dps = []
    if aggregates:
        for i in range(start, end, 10000):
            dp = {}
            for agg in aggregates:
                dp[agg] = random()
            dp["timestamp"] = i
            dps.append(dp)
    else:
        for i in range(start, end, 10000):
            dp = {"timestamp": i, "value": random()}
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

            if "start" in dps_query and "end" in dps_query:
                start, end = dps_query["start"], dps_query["end"]
            else:
                start, end = payload["start"], payload["end"]

            dps = generate_datapoints(start, end, aggregates)
            items.append({"id": dps_query["id"], "datapoints": dps})
        response = {"data": {"items": items}}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._base_url + "/timeseries/data/get",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


class TestDatapointsAPI:
    def test_get_datapoints(self, get_datapoints_mock):
        res = DPS_CLIENT.get(id=123, start=1000000, end=1100000)
        print(res)
        assert get_datapoints_mock.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
