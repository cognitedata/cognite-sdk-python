import json
import math
from contextlib import contextmanager
from datetime import datetime
from random import choice, random
from typing import List
from unittest import mock
from unittest.mock import PropertyMock

import pytest

from cognite.client import CogniteClient, utils
from cognite.client._api.datapoints import DatapointsBin, DatapointsFetcher, _DPTask, _DPWindow
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsList, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicateColumnsError, CogniteNotFoundError
from tests.utils import jsgz_load, set_request_limit

COGNITE_CLIENT = CogniteClient()
DPS_CLIENT = COGNITE_CLIENT.datapoints.synthetic


def generate_datapoints(start: int, end: int, granularity=1):
    dps = []
    for i in range(start, end, granularity):
        dp = {}
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

            if "start" in dps_query and "end" in dps_query:
                start, end = dps_query["start"], dps_query["end"]
            else:
                start, end = payload["start"], payload["end"]

            limit = 100000
            if "limit" in dps_query:
                limit = dps_query["limit"]
            elif "limit" in payload:
                limit = payload["limit"]

            dps = generate_datapoints(start, end)
            dps = dps[:limit]
            id_to_return = dps_query.get("id", int(dps_query.get("externalId", "-1")))
            external_id_to_return = dps_query.get("externalId", str(dps_query.get("id", -1)))
            items.append({"isString": False, "datapoints": dps})
        response = {"items": items}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        DPS_CLIENT._get_base_url_with_base_path() + "/timeseries/synthetic/query",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_empty(rsps):
    rsps.add(
        rsps.POST,
        DPS_CLIENT._get_base_url_with_base_path() + "/timeseries/synthetic",
        status=200,
        json={"items": [{"isString": False, "datapoints": []}]},
    )
    yield rsps


class TestSyntheticQuery:
    def test_retrieve(self, mock_get_datapoints):
        dps_res = DPS_CLIENT.retrieve(expression='TS{externalID:"abc"} + TS{id:1} ', start=1000000, end=1100001)
        assert isinstance(dps_res, Datapoints)
        assert 100001 == len(dps_res)
        assert 2 == len(mock_get_datapoints.calls)

    def test_retrieve_empty(self, mock_get_datapoints_empty):
        dps_res = DPS_CLIENT.retrieve(expression='TS{externalID:"abc"} + TS{id:1} ', start=1000000, end=1100001)
        assert isinstance(dps_res, Datapoints)
        assert 0 == len(dps_res)
        assert 1 == len(mock_get_datapoints_empty.calls)
