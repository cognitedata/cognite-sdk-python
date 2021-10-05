import json
import math
from datetime import datetime
from random import random
from typing import List
from unittest import mock

import pytest

from cognite.client import utils
from cognite.client._api.datapoints import DatapointsBin, DatapointsFetcher, _DPTask, _DPWindow
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsList, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicateColumnsError, CogniteNotFoundError
from tests.utils import jsgz_load, set_request_limit


def generate_datapoints(start: int, end: int, aggregates=None, granularity=None):
    dps = []
    granularity = utils._time.granularity_to_ms(granularity) if granularity else 1000
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
def mock_get_datapoints(rsps, cognite_client):
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
            items.append(
                {
                    "id": id_to_return,
                    "externalId": external_id_to_return,
                    "isString": False,
                    "isStep": False,
                    "datapoints": dps,
                }
            )
        response = {"items": items}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={
            "items": [{"id": 1, "externalId": "1", "isString": False, "isStep": False, "unit": "kPa", "datapoints": []}]
        },
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_include_outside(rsps, cognite_client):
    # return 100001 datapoints with one beyond 'end'
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "1",
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": i, "value": i} for i in range(1000000000, 1000000000 + 100001)],
                }
            ]
        },
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_one_ts_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "1",
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": 1, "value": 1}],
                }
            ]
        },
    )
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={"items": [{"id": 2, "externalId": "2", "isString": False, "isStep": False, "datapoints": []}]},
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_one_ts_has_missing_aggregates(rsps, cognite_client):
    def callback(request):
        item = jsgz_load(request.body)
        if item["aggregates"] == ["average"]:
            dps = {
                "id": 1,
                "externalId": "abc",
                "isString": False,
                "isStep": False,
                "datapoints": [
                    {"timestamp": 0, "average": 0},
                    {"timestamp": 1, "average": 1},
                    {"timestamp": 2, "average": 2},
                    {"timestamp": 3, "average": 3},
                    {"timestamp": 4, "average": 4},
                ],
            }
        else:
            dps = {
                "id": 2,
                "externalId": "def",
                "isString": False,
                "isStep": False,
                "datapoints": [
                    {"timestamp": 0},
                    {"timestamp": 1, "interpolation": 1},
                    {"timestamp": 2},
                    {"timestamp": 3, "interpolation": 3},
                    {"timestamp": 4},
                ],
            }
        return 200, {}, json.dumps({"items": [dps]})

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_several_missing(rsps, cognite_client):
    def callback(request):
        item = jsgz_load(request.body)
        if item["aggregates"] == ["interpolation"]:
            dps = {
                "id": 2,
                "externalId": "abc",
                "isString": False,
                "isStep": False,
                "datapoints": [{"timestamp": 1000, "interpolation": 1}, {"timestamp": 3000, "interpolation": 3}],
            }
        elif item["aggregates"] == ["count"]:
            dps = {
                "id": 3,
                "externalId": "def",
                "isString": False,
                "isStep": False,
                "datapoints": [
                    {"timestamp": 1000, "count": 2},
                    {"timestamp": 3000, "count": 4},
                    {"timestamp": 4000, "count": 5},
                ],
            }
        elif item["aggregates"] == ["average"]:
            dps = {
                "id": 1,
                "externalId": "def",
                "isString": False,
                "isStep": False,
                "datapoints": [
                    {"timestamp": 0, "average": 11},
                    {"timestamp": 1000, "average": 22},
                    {"timestamp": 3000, "average": 44},
                ],
            }

        return 200, {}, json.dumps({"items": [dps]})

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=callback,
        content_type="application/json",
    )

    response_body = {
        "items": [
            {"id": 1, "isStep": False, "isString": False},
            {"id": 2, "isStep": None, "isString": False},
            {"id": 3, "isStep": True, "isString": False},
        ]
    }
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/byids",
        status=200,
        json=response_body,
    )
    rsps.assert_all_requests_are_fired = False
    yield rsps


@pytest.fixture
def mock_get_datapoints_single_isstep(rsps, cognite_client):
    def callback(request):
        item = jsgz_load(request.body)
        if item["aggregates"] == ["interpolation"]:
            dps = {
                "id": 3,
                "externalId": "abc",
                "isStep": True,
                "isString": False,
                "datapoints": [{"timestamp": 1000, "interpolation": 1}, {"timestamp": 3000, "interpolation": 3}],
            }
        return 200, {}, json.dumps({"items": [dps]})

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def set_dps_workers(cognite_client):
    def set_workers(limit):
        cognite_client.datapoints._config.max_workers = limit

    workers_tmp = cognite_client.datapoints._config.max_workers
    yield set_workers
    cognite_client.datapoints._config.max_workers = workers_tmp


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
    def test_retrieve_datapoints_by_id(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.datapoints.retrieve(id=123, start=1000000, end=1100000)
        assert isinstance(dps_res, Datapoints)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_500(self, cognite_client, rsps):
        rsps.add(
            rsps.POST,
            cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
            json={"error": {"code": 500, "message": "Internal Server Error"}},
            status=500,
        )
        with pytest.raises(CogniteAPIError):
            cognite_client.datapoints.retrieve(id=123, start=1000000, end=1100000)

    def test_retrieve_datapoints_by_external_id(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.datapoints.retrieve(external_id="123", start=1000000, end=1100000)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_aggregates(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.datapoints.retrieve(
            id=123, start=1000000, end=1100000, aggregates=["average", "stepInterpolation"], granularity="10s"
        )
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_local_aggregates(self, cognite_client, mock_get_datapoints):
        dps_res_list = cognite_client.datapoints.retrieve(
            external_id={"externalId": "123", "aggregates": ["average"]},
            id={"id": 234},
            start=1000000,
            end=1100000,
            aggregates=["max"],
            granularity="10s",
        )
        for dps_res in dps_res_list:
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_some_aggregates_omitted(
        self, cognite_client, mock_get_datapoints_one_ts_has_missing_aggregates
    ):
        dps_res_list = cognite_client.datapoints.retrieve(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            granularity="1s",
        )
        for dps in dps_res_list:
            if dps.id == 1:
                assert dps.average == [0, 1, 2, 3, 4]
            elif dps.id == 2:
                assert dps.interpolation == [None, 1, None, 3, None]

    def test_datapoints_paging(self, cognite_client, mock_get_datapoints, set_dps_workers):
        set_dps_workers(1)
        with set_request_limit(cognite_client.datapoints, 2):
            dps_res = cognite_client.datapoints.retrieve(
                id=123, start=0, end=10000, aggregates=["average"], granularity="1s"
            )
        assert 6 == len(mock_get_datapoints.calls)
        assert 10 == len(dps_res)

    def test_datapoints_concurrent(self, cognite_client, mock_get_datapoints):
        with set_request_limit(cognite_client.datapoints, 20):
            dps_res = cognite_client.datapoints.retrieve(
                id=123, start=0, end=100000, aggregates=["average"], granularity="1s"
            )
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

    def test_datapoints_paging_with_limit(self, cognite_client, mock_get_datapoints):
        with set_request_limit(cognite_client.datapoints, 3):
            dps_res = cognite_client.datapoints.retrieve(
                id=123, start=0, end=10000, aggregates=["average"], granularity="1s", limit=4
            )
        assert 4 == len(dps_res)

    def test_retrieve_datapoints_multiple_time_series(self, cognite_client, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = cognite_client.datapoints.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
        assert isinstance(dps_res_list, DatapointsList), type(dps_res_list)
        for dps_res in dps_res_list:
            if dps_res.id in ids:
                ids.remove(dps_res.id)
            if dps_res.external_id in external_ids:
                external_ids.remove(dps_res.external_id)
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)
        assert 0 == len(ids)
        assert 0 == len(external_ids)

    def test_retrieve_datapoints_empty(self, cognite_client, mock_get_datapoints_empty):
        res = cognite_client.datapoints.retrieve(id=1, start=0, end=10000)
        assert 0 == len(res)

    def test_retrieve_datapoints_empty_extrafields_set(self, cognite_client, mock_get_datapoints_empty):
        res = cognite_client.datapoints.retrieve(id=1, start=0, end=10000)
        assert "kPa" == res.unit
        assert res.is_step is False
        assert res.is_string is False

    def test_aggregate_limits_correct(self, cognite_client, mock_get_datapoints):
        cognite_client.datapoints.retrieve(id={"id": 1, "aggregates": ["average"]}, start=0, end=10, granularity="1d")
        cognite_client.datapoints.retrieve(id=1, start=0, end=10, granularity="1d", aggregates=["max"])
        cognite_client.datapoints.retrieve(id=1, start=0, end=10)
        assert 10000 == jsgz_load(mock_get_datapoints.calls[0].request.body)["limit"]
        assert 10000 == jsgz_load(mock_get_datapoints.calls[1].request.body)["limit"]
        assert 100000 == jsgz_load(mock_get_datapoints.calls[2].request.body)["limit"]


class TestQueryDatapoints:
    def test_query_single(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.datapoints.query(query=DatapointsQuery(id=1, start=0, end=10000))
        assert isinstance(dps_res, DatapointsList)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res[0])

    def test_query_multiple(self, cognite_client, mock_get_datapoints):
        dps_res_list = cognite_client.datapoints.query(
            query=[
                DatapointsQuery(id=1, start=0, end=10000),
                DatapointsQuery(external_id="2", start=10000, end=20000, aggregates=["average"], granularity="2s"),
            ]
        )
        assert isinstance(dps_res_list, List)
        for dps_res in dps_res_list:
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res[0])

    def test_query_empty(self, cognite_client, mock_get_datapoints_empty):
        dps_res = cognite_client.datapoints.query(query=DatapointsQuery(id=1, start=0, end=10000))
        assert isinstance(dps_res, DatapointsList)
        assert 1 == len(dps_res)
        assert 0 == len(dps_res[0])


@pytest.fixture
def mock_retrieve_latest(rsps, cognite_client):
    def request_callback(request):
        payload = jsgz_load(request.body)

        items = []
        for latest_query in payload["items"]:
            id = latest_query.get("id", -1)
            external_id = latest_query.get("externalId", "-1")
            before = latest_query.get("before", 10001)
            items.append(
                {
                    "id": id,
                    "externalId": external_id,
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": before - 1, "value": random()}],
                }
            )
        return 200, {}, json.dumps({"items": items})

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/latest",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_retrieve_latest_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/latest",
        status=200,
        json={
            "items": [
                {"id": 1, "externalId": "1", "isString": False, "isStep": True, "datapoints": []},
                {"id": 2, "isString": False, "isStep": False, "externalId": "2", "datapoints": []},
            ]
        },
    )
    yield rsps


@pytest.fixture
def mock_retrieve_latest_with_failure(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/latest",
        status=200,
        json={
            "items": [
                {"id": 1, "externalId": "1", "isString": False, "isStep": False, "datapoints": []},
                {"id": 2, "externalId": "2", "isString": False, "isStep": False, "datapoints": []},
            ]
        },
    )
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/latest",
        status=500,
        json={"error": {"code": 500, "message": "Internal Server Error"}},
    )
    yield rsps


class TestGetLatest:
    def test_retrieve_latest(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.datapoints.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 10000 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.datapoints.retrieve_latest(id=1, external_id="2")
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 10000 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_with_before(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.datapoints.retrieve_latest(id=1, before=10)
        assert isinstance(res, Datapoints)
        assert 9 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts_with_before(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.datapoints.retrieve_latest(id=[1, 2], external_id=["1", "2"], before=10)
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 9 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_empty(self, cognite_client, mock_retrieve_latest_empty):
        res = cognite_client.datapoints.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 0 == len(res)

    def test_retrieve_latest_multiple_ts_empty(self, cognite_client, mock_retrieve_latest_empty):
        res_list = cognite_client.datapoints.retrieve_latest(id=[1, 2])
        assert isinstance(res_list, DatapointsList)
        assert 2 == len(res_list)
        for res in res_list:
            assert 0 == len(res)

    def test_retrieve_latest_concurrent_fails(self, cognite_client, mock_retrieve_latest_with_failure):
        with set_request_limit(cognite_client.datapoints, 2):
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.datapoints.retrieve_latest(id=[1, 2, 3])
            assert e.value.code == 500


@pytest.fixture
def mock_post_datapoints(rsps, cognite_client):
    rsps.add(
        rsps.POST, cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data", status=200, json={}
    )
    yield rsps


@pytest.fixture
def mock_post_datapoints_400(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data",
        status=400,
        json={"error": {"message": "Ts not found", "missing": [{"externalId": "does_not_exist"}]}},
    )
    yield rsps


class TestInsertDatapoints:
    def test_insert_tuples(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        res = cognite_client.datapoints.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_insert_dicts(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        res = cognite_client.datapoints.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_by_external_id(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        cognite_client.datapoints.insert(dps, external_id="1")
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}
            ]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    @pytest.mark.parametrize("ts_key, value_key", [("timestamp", "values"), ("timstamp", "value")])
    def test_invalid_datapoints_keys(self, cognite_client, ts_key, value_key):
        dps = [{ts_key: i * 1e11, value_key: i} for i in range(1, 11)]
        with pytest.raises(AssertionError, match="is missing the"):
            cognite_client.datapoints.insert(dps, id=1)

    def test_insert_datapoints_over_limit(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        with set_request_limit(cognite_client.datapoints, 5):
            res = cognite_client.datapoints.insert(dps, id=1)
        assert res is None
        request_bodies = [jsgz_load(call.request.body) for call in mock_post_datapoints.calls]
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 6)]}]
        } in request_bodies
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(6, 11)]}]
        } in request_bodies

    def test_insert_datapoints_no_data(self, cognite_client):
        with pytest.raises(AssertionError, match="No datapoints provided"):
            cognite_client.datapoints.insert(id=1, datapoints=[])

    def test_insert_datapoints_in_multiple_time_series(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        dps_objects = [{"externalId": "1", "datapoints": dps}, {"id": 1, "datapoints": dps}]
        res = cognite_client.datapoints.insert_multiple(dps_objects)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]},
                {"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]},
            ]
        } == request_body

    def test_insert_datapoints_in_multiple_time_series_invalid_key(self, cognite_client):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        dps_objects = [{"extId": "1", "datapoints": dps}]
        with pytest.raises(ValueError, match="Invalid key 'extId'"):
            cognite_client.datapoints.insert_multiple(dps_objects)

    def test_insert_datapoints_ts_does_not_exist(self, cognite_client, mock_post_datapoints_400):
        with pytest.raises(CogniteNotFoundError):
            cognite_client.datapoints.insert(datapoints=[(1e14, 1)], external_id="does_not_exist")

    def test_insert_multiple_ts__below_ts_and_dps_limit(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(100)]
        cognite_client.datapoints.insert_multiple(dps_objects)
        assert 1 == len(mock_post_datapoints.calls)
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        for i, dps in enumerate(request_body["items"]):
            assert i == dps["id"]

    @pytest.fixture
    def set_post_dps_objects_limit_to_100(self, cognite_client):
        tmp = cognite_client.datapoints._POST_DPS_OBJECTS_LIMIT
        cognite_client.datapoints._POST_DPS_OBJECTS_LIMIT = 100
        yield
        cognite_client.datapoints._POST_DPS_OBJECTS_LIMIT = tmp

    def test_insert_multiple_ts_single_call__below_dps_limit_above_ts_limit(
        self, cognite_client, mock_post_datapoints, set_post_dps_objects_limit_to_100
    ):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(101)]
        cognite_client.datapoints.insert_multiple(dps_objects)
        assert 2 == len(mock_post_datapoints.calls)

    def test_insert_multiple_ts_single_call__above_dps_limit_below_ts_limit(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 1002)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(10)]
        cognite_client.datapoints.insert_multiple(dps_objects)
        assert 2 == len(mock_post_datapoints.calls)


@pytest.fixture
def mock_delete_datapoints(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/delete",
        status=200,
        json={},
    )
    yield rsps


class TestDeleteDatapoints:
    def test_delete_range(self, cognite_client, mock_delete_datapoints):
        res = cognite_client.datapoints.delete_range(start=datetime(2018, 1, 1), end=datetime(2018, 1, 2), id=1)
        assert res is None
        assert {"items": [{"id": 1, "inclusiveBegin": 1514764800000, "exclusiveEnd": 1514851200000}]} == jsgz_load(
            mock_delete_datapoints.calls[0].request.body
        )

    @pytest.mark.parametrize(
        "id, external_id, exception",
        [(None, None, AssertionError), (1, "1", AssertionError), ("1", None, TypeError), (None, 1, TypeError)],
    )
    def test_delete_range_invalid_id(self, cognite_client, id, external_id, exception):
        with pytest.raises(exception):
            cognite_client.datapoints.delete_range("1d-ago", "now", id, external_id)

    def test_delete_range_start_after_end(self, cognite_client):
        with pytest.raises(AssertionError, match="must be"):
            cognite_client.datapoints.delete_range(1, 0, 1)

    def test_delete_ranges(self, cognite_client, mock_delete_datapoints):
        ranges = [{"id": 1, "start": 0, "end": 1}, {"externalId": "1", "start": 0, "end": 1}]
        cognite_client.datapoints.delete_ranges(ranges)
        assert {
            "items": [
                {"id": 1, "inclusiveBegin": 0, "exclusiveEnd": 1},
                {"externalId": "1", "inclusiveBegin": 0, "exclusiveEnd": 1},
            ]
        } == jsgz_load(mock_delete_datapoints.calls[0].request.body)

    def test_delete_ranges_invalid_ids(self, cognite_client):
        ranges = [{"idz": 1, "start": 0, "end": 1}]
        with pytest.raises(AssertionError, match="Invalid key 'idz'"):
            cognite_client.datapoints.delete_ranges(ranges)
        ranges = [{"start": 0, "end": 1}]
        with pytest.raises(AssertionError, match="Exactly one of id and external id must be specified"):
            cognite_client.datapoints.delete_ranges(ranges)


class TestDatapointsObject:
    def test_len(self, cognite_client):
        assert 3 == len(Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3]))

    def test_get_non_empty_data_fields(self, cognite_client):
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

    def test_iter(self, cognite_client):
        for dp in Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3]):
            assert dp.timestamp in [1, 2, 3]
            assert dp.value in [1, 2, 3]

    def test_eq(self, cognite_client):
        assert Datapoints(1) == Datapoints(1)
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) == Datapoints(
            1, timestamp=[1, 2, 3], value=[1, 2, 3]
        )
        assert Datapoints(1) != Datapoints(0)
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) != Datapoints(1, timestamp=[1, 2, 3], max=[1, 2, 3])
        assert Datapoints(1, timestamp=[1, 2, 3], value=[1, 2, 3]) != Datapoints(
            1, timestamp=[1, 2, 3], value=[1, 2, 4]
        )

    def test_get_item(self, cognite_client):
        dps = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])

        assert Datapoint(timestamp=1, value=1) == dps[0]
        assert Datapoint(timestamp=2, value=2) == dps[1]
        assert Datapoint(timestamp=3, value=3) == dps[2]
        assert Datapoints(id=1, timestamp=[1, 2], value=[1, 2]) == dps[:2]

    def test_load(self, cognite_client):
        res = Datapoints._load(
            {
                "id": 1,
                "externalId": "1",
                "isString": False,
                "isStep": False,
                "unit": "kPa",
                "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}],
            }
        )
        assert 1 == res.id
        assert "1" == res.external_id
        assert [1, 2] == res.timestamp
        assert [1, 2] == res.value
        assert "kPa" == res.unit
        assert res.is_step is False
        assert res.is_string is False

    def test_load_string(self, cognite_client):
        res = Datapoints._load(
            {
                "id": 1,
                "externalId": "1",
                "isString": True,
                "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}],
            }
        )
        assert 1 == res.id
        assert "1" == res.external_id
        assert [1, 2] == res.timestamp
        assert [1, 2] == res.value
        assert res.is_string is True
        assert res.is_step is None
        assert res.unit is None

    def test_slice(self, cognite_client):
        res = Datapoints(id=1, timestamp=[1, 2, 3])._slice(slice(None, 1))
        assert [1] == res.timestamp

    def test_extend(self, cognite_client):
        d0 = Datapoints()
        d1 = Datapoints(id=1, external_id="1", timestamp=[1, 2, 3], value=[1, 2, 3])
        d2 = Datapoints(id=1, external_id="1", timestamp=[4, 5, 6], value=[4, 5, 6])
        d3 = Datapoints(id=1, external_id="1", timestamp=[7, 8, 9, 10], value=[7, 8, 9, 10])

        d0._extend(d1)
        assert [1, 2, 3] == d0.timestamp
        assert [1, 2, 3] == d0.value
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum is None

        d0._extend(d2)
        assert [1, 2, 3, 4, 5, 6] == d0.value
        assert [1, 2, 3, 4, 5, 6] == d0.timestamp
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum is None

        d0._extend(d3)
        assert [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] == d0.timestamp
        assert [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] == d0.value
        assert 1 == d0.id
        assert "1" == d0.external_id
        assert d0.sum is None


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
    def test_datapoint(self, cognite_client):
        import pandas as pd

        d = Datapoint(timestamp=0, value=2, max=3)
        expected_df = pd.DataFrame({"value": [2], "max": [3]}, index=[utils._time.ms_to_datetime(0)])
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(), check_like=True)

    def test_datapoints(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"1|average": [2, 3, 4], "1|stepInterpolation": [3, 4, 5]},
            index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_no_names(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4])
        expected_df = pd.DataFrame({"1": [2, 3, 4]}, index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]])
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=False))
        expected_df = pd.DataFrame({"1|average": [2, 3, 4]}, index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]])
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=True))

    def test_id_and_external_id_set_gives_external_id_columns(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=0, external_id="abc", timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"abc|average": [2, 3, 4], "abc|stepInterpolation": [3, 4, 5]},
            index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_empty(self, cognite_client):
        d = Datapoints(external_id="1", timestamp=[], value=[])
        assert d.to_pandas().empty

    def test_datapoints_list(self, cognite_client):
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
            index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())

    def test_datapoints_list_names(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4])
        d2 = Datapoints(id=3, timestamp=[1, 3], average=[1, 3])
        dps_list = DatapointsList([d1, d2])
        expected_df = pd.DataFrame(
            {"2|max": [2, 3, 4], "3|average": [1, None, 3]}, index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]]
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())
        expected_df.columns = [c[:1] for c in expected_df.columns]
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(include_aggregate_name=False))

    def test_datapoints_list_names_dup(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4])
        d2 = Datapoints(id=2, timestamp=[1, 3], average=[1, 3])
        dps_list = DatapointsList([d1, d2])
        expected_df = pd.DataFrame(
            {"2|max": [2, 3, 4], "2|average": [1, None, 3]},
            index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3]],
            columns=["2|max", "2|average"],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())
        with pytest.raises(CogniteDuplicateColumnsError):
            dps_list.to_pandas(include_aggregate_name=False)

    def test_datapoints_list_non_aligned(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])
        d2 = Datapoints(id=2, timestamp=[3, 4, 5], value=[3, 4, 5])

        dps_list = DatapointsList([d1, d2])

        expected_df = pd.DataFrame(
            {"1": [1, 2, 3, None, None], "2": [None, None, 3, 4, 5]},
            index=[utils._time.ms_to_datetime(ms) for ms in [1, 2, 3, 4, 5]],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas())

    def test_datapoints_list_empty(self, cognite_client):
        dps_list = DatapointsList([])
        assert dps_list.to_pandas().empty

    def test_retrieve_dataframe(self, cognite_client, mock_get_datapoints):
        df = cognite_client.datapoints.retrieve_dataframe(
            id=[1, {"id": 2, "aggregates": ["max"]}],
            external_id=["123"],
            start=1000000,
            end=1100000,
            aggregates=["average"],
            granularity="10s",
        )

        assert {"1|average", "2|max", "123|average"} == set(df.columns)
        assert df.shape[0] > 0

    def test_retrieve_datapoints_some_aggregates_omitted(
        self, cognite_client, mock_get_datapoints_one_ts_has_missing_aggregates
    ):
        import pandas as pd

        df = cognite_client.datapoints.retrieve_dataframe(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            aggregates=[],
            granularity="1s",
        )

        expected_df = pd.DataFrame(
            {"1|average": [0, 1, 2, 3, 4], "def|interpolation": [None, 1, None, 3, None]},
            index=[utils._time.ms_to_datetime(i) for i in range(5)],
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_datapoints_last_beyond_end(self, cognite_client, mock_get_datapoints_include_outside):
        dpt = cognite_client.datapoints.retrieve(
            id=1, include_outside_points=True, start=1000000000, end=1000000000 + 100000
        )
        assert 100001 == len(dpt)

    def test_retrieve_dataframe_several_missing(self, cognite_client, mock_get_datapoints_several_missing):
        import pandas as pd

        df = cognite_client.datapoints.retrieve_dataframe(
            id=[
                {"id": 1, "aggregates": ["average"]},
                {"id": 2, "aggregates": ["interpolation"]},
                {"id": 3, "aggregates": ["count"]},
            ],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
        )

        expected_df = pd.DataFrame(
            {"1|average": [11, 22, 44, None], "2|interpolation": [None, 1, 3, None], "3|count": [None, 2, 4, 5]},
            index=[utils._time.ms_to_datetime(i * 1000) for i in [0, 1, 3, 4]],
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_dataframe_complete_single_isstep(self, cognite_client, mock_get_datapoints_single_isstep):
        import pandas as pd

        df = cognite_client.datapoints.retrieve_dataframe(
            id=[{"id": 3, "aggregates": ["interpolation"]}],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
            complete="fill",
            include_aggregate_name=False,
        )

        expected_df = pd.DataFrame(
            {"3": [1.0, 1.0, 3.0]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [1, 2, 3]]
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_dataframe_several_missing_complete(self, cognite_client, mock_get_datapoints_several_missing):
        import pandas as pd

        df = cognite_client.datapoints.retrieve_dataframe(
            id=[
                {"id": 1, "aggregates": ["average"]},
                {"id": 2, "aggregates": ["interpolation"]},
                {"id": 3, "aggregates": ["count"]},
            ],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
            complete="fill",
        )

        expected_df = pd.DataFrame(
            {
                "1|average": [11.0, 22.0, None, 44.0, None],
                "2|interpolation": [None, 1.0, 2.0, 3.0, None],
                "3|count": [0.0, 2.0, 0.0, 4.0, 5.0],
            },
            index=[utils._time.ms_to_datetime(i * 1000) for i in range(5)],
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_dataframe_dict_empty(self, cognite_client, mock_get_datapoints_empty):
        dfd = cognite_client.datapoints.retrieve_dataframe_dict(
            id=1,
            aggregates=["count", "interpolation", "stepInterpolation", "totalVariation"],
            start=0,
            end=1,
            granularity="1s",
        )
        assert isinstance(dfd, dict)
        assert 4 == len(dfd)

    def test_retrieve_dataframe_dict_empty_single_aggregate(self, cognite_client, mock_get_datapoints_empty):
        dfd = cognite_client.datapoints.retrieve_dataframe_dict(
            id=1, aggregates=["count"], start=0, end=1, granularity="1s"
        )
        assert isinstance(dfd, dict)
        assert ["count"] == list(dfd.keys())
        assert dfd["count"].empty

    def test_retrieve_dataframe_complete_all(self, cognite_client, mock_get_datapoints):
        import pandas as pd

        df = cognite_client.datapoints.retrieve_dataframe(
            id=[1, 2],
            aggregates=["count", "sum", "average", "totalVariation"],
            start=0,
            end=1,
            granularity="1s",
            complete="fill",
        )
        assert isinstance(df, pd.DataFrame)
        assert 8 == df.shape[1]

    def test_retrieve_dataframe_dict(self, cognite_client, mock_get_datapoints_several_missing):
        import pandas as pd

        dfd = cognite_client.datapoints.retrieve_dataframe_dict(
            id=[
                {"id": 1, "aggregates": ["average"]},
                {"id": 2, "aggregates": ["interpolation"]},
                {"id": 3, "aggregates": ["count"]},
            ],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
        )
        assert isinstance(dfd, dict)
        assert 3 == len(dfd)

        expected_dict = {
            "average": pd.DataFrame(
                {"1": [11.0, 22.0, 44.0, None]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [0, 1, 3, 4]]
            ),
            "count": pd.DataFrame(
                {"3": [None, 2, 4, 5]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [0, 1, 3, 4]]
            ),
            "interpolation": pd.DataFrame(
                {"2": [None, 1, 3, None]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [0, 1, 3, 4]]
            ),
        }
        for k in expected_dict:
            pd.testing.assert_frame_equal(expected_dict[k], dfd[k])

    def test_retrieve_dataframe_dict_complete(self, cognite_client, mock_get_datapoints_several_missing):
        import pandas as pd

        dfd = cognite_client.datapoints.retrieve_dataframe_dict(
            id=[{"id": 2, "aggregates": ["interpolation"]}, {"id": 3, "aggregates": ["count"]}],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
            complete="fill,dropna",
        )
        assert isinstance(dfd, dict)
        assert 2 == len(dfd)

        expected_dict = {
            "count": pd.DataFrame(
                {"3": [2.0, 0.0, 4.0]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [1, 2, 3]]
            ),
            "interpolation": pd.DataFrame(
                {"2": [1.0, 2.0, 3.0]}, index=[utils._time.ms_to_datetime(i * 1000) for i in [1, 2, 3]]
            ),
        }

        for k in expected_dict:
            pd.testing.assert_frame_equal(expected_dict[k], dfd[k])

        with pytest.raises(ValueError, match="is not supported for dataframe completion"):
            dfd = cognite_client.datapoints.retrieve_dataframe_dict(
                id=[{"id": 1, "aggregates": ["average"]}],
                aggregates=[],
                start=0,
                end=1,
                granularity="1s",
                complete="fill,dropna",
            )

    def test_retrieve_dataframe_id_and_external_id_requested(self, cognite_client, rsps):
        rsps.add(
            rsps.POST,
            cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
            status=200,
            json={
                "items": [
                    {
                        "id": 1,
                        "externalId": "abc",
                        "isString": False,
                        "isStep": False,
                        "datapoints": [{"timestamp": 0, "average": 1}],
                    }
                ]
            },
        )
        rsps.add(
            rsps.POST,
            cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
            status=200,
            json={
                "items": [
                    {
                        "id": 2,
                        "externalId": "def",
                        "isString": False,
                        "isStep": False,
                        "datapoints": [{"timestamp": 0, "average": 1}],
                    }
                ]
            },
        )
        res = cognite_client.datapoints.retrieve_dataframe(
            start=0, end="now", id=1, external_id=["def"], aggregates=["average"], granularity="1m"
        )
        assert {"1|average", "def|average"} == set(res.columns)

    def test_insert_dataframe(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, 3, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        res = cognite_client.datapoints.insert_dataframe(df)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        assert {
            "items": [
                {
                    "id": 123,
                    "datapoints": [{"timestamp": ts, "value": val} for ts, val in zip(timestamps, range(1, 5))],
                },
                {
                    "id": 456,
                    "datapoints": [{"timestamp": ts, "value": float(val)} for ts, val in zip(timestamps, range(5, 9))],
                },
            ]
        } == request_body

    def test_insert_dataframe_external_ids(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, 3, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        res = cognite_client.datapoints.insert_dataframe(df, external_id_headers=True)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        assert {
            "items": [
                {
                    "externalId": "123",
                    "datapoints": [{"timestamp": ts, "value": val} for ts, val in zip(timestamps, range(1, 5))],
                },
                {
                    "externalId": "456",
                    "datapoints": [{"timestamp": ts, "value": float(val)} for ts, val in zip(timestamps, range(5, 9))],
                },
            ]
        } == request_body

    def test_insert_dataframe_with_nans(self, cognite_client):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, None, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        with pytest.raises(AssertionError, match="contains NaNs"):
            cognite_client.datapoints.insert_dataframe(df)

    def test_insert_dataframe_with_dropna(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, None, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        res = cognite_client.datapoints.insert_dataframe(df, dropna=True)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        assert {
            "items": [
                {
                    "id": 123,
                    "datapoints": [
                        {"timestamp": ts, "value": val} for ts, val in zip(timestamps, range(1, 5)) if val != 3
                    ],
                },
                {
                    "id": 456,
                    "datapoints": [{"timestamp": ts, "value": float(val)} for ts, val in zip(timestamps, range(5, 9))],
                },
            ]
        } == request_body

    def test_insert_dataframe_single_dp(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000]
        df = pd.DataFrame({"a": [1.0], "b": [2.0]}, index=[utils._time.ms_to_datetime(ms) for ms in timestamps])
        res = cognite_client.datapoints.insert_dataframe(df, external_id_headers=True)
        assert res is None

    def test_insert_dataframe_with_infs(self, cognite_client):
        import numpy as np
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, np.inf, 4], "456": [5.0, 6.0, 7.0, 8.0], "xyz": ["a", "b", "c", "d"]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        with pytest.raises(AssertionError, match="contains Infinity"):
            cognite_client.datapoints.insert_dataframe(df)

    def test_insert_dataframe_with_strings(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": ["a", "b", "c", "d"], "456": [5.0, 6.0, 7.0, 8.0]},
            index=[utils._time.ms_to_datetime(ms) for ms in timestamps],
        )
        cognite_client.datapoints.insert_dataframe(df)

    def test_retrieve_datapoints_multiple_time_series_correct_ordering(self, cognite_client, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = cognite_client.datapoints.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
        assert list(dps_res_list.to_pandas().columns) == ["1", "2", "3", "4", "5", "6"], "Incorrect column ordering"

    def test_retrieve_datapoints_one_ts_empty_correct_number_of_columns(
        self, cognite_client, mock_get_datapoints_one_ts_empty
    ):
        res = cognite_client.datapoints.retrieve(id=[1, 2], start=0, end=10000)
        assert 2 == len(res.to_pandas().columns)


@pytest.fixture
def mock_get_dps_count(rsps, cognite_client):
    def request_callback(request):
        payload = jsgz_load(request.body)
        granularity = payload["granularity"]
        start = payload["start"]
        end = payload["end"]

        assert payload["aggregates"] == ["count"]
        assert utils._time.granularity_to_ms(payload["granularity"]) >= utils._time.granularity_to_ms("1d")

        dps = [{"timestamp": i, "count": 1000} for i in range(start, end, utils._time.granularity_to_ms(granularity))]
        response = {"items": [{"id": 0, "externalId": "bla", "isStep": False, "isString": False, "datapoints": dps}]}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        cognite_client.datapoints._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


class TestDataPoster:
    def test_datapoints_bin_add_dps_object(self, cognite_client):
        bin = DatapointsBin(10, 10)
        dps_object = {"id": 100, "datapoints": [{"timestamp": 1, "value": 1}]}
        bin.add(dps_object)
        assert 1 == bin.current_num_datapoints
        assert [dps_object] == bin.dps_object_list

    def test_datapoints_bin_will_fit__below_dps_and_ts_limit(self, cognite_client):
        bin = DatapointsBin(10, 10)
        assert bin.will_fit(10)
        assert not bin.will_fit(11)

    def test_datapoints_bin_will_fit__below_dps_limit_above_ts_limit(self, cognite_client):
        bin = DatapointsBin(1, 100)
        dps_object = {"id": 100, "datapoints": [{"timestamp": 1, "value": 1}]}
        bin.add(dps_object)
        assert not bin.will_fit(10)

    def test_datapoints_bin_will_fit__above_dps_limit_above_ts_limit(self, cognite_client):
        bin = DatapointsBin(1, 1)
        dps_object = {"id": 100, "datapoints": [{"timestamp": 1, "value": 1}]}
        bin.add(dps_object)
        assert not bin.will_fit(1)


gms = utils._time.granularity_to_ms


class TestDataFetcher:
    @pytest.mark.parametrize(
        "q, exc, message",
        [
            (
                DatapointsQuery(start=1, end=2, id=1, aggregates=["average"]),
                ValueError,
                "granularity must also be provided",
            ),
            (DatapointsQuery(start=1, end=2, id=1, granularity="1d"), ValueError, "aggregates must also be provided"),
            (
                DatapointsQuery(start=1, end=2, id=[1, 1], granularity="1d", aggregates=["average"]),
                ValueError,
                "identifier '1' is duplicated in query",
            ),
            (
                DatapointsQuery(
                    start=1, end=2, id=[1, {"id": 1, "aggregates": ["max"]}], granularity="1d", aggregates=["average"]
                ),
                ValueError,
                "identifier '1' is duplicated in query",
            ),
        ],
    )
    def test_validate_query(self, cognite_client, q, exc, message):
        with pytest.raises(exc, match=message):
            DatapointsFetcher(cognite_client.datapoints).fetch(q)

    @pytest.mark.parametrize(
        "fn",
        [
            lambda cognite_client: (
                [_DPTask(cognite_client.datapoints, 1, 2, {}, None, None, None, None, False)],
                [_DPTask(cognite_client.datapoints, 1, 2, {}, None, None, None, None, False)],
            ),
            lambda cognite_client: (
                [
                    _DPTask(
                        cognite_client.datapoints,
                        datetime(2018, 1, 1),
                        datetime(2019, 1, 1),
                        {},
                        None,
                        None,
                        None,
                        None,
                        False,
                    )
                ],
                [_DPTask(cognite_client.datapoints, 1514764800000, 1546300800000, {}, None, None, None, None, False)],
            ),
            lambda cognite_client: (
                [_DPTask(cognite_client.datapoints, gms("1h"), gms(("25h")), {}, ["average"], "1d", None, None, False)],
                [_DPTask(cognite_client.datapoints, gms("1d"), gms("2d"), {}, ["average"], "1d", None, None, False)],
            ),
        ],
    )
    def test_preprocess_tasks(self, cognite_client, fn):
        q, expected_q = fn(cognite_client)
        DatapointsFetcher(cognite_client.datapoints)._preprocess_tasks(q)
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
    def test_align_with_granularity_unit(self, cognite_client, ts, granularity, expected_output):
        assert expected_output == DatapointsFetcher._align_with_granularity_unit(ts, granularity)

    @pytest.mark.parametrize(
        "start, end, granularity, request_limit, user_limit, expected_output",
        [
            (0, gms("20d"), "10d", 2, None, [_DPWindow(0, 1728000000)]),
            (0, gms("20d"), "10d", 1, None, [_DPWindow(0, 864000000), _DPWindow(864000000, 1728000000)]),
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
            (0, gms("1h"), None, 2000, None, [_DPWindow(0, 3600000)]),
            (0, gms("1s"), None, 1, None, [_DPWindow(0, 1000)]),
            (0, gms("1s"), None, 1, None, [_DPWindow(0, 1000)]),
            (0, gms("3d"), None, 1000, 500, [_DPWindow(0, gms("1d"))]),
        ],
    )
    def test_get_datapoints_windows(
        self, cognite_client, start, end, granularity, request_limit, user_limit, expected_output, mock_get_dps_count
    ):
        user_limit = user_limit or float("inf")
        task = _DPTask(
            client=cognite_client.datapoints,
            start=start,
            end=end,
            ts_item={},
            granularity=granularity,
            aggregates=[],
            limit=None,
            include_outside_points=False,
            ignore_unknown_ids=False,
        )
        task.request_limit = request_limit
        res = DatapointsFetcher(cognite_client.datapoints)._get_windows(
            id=0, task=task, remaining_user_limit=user_limit
        )
        for w in expected_output:
            w.limit = user_limit
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
    def test_align_window_end(self, cognite_client, start, end, granularity, expected_output):
        assert expected_output == DatapointsFetcher._align_window_end(start, end, granularity)

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
    def test_process_time_series_input_ok(self, cognite_client, ids, external_ids, expected_output):
        assert expected_output == DatapointsFetcher._process_ts_identifiers(ids, external_ids)

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
    def test_process_time_series_input_fail(self, cognite_client, ids, external_ids, exception, match):
        with pytest.raises(exception, match=match):
            DatapointsFetcher._process_ts_identifiers(ids, external_ids)
