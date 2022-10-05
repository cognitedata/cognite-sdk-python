import json
import math
import re
from datetime import datetime, timezone
from random import random
from unittest.mock import patch

import pytest

from cognite.client._api.datapoints import DatapointsBin
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsList
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicateColumnsError, CogniteNotFoundError
from cognite.client.utils._time import granularity_to_ms
from tests.utils import jsgz_load

DATAPOINTS_API = "cognite.client._api.datapoints.{}"
DPS_DATA_CLASSES = "cognite.client.data_classes.datapoints.{}"


def generate_datapoints(start: int, end: int, aggregates=None, granularity=None):
    dps = []
    granularity = granularity_to_ms(granularity) if granularity else 1000
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={"items": [{"id": 2, "externalId": "2", "isString": False, "isStep": False, "datapoints": []}]},
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_one_ts_has_missing_aggregates(rsps, cognite_client):
    def callback(request):
        body = jsgz_load(request.body)
        assert len(body["items"]) == 1
        item = body["items"][0]
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_get_datapoints_several_missing(rsps, cognite_client):
    def callback(request):
        body = jsgz_load(request.body)
        assert len(body["items"]) == 1
        item = body["items"][0]
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/byids",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
        callback=callback,
        content_type="application/json",
    )
    yield rsps


def assert_dps_response_is_correct(calls, dps_object):
    datapoints = []
    for call in calls:
        body = jsgz_load(call.request.body)
        assert len(body["items"]) == 1
        item = body["items"][0]
        if item["limit"] > 1 and item.get("aggregates") != ["count"]:
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
        dps_res = cognite_client.time_series.data.retrieve(id=123, start=1000000, end=1100000)
        assert isinstance(dps_res, Datapoints)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_500(self, cognite_client, rsps):
        rsps.add(
            rsps.POST,
            cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
            json={"error": {"code": 500, "message": "Internal Server Error"}},
            status=500,
        )
        with pytest.raises(CogniteAPIError):
            cognite_client.time_series.data.retrieve(id=123, start=1000000, end=1100000)

    def test_retrieve_datapoints_by_external_id(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.time_series.data.retrieve(external_id="123", start=1000000, end=1100000)
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_aggregates(self, cognite_client, mock_get_datapoints):
        dps_res = cognite_client.time_series.data.retrieve(
            id=123, start=1000000, end=1100000, aggregates=["average", "step_interpolation"], granularity="10s"
        )
        assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    def test_retrieve_datapoints_local_aggregates(self, cognite_client, mock_get_datapoints):
        dps_res_list = cognite_client.time_series.data.retrieve(
            external_id={"externalId": "123", "aggregates": ["average"]},
            id={"id": 234},
            start=1000000,
            end=1100000,
            aggregates=["max"],
            granularity="10s",
        )
        for dps_res in dps_res_list:
            assert_dps_response_is_correct(mock_get_datapoints.calls, dps_res)

    @pytest.mark.dsl  # TODO: Revert to old code and use `retrieve`, not `retrieve_arrays`
    def test_retrieve_datapoints_some_aggregates_omitted(
        self, cognite_client, mock_get_datapoints_one_ts_has_missing_aggregates
    ):
        import numpy as np

        dps_res_list = cognite_client.time_series.data.retrieve_arrays(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            granularity="1s",
        )
        for dps in dps_res_list:
            if dps.id == 1:
                np.testing.assert_array_equal(dps.average, [0, 1, 2, 3, 4])
            elif dps.id == 2:
                np.testing.assert_array_equal(dps.interpolation, [np.nan, 1, np.nan, 3, np.nan])

    def test_datapoints_paging_with_limit(self, cognite_client, mock_get_datapoints):
        with patch(DPS_DATA_CLASSES.format("DPS_LIMIT_AGG"), 3):
            with patch(DATAPOINTS_API.format("DPS_LIMIT_AGG"), 3):
                dps_res = cognite_client.time_series.data.retrieve(
                    id=123, start=0, end=10000, aggregates=["average"], granularity="1s", limit=4
                )
        assert 4 == len(dps_res)

    def test_retrieve_datapoints_multiple_time_series(self, cognite_client, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = cognite_client.time_series.data.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
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
        res = cognite_client.time_series.data.retrieve(id=1, start=0, end=10000)
        assert 0 == len(res)

    def test_retrieve_datapoints_empty_extrafields_set(self, cognite_client, mock_get_datapoints_empty):
        res = cognite_client.time_series.data.retrieve(id=1, start=0, end=10000)
        assert "kPa" == res.unit
        assert res.is_step is False
        assert res.is_string is False

    def test_aggregate_limits_correct(self, cognite_client, mock_get_datapoints):
        cognite_client.time_series.data.retrieve(
            id={"id": 1, "aggregates": ["average"]}, start=0, end=10, granularity="1d"
        )
        cognite_client.time_series.data.retrieve(id=1, start=0, end=10, granularity="1d", aggregates=["max"])
        cognite_client.time_series.data.retrieve(id=1, start=0, end=10)
        for i, limit in zip(range(3), [10_000, 10_000, 100_000]):
            body = jsgz_load(mock_get_datapoints.calls[i].request.body)
            assert len(body["items"]) == 1
            assert limit == body["items"][0]["limit"]


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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest",
        callback=request_callback,
        content_type="application/json",
    )
    yield rsps


@pytest.fixture
def mock_retrieve_latest_empty(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest",
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
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest",
        status=500,
        json={"error": {"code": 500, "message": "Internal Server Error"}},
    )
    yield rsps


class TestGetLatest:
    def test_retrieve_latest(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.time_series.data.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 10000 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.time_series.data.retrieve_latest(id=1, external_id="2")
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 10000 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_with_before(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.time_series.data.retrieve_latest(id=1, before=10)
        assert isinstance(res, Datapoints)
        assert 9 == res[0].timestamp
        assert isinstance(res[0].value, float)

    def test_retrieve_latest_multiple_ts_with_before(self, cognite_client, mock_retrieve_latest):
        res = cognite_client.time_series.data.retrieve_latest(id=[1, 2], external_id=["1", "2"], before=10)
        assert isinstance(res, DatapointsList)
        for dps in res:
            assert 9 == dps[0].timestamp
            assert isinstance(dps[0].value, float)

    def test_retrieve_latest_empty(self, cognite_client, mock_retrieve_latest_empty):
        res = cognite_client.time_series.data.retrieve_latest(id=1)
        assert isinstance(res, Datapoints)
        assert 0 == len(res)

    def test_retrieve_latest_multiple_ts_empty(self, cognite_client, mock_retrieve_latest_empty):
        res_list = cognite_client.time_series.data.retrieve_latest(id=[1, 2])
        assert isinstance(res_list, DatapointsList)
        assert 2 == len(res_list)
        for res in res_list:
            assert 0 == len(res)

    def test_retrieve_latest_concurrent_fails(self, cognite_client, mock_retrieve_latest_with_failure):
        with patch(DATAPOINTS_API.format("RETRIEVE_LATEST_LIMIT"), 2):
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.time_series.data.retrieve_latest(id=[1, 2, 3])
            assert e.value.code == 500


@pytest.fixture
def mock_post_datapoints(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data",
        status=200,
        json={},
    )
    yield rsps


@pytest.fixture
def mock_post_datapoints_400(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data",
        status=400,
        json={"error": {"message": "Ts not found", "missing": [{"externalId": "does_not_exist"}]}},
    )
    yield rsps


class TestInsertDatapoints:
    def test_insert_tuples(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        res = cognite_client.time_series.data.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_insert_dicts(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        res = cognite_client.time_series.data.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    def test_by_external_id(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        cognite_client.time_series.data.insert(dps, external_id="1")
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}
            ]
        } == jsgz_load(mock_post_datapoints.calls[0].request.body)

    @pytest.mark.parametrize("ts_key, value_key", [("timestamp", "values"), ("timstamp", "value")])
    def test_invalid_datapoints_keys(self, cognite_client, ts_key, value_key):
        dps = [{ts_key: i * 1e11, value_key: i} for i in range(1, 11)]
        with pytest.raises(AssertionError, match="is missing the"):
            cognite_client.time_series.data.insert(dps, id=1)

    def test_insert_datapoints_over_limit(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        with patch(DATAPOINTS_API.format("DPS_LIMIT"), 5):
            with patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 5):
                res = cognite_client.time_series.data.insert(dps, id=1)
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
            cognite_client.time_series.data.insert(id=1, datapoints=[])

    def test_insert_datapoints_in_multiple_time_series(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        dps_objects = [{"externalId": "1", "datapoints": dps}, {"id": 1, "datapoints": dps}]
        res = cognite_client.time_series.data.insert_multiple(dps_objects)
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
            cognite_client.time_series.data.insert_multiple(dps_objects)

    def test_insert_datapoints_ts_does_not_exist(self, cognite_client, mock_post_datapoints_400):
        with pytest.raises(CogniteNotFoundError):
            cognite_client.time_series.data.insert(datapoints=[(1e14, 1)], external_id="does_not_exist")

    def test_insert_multiple_ts__below_ts_and_dps_limit(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(100)]
        cognite_client.time_series.data.insert_multiple(dps_objects)
        assert 1 == len(mock_post_datapoints.calls)
        request_body = jsgz_load(mock_post_datapoints.calls[0].request.body)
        for i, dps in enumerate(request_body["items"]):
            assert i == dps["id"]

    def test_insert_multiple_ts_single_call__below_dps_limit_above_ts_limit(self, cognite_client, mock_post_datapoints):
        with patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 100):
            dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
            dps_objects = [{"id": i, "datapoints": dps} for i in range(101)]
            cognite_client.time_series.data.insert_multiple(dps_objects)
            assert 2 == len(mock_post_datapoints.calls)

    def test_insert_multiple_ts_single_call__above_dps_limit_below_ts_limit(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 1002)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(10)]
        cognite_client.time_series.data.insert_multiple(dps_objects)
        assert 2 == len(mock_post_datapoints.calls)


@pytest.fixture
def mock_delete_datapoints(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/delete",
        status=200,
        json={},
    )
    yield rsps


class TestDeleteDatapoints:
    def test_delete_range(self, cognite_client, mock_delete_datapoints):
        res = cognite_client.time_series.data.delete_range(
            start=datetime(2018, 1, 1, tzinfo=timezone.utc), end=datetime(2018, 1, 2, tzinfo=timezone.utc), id=1
        )
        assert res is None
        assert {"items": [{"id": 1, "inclusiveBegin": 1514764800000, "exclusiveEnd": 1514851200000}]} == jsgz_load(
            mock_delete_datapoints.calls[0].request.body
        )

    @pytest.mark.parametrize(
        "id, external_id, exception",
        [(None, None, ValueError), (1, "1", ValueError)],
    )
    def test_delete_range_invalid_id(self, cognite_client, id, external_id, exception):
        with pytest.raises(exception):
            cognite_client.time_series.data.delete_range("1d-ago", "now", id, external_id)

    def test_delete_range_start_after_end(self, cognite_client):
        with pytest.raises(AssertionError, match="must be"):
            cognite_client.time_series.data.delete_range(1, 0, 1)

    def test_delete_ranges(self, cognite_client, mock_delete_datapoints):
        ranges = [{"id": 1, "start": 0, "end": 1}, {"externalId": "1", "start": 0, "end": 1}]
        cognite_client.time_series.data.delete_ranges(ranges)
        assert {
            "items": [
                {"id": 1, "inclusiveBegin": 0, "exclusiveEnd": 1},
                {"externalId": "1", "inclusiveBegin": 0, "exclusiveEnd": 1},
            ]
        } == jsgz_load(mock_delete_datapoints.calls[0].request.body)

    def test_delete_ranges_invalid_ids(self, cognite_client):
        ranges = [{"idz": 1, "start": 0, "end": 1}]
        with pytest.raises(AssertionError, match="Invalid key 'idz'"):
            cognite_client.time_series.data.delete_ranges(ranges)
        ranges = [{"start": 0, "end": 1}]
        with pytest.raises(ValueError, match="Exactly one of id or external id must be specified"):
            cognite_client.time_series.data.delete_ranges(ranges)


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
                "isStep": False,
                "isString": True,
                "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}],
            }
        )
        assert 1 == res.id
        assert "1" == res.external_id
        assert [1, 2] == res.timestamp
        assert [1, 2] == res.value
        assert res.is_string is True
        assert res.is_step is False
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
class TestPandasIntegration:
    def test_datapoint(self, cognite_client):
        import pandas as pd

        d = Datapoint(timestamp=0, value=2, max=3)
        expected_df = pd.DataFrame({"value": [2], "max": [3]}, index=[pd.Timestamp(0, unit="ms")])
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(), check_like=True)

    def test_datapoints(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"1|average": [2, 3, 4], "1|step_interpolation": [3, 4, 5]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_no_names(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4])
        expected_df = pd.DataFrame({"1": [2, 3, 4]}, index=pd.to_datetime(range(1, 4), unit="ms"))
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=False))
        expected_df = pd.DataFrame({"1|average": [2, 3, 4]}, index=pd.to_datetime(range(1, 4), unit="ms"))
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=True))

    def test_id_and_external_id_set_gives_external_id_columns(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=0, external_id="abc", timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"abc|average": [2, 3, 4], "abc|step_interpolation": [3, 4, 5]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
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
                "1|step_interpolation": [3, 4, 5],
                "2|max": [2, 3, 4],
                "2|step_interpolation": [3, 4, 5],
                "3": [1, None, 3],
            },
            index=pd.to_datetime(range(1, 4), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)

    def test_datapoints_list_names(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4])
        d2 = Datapoints(id=3, timestamp=[1, 3], average=[1, 3])
        dps_list = DatapointsList([d1, d2])
        expected_df = pd.DataFrame(
            {"2|max": [2, 3, 4], "3|average": [1, None, 3]}, index=pd.to_datetime(range(1, 4), unit="ms")
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)
        expected_df.columns = [c[:1] for c in expected_df.columns]
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(include_aggregate_name=False), check_freq=False)

    def test_datapoints_list_names_dup(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4])
        d2 = Datapoints(id=2, timestamp=[1, 3], average=[1, 3])
        dps_list = DatapointsList([d1, d2])
        expected_df = pd.DataFrame(
            {"2|max": [2, 3, 4], "2|average": [1, None, 3]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
            columns=["2|max", "2|average"],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)
        with pytest.raises(CogniteDuplicateColumnsError):
            dps_list.to_pandas(include_aggregate_name=False)

    def test_datapoints_list_non_aligned(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3])
        d2 = Datapoints(id=2, timestamp=[3, 4, 5], value=[3, 4, 5])

        dps_list = DatapointsList([d1, d2])

        expected_df = pd.DataFrame(
            {"1": [1, 2, 3, None, None], "2": [None, None, 3, 4, 5]},
            index=pd.to_datetime(range(1, 6), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)

    def test_datapoints_list_empty(self, cognite_client):
        dps_list = DatapointsList([])
        assert dps_list.to_pandas().empty

    def test_retrieve_dataframe(self, cognite_client, mock_get_datapoints):
        df = cognite_client.time_series.data.retrieve_dataframe(
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

        df = cognite_client.time_series.data.retrieve_dataframe(
            id={"id": 1, "aggregates": ["average"]},
            external_id={"externalId": "def", "aggregates": ["interpolation"]},
            start=0,
            end=1,
            aggregates=[],
            granularity="1s",
            column_names="external_id",
        )

        expected_df = pd.DataFrame(
            {"abc|average": [0.0, 1, 2, 3, 4], "def|interpolation": [None, 1, None, 3, None]},
            index=pd.to_datetime(range(5), unit="ms"),
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_retrieve_dataframe_several_missing(self, cognite_client, mock_get_datapoints_several_missing):
        import pandas as pd

        df = cognite_client.time_series.data.retrieve_dataframe(
            id=[
                {"id": 1, "aggregates": ["average"]},
                {"id": 2, "aggregates": ["interpolation"]},
                {"id": 3, "aggregates": ["count"]},
            ],
            aggregates=[],
            start=0,
            end=1,
            granularity="1s",
            column_names="id",
        )

        expected_df = pd.DataFrame(
            {"1|average": [11, 22, 44, None], "2|interpolation": [None, 1, 3, None], "3|count": [None, 2, 4, 5]},
            index=pd.to_datetime([0, 1, 3, 4], unit="s"),
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_insert_dataframe(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, 3, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        res = cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False)
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
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        res = cognite_client.time_series.data.insert_dataframe(df, external_id_headers=True)
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
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        with pytest.raises(ValueError, match="contains one or more NaNs"):
            cognite_client.time_series.data.insert_dataframe(df, dropna=False)

    def test_insert_dataframe_with_dropna(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, None, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        res = cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False, dropna=True)
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
        df = pd.DataFrame({"a": [1.0], "b": [2.0]}, index=pd.to_datetime(timestamps, unit="ms"))
        res = cognite_client.time_series.data.insert_dataframe(df, external_id_headers=True)
        assert res is None

    def test_insert_dataframe_with_infs(self, cognite_client):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, math.inf, 4], "456": [5.0, 6.0, 7.0, 8.0], "xyz": ["a", "b", "c", "d"]},
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        with pytest.raises(ValueError, match=re.escape("contains one or more (+/-) Infinity")):
            cognite_client.time_series.data.insert_dataframe(df)

    def test_insert_dataframe_with_strings(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": ["a", "b", "c", "d"], "456": [5.0, 6.0, 7.0, 8.0]},
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        cognite_client.time_series.data.insert_dataframe(df)

    def test_retrieve_datapoints_multiple_time_series_correct_ordering(self, cognite_client, mock_get_datapoints):
        ids = [1, 2, 3]
        external_ids = ["4", "5", "6"]
        dps_res_list = cognite_client.time_series.data.retrieve(id=ids, external_id=external_ids, start=0, end=100000)
        assert list(dps_res_list.to_pandas().columns) == ["1", "2", "3", "4", "5", "6"], "Incorrect column ordering"

    def test_retrieve_datapoints_one_ts_empty_correct_number_of_columns(
        self, cognite_client, mock_get_datapoints_one_ts_empty
    ):
        res = cognite_client.time_series.data.retrieve(id=[1, 2], start=0, end=10000)
        assert 2 == len(res.to_pandas().columns)


@pytest.fixture
def mock_get_dps_count(rsps, cognite_client):
    def request_callback(request):
        payload = jsgz_load(request.body)
        granularity = payload["granularity"]
        start = payload["start"]
        end = payload["end"]

        assert payload["aggregates"] == ["count"]
        assert granularity_to_ms(payload["granularity"]) >= granularity_to_ms("1d")

        dps = [{"timestamp": i, "count": 1000} for i in range(start, end, granularity_to_ms(granularity))]
        response = {"items": [{"id": 0, "externalId": "bla", "isStep": False, "isString": False, "datapoints": dps}]}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/list",
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


class TestDataFetcher:
    pass  # TODO(haakonvt): Get to it!
