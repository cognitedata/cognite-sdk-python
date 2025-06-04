from __future__ import annotations

import math
import re
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from random import randint, random, shuffle

import pytest
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse


import cognite.client._api.datapoints as dps_api  # for mocking
from cognite.client import CogniteClient
from cognite.client._api.datapoints import _InsertDatapoint
from cognite.client.data_classes import Datapoint, Datapoints, DatapointsList, LatestDatapointQuery
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import _json
from cognite.client.utils._time import ZoneInfo, granularity_to_ms
from tests.utils import jsgz_load, random_gamma_dist_integer

DATAPOINTS_API = "cognite.client._api.datapoints.{}"


def generate_datapoints(start: int, end: int, aggregates=None, granularity=None):
    dps = []
    granularity = granularity_to_ms(granularity) if granularity else 1000
    for i in range(start, end, granularity):
        dp = {}
        if aggregates:
            if aggregates == ["count"]:
                dp["count"] = math.ceil((end - start) / 1000)
            else:
                for agg in aggregates:
                    dp[agg] = random()
        else:
            dp["value"] = random()
        dp["timestamp"] = i
        dps.append(dp)
    return dps


@pytest.fixture
def mock_retrieve_latest(respx_mock, cognite_client):
    def request_callback_side_effect(request: HttpxRequest):
        payload = jsgz_load(request.content)
        items = []
        for latest_query in payload["items"]:
            id_val = latest_query.get("id", -1)
            external_id_val = latest_query.get("externalId", "-1")
            before = latest_query.get("before", 10001)
            items.append(
                {
                    "id": id_val,
                    "externalId": external_id_val,
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": before - 1, "value": random()}],
                }
            )
        return HttpxResponse(200, json={"items": items})

    respx_mock.post(
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest"
    ).mock(side_effect=request_callback_side_effect)
    yield respx_mock


@pytest.fixture
def mock_retrieve_latest_empty(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest"
    ).respond(
        status_code=200,
        json={
            "items": [
                {"id": 1, "externalId": "1", "isString": False, "isStep": True, "datapoints": []},
                {"id": 2, "isString": False, "isStep": False, "externalId": "2", "datapoints": []},
            ]
        },
    )
    yield respx_mock


@pytest.fixture
def mock_retrieve_latest_with_failure(respx_mock, cognite_client):
    latest_url = cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/latest"
    # First call, success
    respx_mock.post(latest_url).respond(
        status_code=200,
        json={
            "items": [
                {"id": 1, "externalId": "1", "isString": False, "isStep": False, "datapoints": []},
                {"id": 2, "externalId": "2", "isString": False, "isStep": False, "datapoints": []},
            ]
        },
    )
    # Second call, failure
    # To ensure this is picked up for a subsequent call to the same URL pattern,
    # we can define it after the first one. Respx matches routes in the order they are added
    # if multiple routes match the same request.
    respx_mock.post(latest_url).respond(
        status_code=500,
        json={"error": {"code": 500, "message": "Internal Server Error"}},
    )
    yield respx_mock


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

    def test_retrieve_latest_concurrent_fails(self, cognite_client, mock_retrieve_latest_with_failure, monkeypatch):
        monkeypatch.setattr(cognite_client.time_series.data, "_RETRIEVE_LATEST_LIMIT", 2)
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.time_series.data.retrieve_latest(id=[1, 2, 3]) 
        assert e.value.code == 500

    @pytest.mark.parametrize(
        "id, external_id, instance_id, pass_as, err_msg",
        (
            (None, None, None, "id", "Exactly one of id, external id, or instance_id must be specified, got neither"),
            (
                None,
                None,
                None,
                "external_id",
                "Exactly one of id, external id, or instance_id must be specified, got neither",
            ),
            (None, "foo", None, "id", "Missing 'id' from: 'LatestDatapointQuery"),
            (123, None, None, "external_id", "Missing 'external_id' from: 'LatestDatapointQuery"),
            (123, "foo", None, "id", "Exactly one of id, external id, or instance_id must be specified, got multiple"),
            (
                123,
                "foo",
                None,
                "external_id",
                "Exactly one of id, external id, or instance_id must be specified, got multiple",
            ),
        ),
    )
    def test_using_latest_datapoint_query__fails_wrong_ident(
        self, cognite_client, id, external_id, instance_id, pass_as, err_msg
    ):
        # Pass directly
        with pytest.raises(ValueError, match=err_msg):
            ldq = LatestDatapointQuery(id, external_id, instance_id)
            cognite_client.time_series.data.retrieve_latest(**{pass_as: ldq})

        # Pass as a part of a list
        with pytest.raises(ValueError, match=err_msg):
            ldq = LatestDatapointQuery(id, external_id, instance_id)
            valid = 123 if pass_as == "id" else "foo"
            cognite_client.time_series.data.retrieve_latest(**{pass_as: [valid, ldq, ldq, valid]})


@pytest.fixture
def mock_post_datapoints(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data"
    ).respond(status_code=200, json={})
    yield respx_mock


@pytest.fixture
def mock_post_datapoints_400(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data"
    ).respond(
        status_code=400,
        json={"error": {"message": "Ts not found", "missing": [{"externalId": "does_not_exist"}]}},
    )
    yield respx_mock


class TestInsertDatapoints:
    def test_insert_tuples(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        res = cognite_client.time_series.data.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls.last.request.content)

    def test_insert_dicts(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        res = cognite_client.time_series.data.insert(dps, id=1)
        assert res is None
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}]
        } == jsgz_load(mock_post_datapoints.calls.last.request.content)

    def test_by_external_id(self, cognite_client, mock_post_datapoints):
        dps = [(i * 1e11, i) for i in range(1, 11)]
        cognite_client.time_series.data.insert(dps, external_id="1")
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]}
            ]
        } == jsgz_load(mock_post_datapoints.calls.last.request.content)

    @pytest.mark.parametrize("ts_key, value_key", [("timestamp", "values"), ("timstamp", "value")])
    def test_invalid_datapoints_keys(self, cognite_client, ts_key, value_key):
        dps = [{ts_key: i * 1e11, value_key: i} for i in range(1, 11)]
        with pytest.raises(KeyError, match="A datapoint is missing one or both keys"):
            cognite_client.time_series.data.insert(dps, id=1)

    def test_insert_datapoints_over_limit(self, cognite_client, mock_post_datapoints, monkeypatch):
        monkeypatch.setattr(cognite_client.time_series.data, "_DPS_INSERT_LIMIT", 5)
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 5)
        dps = [(i * 1e11, i) for i in range(1, 11)]
        res = cognite_client.time_series.data.insert(dps, id=1)
        assert res is None
        request_bodies = [jsgz_load(call.request.content) for call in mock_post_datapoints.calls]
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 6)]}]
        } in request_bodies
        assert {
            "items": [{"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(6, 11)]}]
        } in request_bodies

    def test_insert_datapoints_no_data(self, cognite_client):
        with pytest.raises(ValueError, match="No datapoints provided"):
            cognite_client.time_series.data.insert(id=1, datapoints=[])

    def test_insert_datapoints_in_multiple_time_series(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        dps_objects = [{"externalId": "1", "datapoints": dps}, {"id": 1, "datapoints": dps}]
        res = cognite_client.time_series.data.insert_multiple(dps_objects)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls.last.request.content)
        assert {
            "items": [
                {"externalId": "1", "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]},
                {"id": 1, "datapoints": [{"timestamp": int(i * 1e11), "value": i} for i in range(1, 11)]},
            ]
        } == request_body

    def test_insert_datapoints_in_multiple_time_series_invalid_key(self, cognite_client):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 11)]
        dps_objects = [{"extId": "1", "datapoints": dps}]
        with pytest.raises(
            ValueError, match="Exactly one of id, external id, or instance_id must be specified, got neither"
        ):
            cognite_client.time_series.data.insert_multiple(dps_objects)

    def test_insert_datapoints_ts_does_not_exist(self, cognite_client, mock_post_datapoints_400):
        with pytest.raises(CogniteNotFoundError):
            cognite_client.time_series.data.insert(datapoints=[(1e14, 1)], external_id="does_not_exist")

    def test_insert_multiple_ts__below_ts_and_dps_limit(self, cognite_client, mock_post_datapoints):
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(1, 101)]
        cognite_client.time_series.data.insert_multiple(dps_objects)
        assert 1 == len(mock_post_datapoints.calls)
        request_body = jsgz_load(mock_post_datapoints.calls.last.request.content)
        for i, dps_item in enumerate(request_body["items"], 1):
            assert i == dps_item["id"]

    def test_insert_multiple_ts_single_call__below_dps_limit_above_ts_limit(
        self, cognite_client, mock_post_datapoints, monkeypatch
    ):
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 100)
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 2)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(1, 102)]
        cognite_client.time_series.data.insert_multiple(dps_objects)
        assert 2 == len(mock_post_datapoints.calls)

    def test_insert_multiple_ts_single_call__above_dps_limit_below_ts_limit(
        self, cognite_client, mock_post_datapoints, monkeypatch
    ):
        monkeypatch.setattr(cognite_client.time_series.data, "_DPS_INSERT_LIMIT", 10_000)
        dps = [{"timestamp": i * 1e11, "value": i} for i in range(1, 1002)]
        dps_objects = [{"id": i, "datapoints": dps} for i in range(1, 11)]
        cognite_client.time_series.data.insert_multiple(dps_objects)
        assert 2 == len(mock_post_datapoints.calls)


@pytest.fixture
def mock_delete_datapoints(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.time_series.data._get_base_url_with_base_path() + "/timeseries/data/delete"
    ).respond(status_code=200, json={})
    yield respx_mock


class TestDeleteDatapoints:
    def test_delete_range(self, cognite_client, mock_delete_datapoints):
        res = cognite_client.time_series.data.delete_range(
            start=datetime(2018, 1, 1, tzinfo=timezone.utc), end=datetime(2018, 1, 2, tzinfo=timezone.utc), id=1
        )
        assert res is None
        assert {"items": [{"id": 1, "inclusiveBegin": 1514764800000, "exclusiveEnd": 1514851200000}]} == jsgz_load(
            mock_delete_datapoints.calls.last.request.content
        )

    @pytest.mark.parametrize(
        "id, external_id, exception",
        [(None, None, ValueError), (1, "1", ValueError)],
    )
    def test_delete_range_invalid_id(self, cognite_client, id, external_id, exception):
        with pytest.raises(exception):
            cognite_client.time_series.data.delete_range("1d-ago", "now", id, external_id)

    def test_delete_range_start_after_end(self, cognite_client):
        with pytest.raises(ValueError, match="must be"):
            cognite_client.time_series.data.delete_range(1, 0, 1)

    def test_delete_ranges(self, cognite_client, mock_delete_datapoints):
        ranges = [{"id": 1, "start": 0, "end": 1}, {"externalId": "1", "start": 0, "end": 1}]
        cognite_client.time_series.data.delete_ranges(ranges)
        assert {
            "items": [
                {"id": 1, "inclusiveBegin": 0, "exclusiveEnd": 1},
                {"externalId": "1", "inclusiveBegin": 0, "exclusiveEnd": 1},
            ]
        } == jsgz_load(mock_delete_datapoints.calls.last.request.content)

    @pytest.mark.parametrize(
        "input_dct, err_suffix",
        (
            ({}, "neither"),
            ({"id": 1, "external_id": "a"}, "multiple"),
            ({"id": 1, "externalId": "a"}, "multiple"),
        ),
    )
    def test_delete_ranges_invalid_ids(self, input_dct, err_suffix, cognite_client):
        ranges = [{"start": 0, "end": 1, **input_dct}]
        with pytest.raises(
            ValueError, match=f"Exactly one of id, external id, or instance_id must be specified, got {err_suffix}"
        ):
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
        res = Datapoints.load(
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
        res = Datapoints.load(
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

    def test__extend(self, cognite_client):  # test _extend, not extend
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
            {"1|average": [2, 3, 4.0], "1|step_interpolation": [3, 4, 5.0]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_no_names(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4])
        expected_df = pd.DataFrame({"1": [2, 3, 4.0]}, index=pd.to_datetime(range(1, 4), unit="ms"))
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=False))
        expected_df = pd.DataFrame({"1|average": [2, 3, 4.0]}, index=pd.to_datetime(range(1, 4), unit="ms"))
        pd.testing.assert_frame_equal(expected_df, d.to_pandas(include_aggregate_name=True))

    def test_id_and_external_id_set_gives_external_id_columns(self, cognite_client):
        import pandas as pd

        d = Datapoints(id=0, external_id="abc", timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        expected_df = pd.DataFrame(
            {"abc|average": [2, 3, 4.0], "abc|step_interpolation": [3, 4, 5.0]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, d.to_pandas())

    def test_datapoints_empty(self, cognite_client):
        d = Datapoints(external_id="1", timestamp=[], value=[])
        assert d.to_pandas().empty

    def test_datapoints_list(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], average=[2, 3, 4], step_interpolation=[3, 4, 5])
        d2 = Datapoints(id=2, timestamp=[1, 2, 3], count=[2, 3, 4], step_interpolation=[3, 4, 5])
        d3 = Datapoints(id=3, timestamp=[1, 3, 4], value=[1, 3, 4.0])
        dps_list = DatapointsList([d1, d2, d3])
        expected_df = pd.DataFrame(
            {
                "1|average": [2, 3, 4, None],
                "1|step_interpolation": [3, 4, 5, None],
                "2|count": pd.array([2, 3, 4, None], dtype="Int64"),
                "2|step_interpolation": [3, 4, 5, None],
                "3": [1, None, 3, 4.0],
            },
            index=pd.to_datetime(range(1, 5), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)

    def test_datapoints_list_names(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=2, timestamp=[1, 2, 3], max=[2, 3, 4])
        d2 = Datapoints(id=3, timestamp=[1, 3], average=[1, 3])
        dps_list = DatapointsList([d1, d2])
        expected_df = pd.DataFrame(
            {"2|max": [2, 3, 4.0], "3|average": [1, None, 3]}, index=pd.to_datetime(range(1, 4), unit="ms")
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
            {"2|max": [2, 3, 4.0], "2|average": [1, None, 3]},
            index=pd.to_datetime(range(1, 4), unit="ms"),
            columns=["2|max", "2|average"],
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)
        dps_list.to_pandas(include_aggregate_name=False)
        assert True  # Duplicated columns names were not allowed prior to v5

    def test_datapoints_list_non_aligned(self, cognite_client):
        import pandas as pd

        d1 = Datapoints(id=1, timestamp=[1, 2, 3], value=[1, 2, 3.0])
        d2 = Datapoints(id=2, timestamp=[3, 4, 5], value=[3, 4, 5.0])

        dps_list = DatapointsList([d1, d2])

        expected_df = pd.DataFrame(
            {"1": [1, 2, 3, None, None], "2": [None, None, 3, 4, 5]},
            index=pd.to_datetime(range(1, 6), unit="ms"),
        )
        pd.testing.assert_frame_equal(expected_df, dps_list.to_pandas(), check_freq=False)

    def test_datapoints_list_empty(self, cognite_client):
        dps_list = DatapointsList([])
        assert dps_list.to_pandas().empty

    def test_insert_dataframe(self, cognite_client, mock_post_datapoints):
        import pandas as pd

        timestamps = [1500000000000, 1510000000000, 1520000000000, 1530000000000]
        df = pd.DataFrame(
            {"123": [1, 2, 3, 4], "456": [5.0, 6.0, 7.0, 8.0]},
            index=pd.to_datetime(timestamps, unit="ms"),
        )
        res = cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False)
        assert res is None
        request_body = jsgz_load(mock_post_datapoints.calls.last.request.content)
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
        request_body = jsgz_load(mock_post_datapoints.calls.last.request.content)
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
        request_body = jsgz_load(mock_post_datapoints.calls.last.request.content)
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


# Increase readability in test data:
d, t, v = "datapoints", "timestamp", "value"


class TestDatapointsPoster:
    @pytest.mark.parametrize(
        "limits, insert_dps, exp_calls",
        (
            (
                # Below ts limit, last chunk size == same as others
                (4, 5, 4),
                [
                    {"id": 1, d: [(10, 1), (20, 2)]},
                    {"external_id": "a", d: [(30, 3)]},
                    {"id": 2, d: [(40, 4), (50, 5), (60, 6)]},
                    {"external_id": "b", d: list(zip(range(70, 121, 10), range(7, 13)))},
                ],
                [
                    [
                        {"id": 1, "datapoints": [(10, 1), (20, 2)]},
                        {"externalId": "a", "datapoints": [(30, 3)]},
                        {"id": 2, "datapoints": [(40, 4)]},
                    ],
                    [
                        {"id": 2, "datapoints": [(50, 5), (60, 6)]},
                        {"externalId": "b", "datapoints": [(70, 7), (80, 8)]},
                    ],
                    [{"externalId": "b", "datapoints": [(90, 9), (100, 10), (110, 11), (120, 12)]}],
                ],
            ),
            (
                # Below ts limit, last chunk size < same as others
                (2, 5, 1),
                [
                    {"id": 1, d: [(10, 1)]},
                    {"external_id": "a", d: [(20, 2)]},
                    {"id": 2, d: [(30, 3), (40, 4), (50, 5)]},
                ],
                [
                    [{"id": 1, "datapoints": [(10, 1)]}, {"externalId": "a", "datapoints": [(20, 2)]}],
                    [{"id": 2, "datapoints": [(30, 3), (40, 4)]}],
                    [{"id": 2, "datapoints": [(50, 5)]}],
                ],
            ),
            (
                # Above ts limit and dps limit, last chunk size == same as others
                (5, 3, 5),
                [{"id": i, d: [(j, j) for j in range(10)]} for i in range(1, 5)],
                [
                    *([{"id": i, d: [(j, j) for j in range(5)]}] for i in range(1, 5)),
                    *([{"id": i, d: [(j, j) for j in range(5, 10)]}] for i in range(1, 5)),
                ],
            ),
            (
                # Above ts limit and dps limit, last chunk size < same as others.
                # Identifiers duplicated (test merging works)
                (4, 2, 1),
                [
                    {"id": 1, d: [(10, 1)]},
                    {"external_id": "a", d: [(20, 2)]},
                    {"id": 1, d: [(30, 3)]},
                    {"external_id": "a", d: [(40, 4)]},
                    {"external_id": "x", d: [(-10, -1)]},
                    {"id": 1, d: [(50, 5)]},
                    {"external_id": "a", d: [(50, 5), (60, 6), (70, 7)]},
                ],
                [
                    [
                        {"id": 1, "datapoints": [(10, 1), (30, 3), (50, 5)]},
                        {"externalId": "a", "datapoints": [(20, 2)]},
                    ],
                    [{"externalId": "a", "datapoints": [(40, 4), (50, 5), (60, 6), (70, 7)]}],
                    [{"externalId": "x", "datapoints": [(-10, -1)]}],
                ],
            ),
            (
                # Way above ts limit x max_workers, exactly at dps limit
                (10, 10, 7),
                [{"id": i, d: [(i * 2, i)]} for i in range(1, 98)],
                [[{"id": i, d: [(i * 2, i)]} for i in range(i, min(98, i + 10))] for i in range(1, 98, 10)],
            ),
        ),
    )
    def test_full_insert_flow(self, cognite_client, monkeypatch, limits, insert_dps, exp_calls):
        # To keep parametrized tests readable, we convert to the expected nametuple _InsertDatapoint here:
        for call_list in exp_calls:
            for call in call_list:
                call["datapoints"] = [_InsertDatapoint(*tpl) for tpl in call["datapoints"]]

        # A bit of mocking since we clear payloads inplace after insertion and thus need to copy
        # the content before it is garbage collected:
        calls = []
        dps_client = cognite_client.time_series.data
        monkeypatch.setattr(
            dps_api.DatapointsPoster, "_insert_datapoints", lambda self, payload: calls.append(deepcopy(payload))
        )
        dps_limit, ts_limit, last_chunk_size = limits
        monkeypatch.setattr(dps_client, "_DPS_INSERT_LIMIT", dps_limit)
        monkeypatch.setattr(dps_client, "_POST_DPS_OBJECTS_LIMIT", ts_limit)
        monkeypatch.setattr(dps_client._config, "max_workers", 4)

        # Actually run the test:
        dps_client.insert_multiple(insert_dps)

        # We don't know ordering of calls (executed by N threads):
        to_check_n_dps = sorted(
            (sum(len(insert_obj["datapoints"]) for insert_obj in call) for call in calls),
            reverse=True,
        )
        assert len(calls) == len(exp_calls)
        assert all(dps_limit == n_dps for n_dps in to_check_n_dps[:-1])
        assert last_chunk_size == to_check_n_dps[-1]
        unittest.TestCase().assertCountEqual(calls, exp_calls)

    def test_split_logic_adheres_to_limits(self, cognite_client, monkeypatch):
        calls = []
        dps_client = cognite_client.time_series.data
        monkeypatch.setattr(
            dps_api.DatapointsPoster, "_insert_datapoints", lambda self, payload: calls.append(deepcopy(payload))
        )
        dps_limit, ts_limit = randint(200, 2000), randint(2, 20)
        dps_client = cognite_client.time_series.data
        monkeypatch.setattr(dps_client, "_DPS_INSERT_LIMIT", dps_limit)
        monkeypatch.setattr(dps_client, "_POST_DPS_OBJECTS_LIMIT", ts_limit)
        monkeypatch.setattr(dps_client._config, "max_workers", 4)

        insert_dps = [
            {
                "id": identifier,
                "datapoints": [{"timestamp": i, "value": i} for i in range(random_gamma_dist_integer(4000))],
            }
            for identifier in range(1, randint(2, 5))
        ]
        insert_dps.extend(
            {"id": identifier, "datapoints": [(i * 1000, i) for i in range(random_gamma_dist_integer(200))]}
            for identifier in range(1, randint(10, 50))
        )
        insert_dps.extend(
            {"id": identifier, "datapoints": [(i * 1000, i) for i in range(random_gamma_dist_integer(20))]}
            for identifier in range(1, randint(50, 100))
        )
        shuffle(insert_dps)
        expected_n_dps = sum(len(dct["datapoints"]) for dct in insert_dps)
        dps_client.insert_multiple(insert_dps)

        tot_n_dps = 0
        for call in calls:
            tot_n_dps += (n_dps := sum(len(d["datapoints"]) for d in call))
            assert 0 < n_dps <= dps_limit
            assert 0 < len(call) <= ts_limit
        assert expected_n_dps == tot_n_dps


class TestRetrieveDataPointsInTz:
    """
    This test only the error raising functionality, actual retrieval functionality is tested in the integration tests.
    """

    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "args, start_tz, end_tz, expected_error_message",
        [
            pytest.param(
                {"external_id": "123", "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                None,
                None,
                "All times must be timezone aware, start and end do not have timezones.",
                id="Naive timezones",
            ),
            pytest.param(
                {"external_id": "123", "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                "Europe/Oslo",
                "America/Los_Angeles",
                "'start' and 'end' represent different timezones: 'Europe/Oslo' and 'America/Los_Angeles'.",
                id="Mismatch timezone",
            ),
            pytest.param(
                {"external_id": "123", "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                "Europe/Oslo",
                None,
                "All times must be timezone aware, end does not have a timezone.",
                id="Missing end timezone",
            ),
            pytest.param(
                {"external_id": "123", "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                None,
                "America/Los_Angeles",
                "All times must be timezone aware, start does not have a timezone.",
                id="Missing start timezone",
            ),
            pytest.param(
                {"start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                "Europe/Oslo",
                "Europe/Oslo",
                re.escape("Either input id(s) or external_id(s)"),
                id="Neither id or external id",
            ),
            pytest.param(
                {"external_id": "123", "id": 123, "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2)},
                "Europe/Oslo",
                "Europe/Oslo",
                re.escape("Either input id(s) or external_id(s)"),
                id="Passed both id and external id",
            ),
            pytest.param(
                {
                    "external_id": "123",
                    "start": datetime(2023, 1, 1),
                    "end": datetime(2023, 1, 2),
                    "aggregates": "average",
                    "granularity": "12years",
                },
                "Europe/Oslo",
                "Europe/Oslo",
                r"^Granularity, '12years', is above the maximum limit of 100k hours equivalent \(was 105192\)\.$",
                id="Granularity above maximum aggregation limit in hours",
            ),
            pytest.param(
                {
                    "external_id": "123",
                    "start": datetime(2023, 1, 1),
                    "end": datetime(2023, 1, 2),
                    "aggregates": "average",
                    "granularity": "48quarters",
                },
                "Europe/Oslo",
                "Europe/Oslo",
                r"^Granularity, '48quarters', is above the maximum limit of 100k hours equivalent \(was 105192\)\.$",
                id="Granularity above maximum aggregation limit in quarters",
            ),
            pytest.param(
                {
                    "external_id": ["123", "123"],
                    "start": datetime(2023, 1, 1),
                    "end": datetime(2023, 1, 2),
                    "aggregates": "average",
                    "granularity": "1year",
                },
                "Europe/Oslo",
                "Europe/Oslo",
                re.escape("The following identifiers were not unique: {'123'}"),
                id="Duplicated external ids",
            ),
            pytest.param(
                {
                    "id": [123, 123],
                    "start": datetime(2023, 1, 1),
                    "end": datetime(2023, 1, 2),
                    "aggregates": "average",
                    "granularity": "1year",
                },
                "Europe/Oslo",
                "Europe/Oslo",
                re.escape("The following identifiers were not unique: {123}"),
                id="Duplicated ids",
            ),
            pytest.param(
                {"id": 123, "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2), "aggregates": "average"},
                "Europe/Oslo",
                "Europe/Oslo",
                "Got only one of 'aggregates' and 'granularity'. Pass both to get aggregates, or neither to get raw data",
                id="Missing granularity",
            ),
            pytest.param(
                {"id": 123, "start": datetime(2023, 1, 1), "end": datetime(2023, 1, 2), "granularity": "1year"},
                "Europe/Oslo",
                "Europe/Oslo",
                "Got only one of 'aggregates' and 'granularity'. Pass both to get aggregates, or neither to get raw data",
                id="Missing aggregates",
            ),
        ],
    )
    def test_retrieve_data_points_in_tz_invalid_user_input(
        args: dict, expected_error_message: str, start_tz: str | None, end_tz: str | None, cognite_client: CogniteClient
    ):
        if start_tz is not None:
            args["start"] = args["start"].astimezone(ZoneInfo(start_tz))
        if end_tz is not None:
            args["end"] = args["end"].astimezone(ZoneInfo(end_tz))

        with pytest.raises(ValueError, match=expected_error_message):
            cognite_client.time_series.data.retrieve_dataframe_in_tz(**args)

[end of tests/tests_unit/test_api/test_datapoints.py]
