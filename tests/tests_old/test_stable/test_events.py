import pandas as pd
import pytest

import cognite.client.api.events
from cognite.client import APIError, CogniteClient

events = CogniteClient().events


@pytest.fixture(scope="module")
def get_post_event_obj():
    event = cognite.client.api.events.Event(start_time=1521500400000, end_time=1521586800000, description="hahaha")
    res = events.post_events([event])
    yield res
    ids = list(ev["id"] for ev in res.to_json())
    events.delete_events(ids)


def test_post_events(get_post_event_obj):
    assert isinstance(get_post_event_obj, cognite.client.api.events.EventListResponse)
    assert isinstance(get_post_event_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_post_event_obj.to_json(), list)


def test_attributes_not_none(get_post_event_obj):
    assert isinstance(get_post_event_obj, cognite.client.api.events.EventListResponse)
    assert isinstance(get_post_event_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_post_event_obj.to_json(), list)


def test_post_events_length(get_post_event_obj):
    assert len(get_post_event_obj.to_json()) == 1


def test_get_event(get_post_event_obj):
    id = get_post_event_obj.to_json()[0]["id"]
    res = events.get_event(event_id=id)
    assert isinstance(res, cognite.client.api.events.EventResponse)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert isinstance(res.to_json(), dict)
    assert res.to_pandas().shape[1] == 1


def test_get_event_invalid_id():
    with pytest.raises(APIError):
        events.get_event(123456789)


def test_get_events():
    res = events.get_events(min_start_time=1521500399999, max_start_time=1521500400001)
    assert isinstance(res, cognite.client.api.events.EventListResponse)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert isinstance(res.to_json(), list)
    assert isinstance(res[0], cognite.client.api.events.EventResponse)
    assert isinstance(res[:1], cognite.client.api.events.EventListResponse)
    assert len(res[:1]) == 1
    for event in res:
        assert isinstance(event, cognite.client.api.events.EventResponse)


def test_get_events_invalid_param_combination():
    with pytest.raises(APIError, match="disabled"):
        events.get_events(type="bla", asset_id=1)


def test_get_events_empty():
    res = events.get_events(asset_id=0)
    assert res.to_pandas().empty
    assert len(res.to_json()) == 0


@pytest.fixture()
def post_event():
    event = cognite.client.api.events.Event(start_time=1521500400000, end_time=1521586800000)
    res = events.post_events([event])
    return res


def test_delete_event(post_event):
    id = post_event.to_json()[0]["id"]
    res = events.delete_events([id])
    assert res is None


def test_search_for_events(get_post_event_obj):
    events.search_for_events(description="hahaha")
