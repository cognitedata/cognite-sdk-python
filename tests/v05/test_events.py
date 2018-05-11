import numpy as np
import pandas as pd
import pytest

from cognite import _utils
from cognite.v05 import events, dto


@pytest.fixture(scope='module')
def get_post_event_obj():
    event = dto.Event(start_time=1521500400000, end_time=1521586800000)
    res = events.post_events([event])
    yield res
    res = events.get_events()
    ids = list(ev['id'] for ev in res.to_json())
    events.delete_events(ids)


def test_post_events(get_post_event_obj):
    assert isinstance(get_post_event_obj, dto.EventListResponse)
    assert isinstance(get_post_event_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_post_event_obj.to_json(), list)
    assert isinstance(get_post_event_obj.to_ndarray(), np.ndarray)


def test_post_events_length(get_post_event_obj):
    assert len(get_post_event_obj.to_json()) == 1


def test_get_event(get_post_event_obj):
    id = get_post_event_obj.to_json()[0]['id']
    res = events.get_event(event_id=id)
    assert isinstance(res, dto.EventResponse)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert isinstance(res.to_json(), dict)
    assert isinstance(res.to_ndarray(), np.ndarray)


def test_get_event_invalid_id():
    with pytest.raises(_utils.APIError):
        events.get_event(123456789)


def test_get_events():
    res = events.get_events(min_start_time=1521500399999, max_start_time=1521500400001)
    assert isinstance(res, dto.EventListResponse)
    assert isinstance(res.to_pandas(), pd.DataFrame)
    assert isinstance(res.to_json(), list)
    assert isinstance(res.to_ndarray(), np.ndarray)
    for event in res:
        assert isinstance(event, dto.EventResponse)


def test_get_events_empty():
    res = events.get_events(max_start_time=1)
    assert res.to_pandas().empty
    assert len(res.to_json()) == 0


@pytest.fixture()
def post_event():
    event = dto.Event(start_time=1521500400000, end_time=1521586800000)
    res = events.post_events([event])
    return res


def test_delete_event(post_event):
    id = post_event.to_json()[0]['id']
    res = events.delete_events([id])
    assert res == {}
