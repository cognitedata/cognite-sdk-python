from copy import deepcopy

import pytest

from cognite.client.stable.events import EventListResponse, EventResponse
from cognite.client.stable.files import FileInfoResponse
from cognite.client.stable.timeseries import TimeSeriesResponse


@pytest.fixture(scope="module", params=["ts", "file", "event", "eventlist"])
def get_response_obj(request):
    TS_INTERNAL_REPR = {"data": {"items": [{"name": "0", "metadata": {"md1": "val1"}}]}}
    EVENT_LIST_INTERNAL_REPR = {"data": {"items": [{"id": 0, "metadata": {"md1": "val1"}}]}}
    EVENT_INTERNAL_REPR = {"data": {"items": [{"id": 0, "metadata": {"md1": "val1"}, "assetIds": []}]}}
    FILE_INFO_INTERNAL_REPR = {"data": {"items": [{"id": 0, "metadata": {"md1": "val1"}}]}}

    response = None
    if request.param == "ts":
        response = TimeSeriesResponse(TS_INTERNAL_REPR)
    elif request.param == "file":
        response = FileInfoResponse(FILE_INFO_INTERNAL_REPR)
    elif request.param == "eventlist":
        response = EventListResponse(EVENT_LIST_INTERNAL_REPR)
    elif request.param == "event":
        response = EventResponse(EVENT_INTERNAL_REPR)

    yield response


class TestDTOs:
    def test_internal_representation_not_mutated(self, get_response_obj):
        repr = deepcopy(get_response_obj.internal_representation)
        get_response_obj.to_pandas()
        get_response_obj.to_json()
        assert repr == get_response_obj.internal_representation
