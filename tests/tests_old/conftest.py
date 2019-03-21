import datetime
import json
import logging
import os
import random
import string
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from requests.structures import CaseInsensitiveDict

from cognite.client import CogniteClient
from cognite.client.api.datapoints import Datapoint
from cognite.client.api.time_series import TimeSeries
from cognite.client.exceptions import APIError

log = logging.getLogger(__name__)

TEST_TS_START = 1514761200000
TEST_TS_END = 1544828400000
TEST_TS_MID = int((TEST_TS_START + TEST_TS_END) / 2)
TEST_TS_REASONABLE_INTERVAL = {"start": TEST_TS_MID - 2000000, "end": TEST_TS_MID + 2000000}
TEST_TS_REASONABLE_INTERVAL_DATETIME = {
    "start": datetime.fromtimestamp((TEST_TS_MID - 600000) / 1000),
    "end": datetime.fromtimestamp((TEST_TS_MID + 600000) / 1000),
}
TEST_TS_1_NAME = "SDK_TEST_TS_1_DO_NOT_DELETE"
TEST_TS_2_NAME = "SDK_TEST_TS_2_DO_NOT_DELETE"
TEST_TS_1_ID = None
TEST_TS_2_ID = None


@pytest.fixture(scope="session", autouse=True)
def time_series_in_cdp():
    global TEST_TS_1_ID, TEST_TS_2_ID
    client = CogniteClient()

    try:
        ts_list = [TimeSeries(name=TEST_TS_1_NAME)]
        client.time_series.post_time_series(ts_list)
        log.warning("Posted sdk test time series 1")
        client.datapoints.post_datapoints(
            name=TEST_TS_1_NAME,
            datapoints=[Datapoint(timestamp=i, value=i) for i in range(TEST_TS_START, TEST_TS_END, int(3.6e6))],
        )
        log.warning("Posted datapoints to sdk test time series 1")
        TEST_TS_1_ID = client.time_series.get_time_series(prefix=TEST_TS_1_NAME).to_json()[0]["id"]
    except APIError as e:
        log.warning("Posting test time series 1 failed with code {}".format(e.code))

    try:
        ts_list = [TimeSeries(name=TEST_TS_2_NAME)]
        client.time_series.post_time_series(ts_list)
        log.warning("Posted sdk test time series 2")
        client.datapoints.post_datapoints(
            name=TEST_TS_2_NAME,
            datapoints=[Datapoint(timestamp=i, value=i) for i in range(TEST_TS_START, TEST_TS_END, int(3.6e6))],
        )
        log.warning("Posted datapoints to sdk test time series 2")
        TEST_TS_2_ID = client.time_series.get_time_series(prefix=TEST_TS_2_NAME).to_json()[0]["id"]
    except APIError as e:
        log.warning("Posting test time series 2 failed with code {}".format(e.code))

    TEST_TS_1_ID = client.time_series.get_time_series(prefix=TEST_TS_1_NAME).to_json()[0]["id"]
    TEST_TS_2_ID = client.time_series.get_time_series(prefix=TEST_TS_2_NAME).to_json()[0]["id"]
    yield TEST_TS_1_ID, TEST_TS_2_ID


@pytest.fixture
def disable_gzip():
    os.environ["COGNITE_DISABLE_GZIP"] = "1"
    yield
    del os.environ["COGNITE_DISABLE_GZIP"]


class MockRequest(mock.Mock):
    def __init__(self):
        super().__init__()
        self.method = "GET"
        self.url = "http://some.url"
        self.headers = {}


class MockReturnValue(mock.Mock):
    """Helper class for building mock request responses.

    Should be assigned to MagicMock.return_value
    """

    def __init__(
        self, status=200, content="CONTENT", json_data=None, raise_for_status=None, headers=CaseInsensitiveDict()
    ):
        mock.Mock.__init__(self)
        if "X-Request-Id" not in headers:
            headers["X-Request-Id"] = "1234567890"

        # mock raise_for_status call w/optional error
        self.raise_for_status = mock.Mock()
        if raise_for_status:
            self.raise_for_status.side_effect = raise_for_status

        # set status code and content
        self.status_code = status

        # requests.models.Response.ok mock
        self.ok = status < 400
        self.content = content
        self.headers = headers

        # add json data if provided
        if json_data:
            self.json = lambda: json_data

        self.request = MockRequest()

        self.raw = MagicMock()

    def __setattr__(self, key, value):
        if key == "_content":
            self.json = lambda: json.loads(value.decode())
        super().__setattr__(key, value)


def get_time_w_offset(**kwargs):
    curr_time = datetime.datetime.now()
    offset_time = curr_time - datetime.timedelta(**kwargs)
    return int(round(offset_time.timestamp() * 1000))


def generate_random_string(n):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))
