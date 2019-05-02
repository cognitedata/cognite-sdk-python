import cProfile
import functools
import gzip
import json
from contextlib import contextmanager
from unittest import mock
from unittest.mock import PropertyMock

BASE_URL = "https://greenfield.cognitedata.com"


def jsgz_load(s):
    return json.loads(gzip.decompress(s).decode())


@contextmanager
def profilectx():
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    pr.print_stats(sort="cumtime")


def profile(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        with profilectx():
            method(*args, **kwargs)

    return wrapper


@contextmanager
def set_request_limit(limit):
    with mock.patch("cognite.client._api_client.APIClient._LIMIT", new_callable=PropertyMock) as limit_mock:
        limit_mock.return_value = limit
        yield
