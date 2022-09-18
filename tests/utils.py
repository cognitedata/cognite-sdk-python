import cProfile
import functools
import gzip
import json
import os
import random
from contextlib import contextmanager

from cognite.client._api.datapoint_constants import ALL_SORTED_DP_AGGS
from cognite.client.utils._auxiliary import random_string


def random_cognite_ids(n):
    # Returns list of random, valid Cognite internal IDs:
    return random.choices(range(1, 9007199254740992), k=n)


def random_cognite_external_ids(n):
    # Returns list of random, valid Cognite external IDs:
    return [random_string(50) for _ in range(n)]


def random_valid_granularity():
    gran = random.choice("smhd")
    upper = {"s": 120, "m": 120, "h": 100000, "d": 100000}
    unit = random.randint(1, upper[gran])
    return f"{unit}{gran}"


def random_valid_aggregates(n=None):
    """Return n random aggregates in a list or random (at least 1) if n is None"""
    n = n or random.randint(1, len(ALL_SORTED_DP_AGGS))
    return random.sample(ALL_SORTED_DP_AGGS, k=n)


@contextmanager
def set_max_workers(cognite_client, new):
    old = cognite_client._config.max_workers
    cognite_client._config.max_workers = new
    yield
    cognite_client._config.max_workers = old


@contextmanager
def tmp_set_envvar(envvar: str, value: str):
    old = os.getenv(envvar)
    os.environ[envvar] = value
    yield
    if old is None:
        del os.environ[envvar]
    else:
        os.environ[envvar] = old


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
def set_request_limit(client, limit):
    limits = [
        "_CREATE_LIMIT",
        "_LIST_LIMIT",
        "_RETRIEVE_LIMIT",
        "_UPDATE_LIMIT",
        "_DELETE_LIMIT",
        "_DPS_LIMIT",
        "_POST_DPS_OBJECTS_LIMIT",
        "_RETRIEVE_LATEST_LIMIT",
    ]

    tmp = {lim: 0 for lim in limits}
    for limit_name in limits:
        if hasattr(client, limit_name):
            tmp[limit_name] = getattr(client, limit_name)
            setattr(client, limit_name, limit)
    yield
    for limit_name, limit_val in tmp.items():
        if hasattr(client, limit_name):
            setattr(client, limit_name, limit_val)
