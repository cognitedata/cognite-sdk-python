import cProfile
import functools
import gzip
import json
import os
from contextlib import contextmanager

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
def set_request_limit(client, limit):
    limits = [
        "_CREATE_LIMIT",
        "_LIST_LIMIT",
        "_RETRIEVE_LIMIT",
        "_UPDATE_LIMIT",
        "_DELETE_LIMIT",
        "_DPS_LIMIT",
        "_DPS_LIMIT_AGG",
        "_POST_DPS_OBJECTS_LIMIT",
        "_RETRIEVE_LATEST_LIMIT",
    ]

    tmp = {l: 0 for l in limits}
    for limit_name in limits:
        if hasattr(client, limit_name):
            tmp[limit_name] = getattr(client, limit_name)
            setattr(client, limit_name, limit)
    yield
    for limit_name, limit_val in tmp.items():
        if hasattr(client, limit_name):
            setattr(client, limit_name, limit_val)


@contextmanager
def unset_env_var(*name: str):
    if isinstance(name, str):
        name = [name]
    tmp = {}
    for n in name:
        tmp[n] = os.getenv(n)
        if tmp[n] is not None:
            del os.environ[n]
    yield
    for n in name:
        if tmp[n] is not None:
            os.environ[n] = tmp[n]


@contextmanager
def set_env_var(name: str, value: str):
    tmp = os.getenv(name)
    os.environ[name] = value
    yield
    if tmp is not None:
        os.environ[name] = tmp
    else:
        del os.environ[name]
