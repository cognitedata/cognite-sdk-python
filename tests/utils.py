from __future__ import annotations

import cProfile
import functools
import gzip
import json
import math
import os
import random
from contextlib import contextmanager

from cognite.client.data_classes.datapoints import ALL_SORTED_DP_AGGS
from cognite.client.utils._auxiliary import random_string


def all_subclasses(base: type) -> list[type]:
    """Returns a list (without duplicates) of all subclasses of a given class, sorted on import-path-name.
    Ignores classes not part of the main library, e.g. subclasses part of tests.
    """
    return sorted(
        filter(
            lambda sub: str(sub).startswith("<class 'cognite.client"),
            set(base.__subclasses__()).union(s for c in base.__subclasses__() for s in all_subclasses(c)),
        ),
        key=str,
    )


def all_mock_children(mock, parent_names=()):
    """Returns a dictionary with correct dotted names mapping to mocked classes."""
    dct = {".".join((*parent_names, k)): v for k, v in mock._mock_children.items()}
    for name, child in dct.copy().items():
        dct.update(all_mock_children(child, parent_names=(*parent_names, name)))
    return dct


@contextmanager
def rng_context(seed: int):
    """Temporarily override internal random state for deterministic behaviour without side-effects

    Idea stolen from pandas source `class RNGContext`.
    """
    state = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(state)


def random_cognite_ids(n):
    # Returns list of random, valid Cognite internal IDs:
    return random.choices(range(1, 9007199254740992), k=n)


def random_cognite_external_ids(n, str_len=50):
    # Returns list of random, valid Cognite external IDs:
    return [random_string(str_len) for _ in range(n)]


def random_granularity(granularities="smhd", lower_lim=1, upper_lim=100000):
    gran = random.choice(granularities)
    upper = {"s": 120, "m": 120, "h": 100000, "d": 100000}
    unit = random.choice(range(max(lower_lim, 1), min(upper_lim, upper[gran]) + 1))
    return f"{unit}{gran}"


def random_aggregates(n=None, exclude=None):
    """Return n random aggregates in a list - or random (at least 1) if n is None.
    Accepts a container object of aggregates to `exclude`
    """
    agg_lst = ALL_SORTED_DP_AGGS
    if exclude:
        agg_lst = [a for a in agg_lst if a not in exclude]
    n = n or random.randint(1, len(agg_lst))
    return random.sample(agg_lst, k=n)


def random_gamma_dist_integer(inclusive_max, max_tries=100):
    # "Smaller integers are more likely"
    for _ in range(max_tries):
        i = 1 + math.floor(random.gammavariate(1, inclusive_max * 0.3))
        if i <= inclusive_max:  # rejection sampling
            return i
    raise RuntimeError(f"Max tries exceeded while generating a random integer in range [1, {inclusive_max}]")


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
