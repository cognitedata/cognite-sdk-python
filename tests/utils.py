from __future__ import annotations

import cProfile
import functools
import gzip
import inspect
import json
import math
import os
import random
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Type, cast

from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.datapoints import ALL_SORTED_DP_AGGS
from cognite.client.utils._auxiliary import local_import
from cognite.client.utils._text import random_string

if TYPE_CHECKING:
    import pandas


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


def all_mock_children(mock, parent_name=()):
    """Returns a dictionary with correct dotted names mapping to mocked classes."""
    dct = {".".join((*parent_name, k)): v for k, v in mock._mock_children.items()}
    for name, child in dct.copy().items():
        dct.update(all_mock_children(child, parent_name=(name,)))
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
    return random.choices(range(1, MAX_VALID_INTERNAL_ID + 1), k=n)


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


def cdf_aggregate(
    raw_df: pandas.DataFrame,
    aggregate: Literal["average", "sum", "count"],
    granularity: str,
    is_step: bool = False,
    raw_freq: str = None,
) -> pandas.DataFrame:
    """Aggregates the dataframe as CDF is doing it on the database layer.

    **Motivation**: This is used in testing to verify that the correct aggregation is done with
    on the client side when aggregating in given time zone.

    Current assumptions:
        * No step timeseries
        * Uniform index.
        * Known frequency of raw data.

    Args:
        raw_df (pd.DataFrame): Dataframe with the raw datapoints.
        aggregate (str): Single aggregate to calculate, supported average, sum.
        granularity (str): The granularity to aggregates at. e.g. '15s', '2h', '10d'.
        is_step (bool): Whether to use stepwise or continuous interpolation.
        raw_freq (str): The frequency of the raw data. If it is not given, it is attempted inferred from raw_df.
    """
    if is_step:
        raise NotImplementedError()

    pd = cast(Any, local_import("pandas"))
    granularity_pd = granularity.replace("m", "T")
    grouping = raw_df.groupby(pd.Grouper(freq=granularity_pd))
    if aggregate == "sum":
        return grouping.sum()
    elif aggregate == "count":
        return grouping.count().astype("Int64")

    # The average is calculated by the formula '1/(b-a) int_a^b f(t) dt' where f(t) is the continuous function
    # This is weighted average of the sampled version of f(t)
    np = cast(Any, local_import("numpy"))

    def integrate_average(values: pandas.DataFrame) -> pandas.Series:
        inner = (values.iloc[1:].values + values.iloc[:-1].values) / 2.0
        res = inner.mean() if inner.shape[0] else np.nan
        return pd.Series(res, index=values.columns)

    freq = raw_freq or raw_df.index.inferred_freq
    if freq is None:
        raise ValueError("Failed to infer frequency raw data.")
    if not freq[0].isdigit():
        freq = f"1{freq}"

    # When the frequency of the data is 1 hour and above, the end point is excluded.
    freq = pd.Timedelta(freq)
    if freq >= pd.Timedelta("1hour"):
        return grouping.apply(integrate_average)

    def integrate_average_end_points(values: pandas.Series) -> float:
        dt = values.index[-1] - values.index[0]
        scale = np.diff(values.index) / 2.0 / dt
        return (scale * (values.values[1:] + values.values[:-1])).sum()

    step = pd.Timedelta(granularity_pd) // freq

    return (
        raw_df.rolling(window=pd.Timedelta(granularity_pd), closed="both")
        .apply(integrate_average_end_points)
        .shift(-step)
        .iloc[::step]
    )


def create_fake_cognite_resource(resource_cls: Type[CogniteResource]) -> CogniteResource:
    signature = inspect.signature(resource_cls.__init__)
    arguments: dict[str, Any] = {}
    for name, parameter in signature.parameters.items():
        if name == "self":
            continue
        if parameter.annotation is inspect.Parameter.empty:
            raise ValueError(f"Parameter {name} of {resource_cls} is missing annotation")
        if name == "external_id":
            arguments[name] = random_cognite_external_ids(1)[0]
        elif name == "id":
            arguments[name] = random_cognite_ids(1)[0]
        elif parameter.annotation in ["str", "Optional[str]"]:
            arguments[name] = random_string(10)
        elif parameter.annotation in ["int", "Optional[int]"]:
            arguments[name] = random.randint(1, 100000)
        elif parameter.annotation in ["float", "Optional[float]"]:
            arguments[name] = random.random()
        elif parameter.annotation in ["bool", "Optional[bool]"]:
            arguments[name] = random.choice([True, False])
        # else:
        #     raise ValueError(f"Unsupported annotation {parameter.annotation} for parameter {name}")

    return resource_cls(**arguments)
