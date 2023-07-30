from __future__ import annotations

import abc
import cProfile
import enum
import functools
import gzip
import importlib
import inspect
import json
import math
import os
import random
import re
import string
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Type, TypeVar, cast, get_args, get_origin

from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.data_classes import DataPointSubscriptionCreate, Relationship, filters
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, T_CogniteResource
from cognite.client.data_classes.datapoints import ALL_SORTED_DP_AGGS, Datapoints, DatapointsArray
from cognite.client.testing import CogniteClientMock
from cognite.client.utils._auxiliary import local_import
from cognite.client.utils._text import random_string

if TYPE_CHECKING:
    import pandas

T_Type = TypeVar("T_Type", bound=type)


def all_subclasses(base: T_Type) -> list[T_Type]:
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


def all_concrete_subclasses(base: T_Type) -> list[T_Type]:
    return [
        sub
        for sub in all_subclasses(base)
        if all(base is not abc.ABC for base in sub.__bases__) and not inspect.isabstract(sub)
    ]


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


T_Object = TypeVar("T_Object", bound=object)


class FakeCogniteResourceGenerator:
    def __init__(self, seed: int | None = None, cognite_client: CogniteClientMock | None = None):
        self._random = random.Random(seed)
        self._cognite_client = cognite_client or CogniteClientMock()

    def create(self, resource_cls: Type[T_CogniteResource]) -> T_CogniteResource:
        return self._create(resource_cls)

    def _create(self, object_cls: Type[T_Object]) -> T_Object:
        signature = inspect.signature(object_cls.__init__)
        keyword_arguments: dict[str, Any] = {}
        positional_arguments: list[Any] = []
        for name, parameter in signature.parameters.items():
            if name == "self":
                continue
            elif name == "args" or name == "kwargs":
                # Skipping generic arguments.
                continue
            elif parameter.annotation is inspect.Parameter.empty:
                raise ValueError(f"Parameter {name} of {object_cls.__name__} is missing annotation")

            annotation = str(parameter.annotation)
            if name == "_" and annotation == "Any":
                # This is used to goble up extra arguments in the data modeling read classes.
                continue

            value = self._create_complex_value(name, annotation, object_cls)
            if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL}:
                positional_arguments.append(value)
            else:
                keyword_arguments[name] = value

        # Special cases
        if object_cls is DataPointSubscriptionCreate:
            # DataPointSubscriptionCreate requires either timeseries_ids or filter
            keyword_arguments.pop("filter", None)
        elif object_cls is Relationship:
            # Relationship must set the source and target type consistently with the source and target
            keyword_arguments["source_type"] = type(keyword_arguments["source"]).__name__
            keyword_arguments["target_type"] = type(keyword_arguments["target"]).__name__
        elif object_cls is Datapoints:
            # All lists have to be equal in length and only value and timestamp
            keyword_arguments["timestamp"] = keyword_arguments["timestamp"][:1]
            keyword_arguments["value"] = keyword_arguments["value"][:1]
            for key in list(keyword_arguments):
                if isinstance(keyword_arguments[key], list) and key not in {"timestamp", "value"}:
                    keyword_arguments.pop(key)
        elif object_cls is DatapointsArray:
            keyword_arguments["is_string"] = False
        return object_cls(*positional_arguments, **keyword_arguments)

    def _create_complex_value(self, name: str, annotation: str, resource_cls: Type[CogniteResource]) -> Any:
        while True:
            is_dict = False
            is_list = False
            if annotation.startswith("Optional[") and annotation.endswith("]"):
                annotation = annotation[9:-1]
            elif (annotation.startswith("Union[") and annotation.endswith("]")) or self._is_vertical_union(annotation):
                if "|" in annotation:
                    alternatives = [a.strip() for a in annotation.split("|")]
                else:
                    annotation = annotation[6:-1]
                    alternatives = [a.strip() for a in re.split(r",\s*(?![^[]*])", annotation)]
                if "LabelDefinition" in alternatives:
                    annotation = "LabelDefinition"
                else:
                    annotation = next((a for a in alternatives if "Any" not in a and "None" not in a), None)
                    annotation = annotation or alternatives[0]
            elif annotation.lower().startswith("list[") and annotation.endswith("]"):
                annotation = annotation[5:-1]
                is_list = True
            elif annotation.startswith("Sequence[") and annotation.endswith("]"):
                annotation = annotation[9:-1]
                is_list = True
            elif annotation.startswith("SequenceType[") and annotation.endswith("]"):
                annotation = annotation[13:-1]
                is_list = True
            elif annotation.startswith("Mapping[") and annotation.endswith("]"):
                annotation = annotation[8:-1]
                is_dict = True
            elif annotation.startswith("MutableMapping[") and annotation.endswith("]"):
                annotation = annotation[15:-1]
                is_dict = True
            elif annotation.lower().startswith("dict[") and annotation.endswith("]"):
                annotation = annotation[5:-1]
                is_dict = True
            elif annotation.lower().startswith("tuple[") and annotation.endswith("]"):
                annotation = annotation[6:-1]
                annotation = annotation.split(",")[0]
            else:
                return self._create_simple_value(name, annotation, resource_cls)

            if is_dict:
                key, value = (x.strip() for x in annotation.split(",", maxsplit=1))
                return {
                    self._create_complex_value(name, key, resource_cls): self._create_complex_value(
                        name, value, resource_cls
                    )
                    for _ in range(self._random.randint(1, 3))
                }
            elif is_list:
                return [
                    self._create_complex_value(name, annotation, resource_cls)
                    for _ in range(self._random.randint(1, 3))
                ]

    def _create_simple_value(
        self, name: str, annotation: str, resource_cls: Type[CogniteResource]
    ) -> str | int | float | bool | dict | CogniteResource | None | Literal:
        # Primitive types
        if name == "external_id" and annotation == "str":
            return self._random_string(50, sample_from=string.ascii_uppercase + string.digits)
        elif name == "id" and annotation == "int":
            return self._random.choice(range(1, MAX_VALID_INTERNAL_ID + 1))
        elif annotation == "str" or annotation == "Any":
            return self._random_string()
        elif annotation == "int":
            return self._random.randint(1, 100000)
        elif annotation == "float":
            return self._random.random()
        elif annotation == "bool":
            return self._random.choice([True, False])
        elif annotation.lower() == "dict":
            return {self._random_string(10): self._random_string(10) for _ in range(self._random.randint(1, 3))}
        elif annotation == "CogniteClient":
            return self._cognite_client
        elif annotation.startswith("Literal[") and annotation.endswith("]"):
            annotation = annotation[8:-1]
            if annotation.startswith("(") and annotation.endswith(")"):
                annotation = annotation[1:-1]
            alternatives = [a.strip()[1:-1] for a in annotation.split(",")]
            return self._random.choice(alternatives)
        elif annotation == "NumpyFloat64Array":
            import numpy as np

            return np.array([self._random.random() for _ in range(3)], dtype=np.float64)
        elif annotation == "NumpyInt64Array":
            import numpy as np

            return np.array([self._random.randint(1, 100) for _ in range(3)], dtype=np.int64)
        elif annotation == "NumpyDatetime64NSArray":
            import numpy as np

            return np.array([self._random.randint(1, 1704067200000) for _ in range(3)], dtype="datetime64[ms]")

        resource_module_vars = vars(importlib.import_module(resource_cls.__module__))
        annotation_cls = resource_module_vars.get(annotation)
        is_class = inspect.isclass(annotation_cls)
        if get_origin(annotation_cls) is not None:
            annotation = self._get_typing_args(annotation_cls)
            return self._create_complex_value(name, annotation, resource_cls)
        elif issubclass(resource_cls, CogniteResourceList):
            return [self._create(resource_cls._RESOURCE) for _ in range(self._random.randint(1, 3))]
        elif isinstance(annotation_cls, enum.EnumMeta):
            return self._random.choice(list(annotation_cls))
        elif isinstance(annotation_cls, TypeVar):
            annotation = self._get_typing_args(annotation_cls.__bound__)
            return self._create_complex_value(name, annotation, resource_cls)
        elif is_class and issubclass(annotation_cls, (str, int, float, bool)):
            return self._create_simple_value(name, annotation_cls.__name__, resource_cls)
        elif is_class and issubclass(annotation_cls, CogniteResource):
            return self.create(annotation_cls)
        elif is_class and any(base is abc.ABC for base in annotation_cls.__bases__):
            implementations = all_concrete_subclasses(annotation_cls)
            if annotation == "Filter":
                # Remove filters which are only used by data modeling classes
                implementations.remove(filters.HasData)
                implementations.remove(filters.Nested)
            selected = self._random.choice(implementations)
            return self._create(selected)
        elif is_class:
            return self._create(annotation_cls)

        raise NotImplementedError(
            f"Unsupported annotation {annotation!r} for parameter {name!r} in {resource_cls.__name__!r}."
            " Please extend this function to support generating fake data for this type."
        )

    def _random_string(
        self,
        size: int | None = None,
        sample_from: str = string.ascii_uppercase + string.digits + string.ascii_lowercase + string.punctuation,
    ) -> str:
        k = size or self._random.randint(1, 100)
        return "".join(self._random.choices(sample_from, k=k))

    @classmethod
    def _is_vertical_union(cls, annotation: str) -> bool:
        if "|" not in annotation:
            return False
        parts = [p.strip() for p in annotation.split("|")]
        for part in parts:
            counts = Counter(part)
            if counts.get("[", 0) != counts.get("]", 0):
                return False
        return True

    @classmethod
    def _get_typing_args(cls, typing_cls: Any) -> str:
        args = get_args(typing_cls)
        arg = args[0]
        if str(arg).startswith("typing."):
            return str(arg).replace("typing.", "")
        else:
            return arg.__name__
