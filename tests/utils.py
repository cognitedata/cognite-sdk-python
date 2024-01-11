from __future__ import annotations

import abc
import collections.abc
import enum
import gzip
import importlib
import inspect
import json
import math
import os
import random
import string
import typing
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Mapping, TypeVar, cast, get_args, get_origin, get_type_hints

from cognite.client import CogniteClient
from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.data_classes import (
    DataPointSubscriptionWrite,
    EndTimeFilter,
    Relationship,
    SequenceColumn,
    SequenceColumnList,
    SequenceData,
    SequenceRow,
    SequenceRows,
    Transformation,
    filters,
)
from cognite.client.data_classes._base import CogniteResourceList, Geometry
from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Query
from cognite.client.data_classes.datapoints import ALL_SORTED_DP_AGGS, Datapoints, DatapointsArray
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.transformations.notifications import TransformationNotificationWrite
from cognite.client.data_classes.transformations.schedules import TransformationScheduleWrite
from cognite.client.data_classes.workflows import (
    FunctionTaskOutput,
    FunctionTaskParameters,
    WorkflowTaskOutput,
    WorkflowTaskParameters,
)
from cognite.client.testing import CogniteClientMock
from cognite.client.utils._importing import local_import
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
            lambda sub: sub.__module__.startswith("cognite.client"),
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
def rng_context(seed: int | str):
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
    raw_freq: str | None = None,
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


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def dict_without(input_dict: Mapping[K, V], without_keys: set[str]) -> dict[K, V]:
    """Copy `input_dict`, but exclude the keys in `without_keys`.

    >>> a = {"foo": "bar", "bar": "baz", "zip": "zap"}
    >>> b = dict_without(a, {"foo", "bar"})
    >>> b
    {'zip': 'zap'}
    >>> b["foo"] = "not bar"
    >>> a
    {'foo': 'bar', 'bar': 'baz', 'zip': 'zap'}
    """
    return {k: v for k, v in input_dict.items() if k not in without_keys}


T_Object = TypeVar("T_Object", bound=object)


class FakeCogniteResourceGenerator:
    _error_msg: typing.ClassVar[str] = "Please extend this function to support generating fake data for this type"

    def __init__(self, seed: int | None = None, cognite_client: CogniteClientMock | CogniteClient | None = None):
        self._random = random.Random(seed)
        self._cognite_client = cognite_client or CogniteClientMock()

    def create_instance(self, resource_cls: type[T_Object], skip_defaulted_args: bool = False) -> T_Object:
        signature = inspect.signature(resource_cls.__init__)
        try:
            type_hint_by_name = get_type_hints(resource_cls.__init__, localns=self._type_checking)
        except TypeError:
            # Python 3.10 Type hints cannot be evaluated with get_type_hints,
            # ref https://stackoverflow.com/questions/66006087/how-to-use-typing-get-type-hints-with-pep585-in-python3-8
            resource_module_vars = vars(importlib.import_module(resource_cls.__module__))
            resource_module_vars.update(self._type_checking())
            type_hint_by_name = self._get_type_hints_3_10(resource_module_vars, signature, vars(resource_cls))

        keyword_arguments: dict[str, Any] = {}
        positional_arguments: list[Any] = []
        for name, parameter in signature.parameters.items():
            if name == "self":
                continue
            elif name == "args" or name == "kwargs":
                # Skipping generic arguments.
                continue
            elif parameter.annotation is inspect.Parameter.empty:
                raise ValueError(f"Parameter {name} of {resource_cls.__name__} is missing annotation")
            elif skip_defaulted_args and parameter.default is not inspect.Parameter.empty:
                continue

            if resource_cls is Geometry and name == "geometries":
                # Special case for Geometry to avoid recursion.
                value = None
            else:
                value = self.create_value(type_hint_by_name[name], var_name=name)

            if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL}:
                positional_arguments.append(value)
            else:
                keyword_arguments[name] = value

        # Special cases
        if resource_cls is DataPointSubscriptionWrite:
            # DataPointSubscriptionWrite requires either timeseries_ids or filter
            if skip_defaulted_args:
                keyword_arguments["time_series_ids"] = ["my_timeseries1", "my_timeseries2"]
            else:
                keyword_arguments.pop("filter", None)

        if resource_cls is Query:
            # The fake generator makes all dicts from 1-3 values, we need to make sure that the query is valid
            # by making sure that the list of equal length, so we make both to length 1.
            with_key, with_value = next(iter(keyword_arguments["with_"].items()))
            select_value = next(iter(keyword_arguments["select"].values()))
            keyword_arguments["with_"] = {with_key: with_value}
            keyword_arguments["select"] = {with_key: select_value}
        elif resource_cls is Relationship and not skip_defaulted_args:
            # Relationship must set the source and target type consistently with the source and target
            keyword_arguments["source_type"] = type(keyword_arguments["source"]).__name__
            keyword_arguments["target_type"] = type(keyword_arguments["target"]).__name__
        elif resource_cls is Datapoints and not skip_defaulted_args:
            # All lists have to be equal in length and only value and timestamp
            keyword_arguments["timestamp"] = keyword_arguments["timestamp"][:1]
            keyword_arguments["value"] = keyword_arguments["value"][:1]
            for key in list(keyword_arguments):
                if isinstance(keyword_arguments[key], list) and key not in {"timestamp", "value"}:
                    keyword_arguments.pop(key)
        elif resource_cls is DatapointsArray:
            keyword_arguments["is_string"] = False
        elif resource_cls is SequenceRows:
            # All row values must match the number of columns
            # Reducing to one column, and one value for each row
            if skip_defaulted_args:
                keyword_arguments["external_id"] = "my_sequence_rows"
            keyword_arguments["columns"] = keyword_arguments["columns"][:1]
            for row in keyword_arguments["rows"]:
                row.values = row.values[:1]
        elif resource_cls is SequenceData:
            if skip_defaulted_args:
                # At least external_id or id must be set
                keyword_arguments["external_id"] = "my_sequence"
                keyword_arguments["rows"] = [
                    SequenceRow(
                        row_number=1,
                        values=[
                            1,
                        ],
                    )
                ]
                keyword_arguments["columns"] = SequenceColumnList(
                    [
                        SequenceColumn("my_column"),
                    ]
                )
            else:
                # All row values must match the number of columns
                keyword_arguments.pop("rows", None)
                keyword_arguments["columns"] = keyword_arguments["columns"][:1]
                keyword_arguments["row_numbers"] = keyword_arguments["row_numbers"][:1]
                keyword_arguments["values"] = keyword_arguments["values"][:1]
                keyword_arguments["values"][0] = keyword_arguments["values"][0][:1]
        elif resource_cls is EndTimeFilter:
            # EndTimeFilter requires either is null or (max and/or min)
            keyword_arguments.pop("is_null", None)
        elif resource_cls is Transformation and not skip_defaulted_args:
            # schedule and jobs must match external id and id
            keyword_arguments["schedule"].external_id = keyword_arguments["external_id"]
            keyword_arguments["schedule"].id = keyword_arguments["id"]
            keyword_arguments["running_job"].transformation_id = keyword_arguments["id"]
            keyword_arguments["last_finished_job"].transformation_id = keyword_arguments["id"]
        elif resource_cls is TransformationScheduleWrite:
            # TransformationScheduleWrite requires either id or external_id
            keyword_arguments.pop("id", None)
        elif resource_cls is TransformationNotificationWrite:
            # TransformationNotificationWrite requires either transformation_id or transformation_external_id
            if skip_defaulted_args:
                keyword_arguments["transformation_external_id"] = "my_transformation"
            else:
                keyword_arguments.pop("transformation_id", None)
        elif resource_cls is NodeResultSetExpression and not skip_defaulted_args:
            # Through has a special format.
            keyword_arguments["through"] = [keyword_arguments["through"][0], "my_view/v1", "a_property"]

        return resource_cls(*positional_arguments, **keyword_arguments)

    def create_value(self, type_: Any, var_name: str | None = None) -> Any:
        if isinstance(type_, typing.ForwardRef):
            type_ = type_._evaluate(globals(), self._type_checking())

        if var_name == "external_id" and type_ is str:
            return self._random_string(50, sample_from=string.ascii_uppercase + string.digits)
        elif var_name == "id" and type_ is int:
            return self._random.choice(range(1, MAX_VALID_INTERNAL_ID + 1))
        if type_ is str or type_ is Any:
            return self._random_string()
        elif type_ is int:
            return self._random.randint(1, 100000)
        elif type_ is float:
            return self._random.random()
        elif type_ is bool:
            return self._random.choice([True, False])
        elif type_ is dict:
            return {self._random_string(10): self._random_string(10) for _ in range(self._random.randint(1, 3))}
        elif type_ is CogniteClient:
            return self._cognite_client
        elif inspect.isclass(type_) and any(base is abc.ABC for base in type_.__bases__):
            implementations = all_concrete_subclasses(type_)
            if type_ is Filter:
                # Remove filters which are only used by data modeling classes
                implementations.remove(filters.HasData)
                implementations.remove(filters.Nested)
                implementations.remove(filters.GeoJSONWithin)
                implementations.remove(filters.GeoJSONDisjoint)
                implementations.remove(filters.GeoJSONIntersects)
            if type_ is WorkflowTaskOutput:
                # For Workflow Output has to match the input type
                selected = FunctionTaskOutput
            elif type_ is WorkflowTaskParameters:
                selected = FunctionTaskParameters
            else:
                selected = self._random.choice(implementations)
            return self.create_instance(selected)
        elif isinstance(type_, enum.EnumMeta):
            return self._random.choice(list(type_))
        elif isinstance(type_, TypeVar):
            return self.create_value(type_.__bound__)
        elif inspect.isclass(type_) and issubclass(type_, CogniteResourceList):
            return type_([self.create_value(type_._RESOURCE) for _ in range(self._random.randint(1, 3))])
        elif inspect.isclass(type_):
            return self.create_instance(type_)

        container_type = get_origin(type_)
        is_container = container_type is not None
        if not is_container:
            # Handle numpy types
            import numpy as np
            from numpy.typing import NDArray

            if type_ == NDArray[np.float64]:
                return np.array([self._random.random() for _ in range(3)], dtype=np.float64)
            elif type_ == NDArray[np.int64]:
                return np.array([self._random.randint(1, 100) for _ in range(3)], dtype=np.int64)
            elif type_ == NDArray[np.datetime64]:
                return np.array([self._random.randint(1, 1704067200000) for _ in range(3)], dtype="datetime64[ms]")
            else:
                raise ValueError(f"Unknown type {type_} {type(type_)}. {self._error_msg}")

        # Handle containers
        args = get_args(type_)
        first_not_none = next((arg for arg in args if arg is not None), None)
        if container_type is typing.Union:
            return self.create_value(first_not_none)
        elif container_type is typing.Literal:
            return self._random.choice(args)
        elif container_type in [
            typing.List,
            list,
            typing.Sequence,
            collections.abc.Sequence,
            collections.abc.Collection,
        ]:
            return [self.create_value(first_not_none) for _ in range(3)]
        elif container_type in [typing.Dict, dict, collections.abc.MutableMapping, collections.abc.Mapping]:
            if first_not_none is None:
                return self.create_value(dict)
            key_type, value_type = args
            return {
                self.create_value(key_type): self.create_value(value_type) for _ in range(self._random.randint(1, 3))
            }
        elif container_type in [typing.Tuple, tuple]:
            if any(arg is ... for arg in args):
                return tuple(self.create_value(first_not_none) for _ in range(self._random.randint(1, 3)))
            raise NotImplementedError(f"Tuple with multiple types is not supported. {self._error_msg}")

        raise NotImplementedError(f"Unsupported container type {container_type}. {self._error_msg}")

    def _random_string(
        self,
        size: int | None = None,
        sample_from: str = string.ascii_uppercase + string.digits + string.ascii_lowercase + string.punctuation,
    ) -> str:
        k = size or self._random.randint(1, 100)
        return "".join(self._random.choices(sample_from, k=k))

    @classmethod
    def _type_checking(cls) -> dict[str, Any]:
        """
        When calling the get_type_hints function, it imports the module with the function TYPE_CHECKING is set to False.

        This function takes all the special types used in data classes and returns them as a dictionary so it
        can be used in the local namespaces.
        """
        import numpy as np
        import numpy.typing as npt

        from cognite.client import CogniteClient

        NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
        NumpyInt64Array = npt.NDArray[np.int64]
        NumpyFloat64Array = npt.NDArray[np.float64]
        NumpyObjArray = npt.NDArray[np.object_]
        return {
            "CogniteClient": CogniteClient,
            "NumpyDatetime64NSArray": NumpyDatetime64NSArray,
            "NumpyInt64Array": NumpyInt64Array,
            "NumpyFloat64Array": NumpyFloat64Array,
            "NumpyObjArray": NumpyObjArray,
        }

    @classmethod
    def _get_type_hints_3_10(
        cls, resource_module_vars: dict[str, Any], signature310: inspect.Signature, local_vars: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            name: cls._create_type_hint_3_10(parameter.annotation, resource_module_vars, local_vars)
            for name, parameter in signature310.parameters.items()
            if name != "self"
        }

    @classmethod
    def _create_type_hint_3_10(
        cls, annotation: str, resource_module_vars: dict[str, Any], local_vars: dict[str, Any]
    ) -> Any:
        if annotation.endswith(" | None"):
            annotation = annotation[:-7]
        try:
            return eval(annotation, resource_module_vars, local_vars)
        except TypeError:
            # Python 3.10 Type Hint
            return cls._type_hint_3_10_to_8(annotation, resource_module_vars, local_vars)

    @classmethod
    def _type_hint_3_10_to_8(
        cls, annotation: str, resource_module_vars: dict[str, Any], local_vars: dict[str, Any]
    ) -> Any:
        if cls._is_vertical_union(annotation):
            alternatives = [
                cls._create_type_hint_3_10(a.strip(), resource_module_vars, local_vars) for a in annotation.split("|")
            ]
            return typing.Union[tuple(alternatives)]
        elif annotation.startswith("dict[") and annotation.endswith("]"):
            if Counter(annotation)[","] > 1:
                key, rest = annotation[5:-1].split(",", 1)
                return typing.Dict[
                    key.strip(), cls._create_type_hint_3_10(rest.strip(), resource_module_vars, local_vars)
                ]
            key, value = annotation[5:-1].split(",")
            return typing.Dict[
                cls._create_type_hint_3_10(key.strip(), resource_module_vars, local_vars),
                cls._create_type_hint_3_10(value.strip(), resource_module_vars, local_vars),
            ]
        elif annotation.startswith("Mapping[") and annotation.endswith("]"):
            if Counter(annotation)[","] > 1:
                key, rest = annotation[8:-1].split(",", 1)
                return typing.Mapping[
                    key.strip(), cls._create_type_hint_3_10(rest.strip(), resource_module_vars, local_vars)
                ]
            key, value = annotation[8:-1].split(",")
            return typing.Mapping[
                cls._create_type_hint_3_10(key.strip(), resource_module_vars, local_vars),
                cls._create_type_hint_3_10(value.strip(), resource_module_vars, local_vars),
            ]
        elif annotation.startswith("Optional[") and annotation.endswith("]"):
            return typing.Optional[cls._create_type_hint_3_10(annotation[9:-1], resource_module_vars, local_vars)]
        elif annotation.startswith("list[") and annotation.endswith("]"):
            return typing.List[cls._create_type_hint_3_10(annotation[5:-1], resource_module_vars, local_vars)]
        elif annotation.startswith("tuple[") and annotation.endswith("]"):
            return typing.Tuple[cls._create_type_hint_3_10(annotation[6:-1], resource_module_vars, local_vars)]
        elif annotation.startswith("typing.Sequence[") and annotation.endswith("]"):
            # This is used in the Sequence data class file to avoid name collision
            return typing.Sequence[cls._create_type_hint_3_10(annotation[16:-1], resource_module_vars, local_vars)]
        raise NotImplementedError(f"Unsupported conversion of type hint {annotation!r}. {cls._error_msg}")

    @classmethod
    def _is_vertical_union(cls, annotation: str) -> bool:
        if "|" not in annotation:
            return False
        parts = [p.strip() for p in annotation.split("|")]
        for part in parts:
            counts = Counter(part)
            if counts["["] != counts["]"]:
                return False
        return True
