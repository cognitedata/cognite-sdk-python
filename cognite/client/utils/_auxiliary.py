"""Utilities for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not be used by end-users.
"""
from __future__ import annotations

import functools
import heapq
import importlib
import math
import numbers
import platform
import random
import re
import string
import warnings
from decimal import Decimal
from types import ModuleType
from typing import (
    Any,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    overload,
)
from urllib.parse import quote

import cognite.client
from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._version_checker import get_newest_version_in_major_release

T = TypeVar("T")
THashable = TypeVar("THashable", bound=Hashable)


def is_unlimited(limit: Optional[Union[float, int]]) -> bool:
    return limit in {None, -1, math.inf}


@functools.lru_cache(maxsize=128)
def to_camel_case(snake_case_string: str) -> str:
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


@functools.lru_cache(maxsize=128)
def to_snake_case(camel_case_string: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def convert_all_keys_to_camel_case(d: Dict[str, Any]) -> Dict[str, Any]:
    return dict(zip(map(to_camel_case, d.keys()), d.values()))


def convert_all_keys_to_snake_case(d: Dict[str, Any]) -> Dict[str, Any]:
    return dict(zip(map(to_snake_case, d.keys()), d.values()))


def basic_obj_dump(obj: Any, camel_case: bool) -> Dict[str, Any]:
    if camel_case:
        return convert_all_keys_to_camel_case(vars(obj))
    return convert_all_keys_to_snake_case(vars(obj))


def handle_deprecated_camel_case_argument(new_arg: T, old_arg_name: str, fn_name: str, kw_dct: Dict[str, Any]) -> T:
    old_arg = kw_dct.pop(old_arg_name, None)
    if kw_dct:
        raise TypeError(f"Got unexpected keyword argument(s): {list(kw_dct)}")

    new_arg_name = to_snake_case(old_arg_name)
    if old_arg is None:
        if new_arg is None:
            raise TypeError(f"{fn_name}() missing 1 required positional argument: '{new_arg_name}'")
        return new_arg

    warnings.warn(
        f"Argument '{old_arg_name}' have been changed to '{new_arg_name}', but the old is still supported until "
        "the next major version. Consider updating your code.",
        UserWarning,
        stacklevel=2,
    )
    if new_arg is not None:
        raise TypeError(f"Pass either '{new_arg_name}' or '{old_arg_name}' (deprecated), not both")
    return old_arg


def json_dump_default(x: Any) -> Any:
    if isinstance(x, numbers.Integral):
        return int(x)
    if isinstance(x, (Decimal, numbers.Real)):
        return float(x)
    if hasattr(x, "__dict__"):
        return x.__dict__
    raise TypeError(f"Object {x} of type {x.__class__} can't be serialized by the JSON encoder")


def unwrap_identifer(identifier: Union[str, int, Dict]) -> Union[str, int]:
    # TODO: Move to Identifier class?
    if isinstance(identifier, (str, int)):
        return identifier
    if "externalId" in identifier:
        return identifier["externalId"]
    if "id" in identifier:
        return identifier["id"]
    raise ValueError(f"{identifier} does not contain 'id' or 'externalId'")


def assert_type(var: Any, var_name: str, types: List[type], allow_none: bool = False) -> None:
    if var is None:
        if not allow_none:
            raise TypeError(f"{var_name} cannot be None")
    elif not isinstance(var, tuple(types)):
        raise TypeError(f"{var_name} must be one of types {types}")


def interpolate_and_url_encode(path: str, *args: Any) -> str:
    return path.format(*[quote(str(arg), safe="") for arg in args])


def local_import(*module: str) -> Union[ModuleType, Tuple[ModuleType, ...]]:
    assert_type(module, "module", [tuple])
    if len(module) == 1:
        name = module[0]
        try:
            return importlib.import_module(name)
        except ImportError as e:
            raise CogniteImportError(name.split(".")[0]) from e

    modules = []
    for name in module:
        try:
            modules.append(importlib.import_module(name))
        except ImportError as e:
            raise CogniteImportError(name.split(".")[0]) from e
    return tuple(modules)


def import_legacy_protobuf() -> bool:
    from google.protobuf import __version__ as pb_version

    return 4 > int(pb_version.split(".")[0])


def get_current_sdk_version() -> str:
    return cognite.client.__version__


@functools.lru_cache(maxsize=1)
def get_user_agent() -> str:
    sdk_version = f"CognitePythonSDK/{get_current_sdk_version()}"
    python_version = (
        f"{platform.python_implementation()}/{platform.python_version()} "
        f"({platform.python_build()};{platform.python_compiler()})"
    )
    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]  # Ignore empty strings
    os_version_info_str = "-".join(os_version_info)
    operating_system = f"{platform.system()}/{os_version_info_str}"

    return f"{sdk_version} {python_version} {operating_system}"


def _check_client_has_newest_major_version() -> None:
    version = get_current_sdk_version()
    newest_version = get_newest_version_in_major_release("cognite-sdk", version)
    if newest_version != version:
        warnings.warn(
            f"You are using {version=} of the SDK, however version='{newest_version}' is available. "
            "To suppress this warning, either upgrade or do the following:\n"
            ">>> from cognite.client.config import global_config\n"
            ">>> global_config.disable_pypi_version_check = True",
            stacklevel=3,
        )


def random_string(size: int = 100, sample_from: str = string.ascii_uppercase + string.digits) -> str:
    return "".join(random.choices(sample_from, k=size))


class PriorityQueue:
    # TODO: Just use queue.PriorityQueue()
    def __init__(self) -> None:
        self.__heap: List[Any] = []
        self.__id = 0

    def add(self, elem: Any, priority: int) -> None:
        heapq.heappush(self.__heap, (-priority, self.__id, elem))
        self.__id += 1

    def get(self) -> Any:
        _, _, elem = heapq.heappop(self.__heap)
        return elem

    def __bool__(self) -> bool:
        return len(self.__heap) > 0


@overload
def split_into_n_parts(seq: List[T], /, n: int) -> Iterator[List[T]]:
    ...


@overload
def split_into_n_parts(seq: Sequence[T], /, n: int) -> Iterator[Sequence[T]]:
    ...


def split_into_n_parts(seq: Sequence[T], /, n: int) -> Iterator[Sequence[T]]:
    # NB: Chaotic sampling: jumps n for each starting position
    yield from (seq[i::n] for i in range(n))


def split_into_chunks(collection: Union[List, Dict], chunk_size: int) -> List[Union[List, Dict]]:
    chunks: List[Union[List, Dict]] = []
    if isinstance(collection, list):
        for i in range(0, len(collection), chunk_size):
            chunks.append(collection[i : i + chunk_size])
        return chunks
    if isinstance(collection, dict):
        collection = list(collection.items())
        for i in range(0, len(collection), chunk_size):
            chunks.append({k: v for k, v in collection[i : i + chunk_size]})
        return chunks
    raise ValueError("Can only split list or dict")


def convert_true_match(true_match: Union[dict, list, Tuple[Union[int, str], Union[int, str]]]) -> dict:
    if isinstance(true_match, Sequence) and len(true_match) == 2:
        converted_true_match = {}
        for i, fromto in enumerate(["source", "target"]):
            if isinstance(true_match[i], str):
                converted_true_match[fromto + "ExternalId"] = true_match[i]
            else:
                converted_true_match[fromto + "Id"] = true_match[i]
        return converted_true_match
    elif isinstance(true_match, dict):
        return true_match
    else:
        raise ValueError(f"true_matches should be a dictionary or a two-element list: found {true_match}")


def find_duplicates(seq: Iterable[THashable]) -> Set[THashable]:
    seen: Set[THashable] = set()
    add = seen.add  # skip future attr lookups for perf
    return {x for x in seq if x in seen or add(x)}


def exactly_one_is_not_none(*args: Any) -> bool:
    return sum(1 if a is not None else 0 for a in args) == 1
