"""Utilities for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import functools
import heapq
import importlib
import numbers
import platform
import random
import re
import string
import warnings
from decimal import Decimal
from types import ModuleType
from typing import Any, Dict, List, Sequence, Tuple, Union
from urllib.parse import quote

import cognite.client
from cognite.client import utils
from cognite.client.exceptions import CogniteImportError


@functools.lru_cache(maxsize=128)
def to_camel_case(snake_case_string: str) -> str:
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


@functools.lru_cache(maxsize=128)
def to_snake_case(camel_case_string: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def convert_all_keys_to_camel_case(d: dict) -> dict:
    new_d = {}
    for k, v in d.items():
        new_d[to_camel_case(k)] = v
    return new_d


def json_dump_default(x: Any) -> Any:
    if isinstance(x, numbers.Integral):
        return int(x)
    if isinstance(x, (Decimal, numbers.Real)):
        return float(x)
    if hasattr(x, "__dict__"):
        return x.__dict__
    raise TypeError("Object {} of type {} can't be serialized by the JSON encoder".format(x, x.__class__))


def unwrap_identifer(identifier: Union[str, int, Dict]) -> Union[str, int]:
    if isinstance(identifier, (str, int)):
        return identifier
    if "externalId" in identifier:
        return identifier["externalId"]
    if "id" in identifier:
        return identifier["id"]
    raise ValueError("{} does not contain 'id' or 'externalId'".format(identifier))


def assert_type(var: Any, var_name: str, types: List[type], allow_none: bool = False) -> None:
    if var is None:
        if not allow_none:
            raise TypeError("{} cannot be None".format(var_name))
    elif not isinstance(var, tuple(types)):
        raise TypeError("{} must be one of types {}".format(var_name, types))


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


def get_current_sdk_version() -> str:
    return cognite.client.__version__


@functools.lru_cache(maxsize=1)
def get_user_agent() -> str:
    sdk_version = "CognitePythonSDK/{}".format(get_current_sdk_version())

    python_version = "{}/{} ({};{})".format(
        platform.python_implementation(), platform.python_version(), platform.python_build(), platform.python_compiler()
    )

    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]  # Ignore empty strings
    os_version_info_str = "-".join(os_version_info)
    operating_system = "{}/{}".format(platform.system(), os_version_info_str)

    return "{} {} {}".format(sdk_version, python_version, operating_system)


def _check_client_has_newest_major_version() -> None:
    this_version = utils._auxiliary.get_current_sdk_version()
    newest_version = utils._version_checker.get_newest_version_in_major_release("cognite-sdk", this_version)
    if newest_version != this_version:
        warnings.warn(
            "You are using version {} of the SDK, however version {} is available. "
            "Upgrade or set the environment variable 'COGNITE_DISABLE_PYPI_VERSION_CHECK' to suppress this "
            "warning.".format(this_version, newest_version),
            stacklevel=3,
        )


def random_string(size: int = 100) -> str:
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


class PriorityQueue:
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
        raise ValueError("true_matches should be a dictionary or a two-element list: found {}".format(true_match))
