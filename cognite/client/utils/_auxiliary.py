"""Utilites for Cognite API SDK

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
from typing import Any, Dict, List, Union
from urllib.parse import quote

import cognite.client
from cognite.client import utils
from cognite.client.exceptions import CogniteImportError


@functools.lru_cache(maxsize=128)
def to_camel_case(snake_case_string: str):
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


@functools.lru_cache(maxsize=128)
def to_snake_case(camel_case_string: str):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def convert_all_keys_to_camel_case(d: Dict):
    new_d = {}
    for k, v in d.items():
        new_d[to_camel_case(k)] = v
    return new_d


def json_dump_default(x):
    if isinstance(x, numbers.Integral):
        return int(x)
    if isinstance(x, (Decimal, numbers.Real)):
        return float(x)
    if hasattr(x, "__dict__"):
        return x.__dict__
    raise TypeError("Object {} of type {} can't be serialized by the JSON encoder".format(x, x.__class__))


def assert_exactly_one_of_id_or_external_id(id, external_id):
    assert_type(id, "id", [numbers.Integral], allow_none=True)
    assert_type(external_id, "external_id", [str], allow_none=True)
    has_id = id is not None
    has_external_id = external_id is not None

    assert (has_id or has_external_id) and not (
        has_id and has_external_id
    ), "Exactly one of id and external id must be specified"

    if has_id:
        return {"id": id}
    elif has_external_id:
        return {"external_id": external_id}


def assert_at_least_one_of_id_or_external_id(id, external_id):
    assert_type(id, "id", [numbers.Integral], allow_none=True)
    assert_type(external_id, "external_id", [str], allow_none=True)
    has_id = id is not None
    has_external_id = external_id is not None
    assert has_id or has_external_id, "At least one of id and external id must be specified"
    if has_id:
        return {"id": id}
    elif has_external_id:
        return {"external_id": external_id}


def unwrap_identifer(identifier: Union[str, int, Dict]):
    if type(identifier) in [str, int]:
        return identifier
    if "externalId" in identifier:
        return identifier["externalId"]
    if "id" in identifier:
        return identifier["id"]
    raise ValueError("{} does not contain 'id' or 'externalId'".format(identifier))


def assert_type(var: Any, var_name: str, types: List, allow_none=False):
    if var is None:
        if not allow_none:
            raise TypeError("{} cannot be None".format(var_name))
    elif not isinstance(var, tuple(types)):
        raise TypeError("{} must be one of types {}".format(var_name, types))


def interpolate_and_url_encode(path, *args):
    return path.format(*[quote(str(arg), safe="") for arg in args])


def local_import(*module: str):
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


def get_current_sdk_version():
    return cognite.client.__version__


@functools.lru_cache(maxsize=1)
def get_user_agent():
    sdk_version = "CognitePythonSDK/{}".format(get_current_sdk_version())

    python_version = "{}/{} ({};{})".format(
        platform.python_implementation(), platform.python_version(), platform.python_build(), platform.python_compiler()
    )

    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]  # Ignore empty strings
    os_version_info = "-".join(os_version_info)
    operating_system = "{}/{}".format(platform.system(), os_version_info)

    return "{} {} {}".format(sdk_version, python_version, operating_system)


def _check_client_has_newest_major_version():
    this_version = utils._auxiliary.get_current_sdk_version()
    newest_version = utils._version_checker.get_newest_version_in_major_release("cognite-sdk", this_version)
    if newest_version != this_version:
        warnings.warn(
            "You are using version {} of the SDK, however version {} is available. "
            "Upgrade or set the environment variable 'COGNITE_DISABLE_PYPI_VERSION_CHECK' to suppress this "
            "warning.".format(this_version, newest_version),
            stacklevel=3,
        )


def random_string(size=100):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


class PriorityQueue:
    def __init__(self):
        self.heap = []
        self.id = 0

    def add(self, elem, priority):
        heapq.heappush(self.heap, (-priority, self.id, elem))
        self.id += 1

    def get(self):
        _, _, elem = heapq.heappop(self.heap)
        return elem

    def __bool__(self):
        return len(self.heap) > 0


def split_into_chunks(collection: Union[List, Dict], chunk_size: int) -> List[Union[List, Dict]]:
    chunks = []
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


def convert_true_match(true_match):
    if not isinstance(true_match, dict) and len(true_match) == 2:
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
