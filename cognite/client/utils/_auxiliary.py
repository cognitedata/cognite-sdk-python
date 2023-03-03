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
from collections.abc import Mapping
from decimal import Decimal
from typing import Hashable, Sequence, Set, TypeVar
from urllib.parse import quote

import cognite.client
from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._version_checker import get_newest_version_in_major_release

T = TypeVar("T")
THashable = TypeVar("THashable", bound=Hashable)


def is_unlimited(limit):
    return limit in {None, (-1), math.inf}


@functools.lru_cache(maxsize=128)
def to_camel_case(snake_case_string):
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


@functools.lru_cache(maxsize=128)
def to_snake_case(camel_case_string):
    s1 = re.sub("(.)([A-Z][a-z]+)", "\\1_\\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", "\\1_\\2", s1).lower()


def convert_all_keys_to_camel_case(d):
    return dict(zip(map(to_camel_case, d.keys()), d.values()))


def convert_all_keys_to_snake_case(d):
    return dict(zip(map(to_snake_case, d.keys()), d.values()))


def basic_obj_dump(obj, camel_case):
    if camel_case:
        return convert_all_keys_to_camel_case(vars(obj))
    return convert_all_keys_to_snake_case(vars(obj))


def handle_deprecated_camel_case_argument(new_arg, old_arg_name, fn_name, kw_dct):
    old_arg = kw_dct.pop(old_arg_name, None)
    if kw_dct:
        raise TypeError(f"Got unexpected keyword argument(s): {list(kw_dct)}")
    new_arg_name = to_snake_case(old_arg_name)
    if old_arg is None:
        if new_arg is None:
            raise TypeError(f"{fn_name}() missing 1 required positional argument: '{new_arg_name}'")
        return new_arg
    warnings.warn(
        f"Argument '{old_arg_name}' have been changed to '{new_arg_name}', but the old is still supported until the next major version. Consider updating your code.",
        UserWarning,
        stacklevel=2,
    )
    if new_arg is not None:
        raise TypeError(f"Pass either '{new_arg_name}' or '{old_arg_name}' (deprecated), not both")
    return old_arg


def json_dump_default(x):
    if isinstance(x, numbers.Integral):
        return int(x)
    if isinstance(x, (Decimal, numbers.Real)):
        return float(x)
    if hasattr(x, "__dict__"):
        return x.__dict__
    raise TypeError(f"Object {x} of type {x.__class__} can't be serialized by the JSON encoder")


def unwrap_identifer(identifier):
    if isinstance(identifier, (str, int)):
        return identifier
    if "externalId" in identifier:
        return identifier["externalId"]
    if "id" in identifier:
        return identifier["id"]
    raise ValueError(f"{identifier} does not contain 'id' or 'externalId'")


def assert_type(var, var_name, types, allow_none=False):
    if var is None:
        if not allow_none:
            raise TypeError(f"{var_name} cannot be None")
    elif not isinstance(var, tuple(types)):
        raise TypeError(f"{var_name} must be one of types {types}")


def validate_user_input_dict_with_identifier(dct, required_keys):
    if not isinstance(dct, Mapping):
        raise TypeError(f"Expected dict-like object, got {type(dct)}")
    xid = dct.get("externalId") or dct.get("external_id")
    id_dct = Identifier.of_either(dct.get("id"), xid).as_dict(camel_case=True)
    missing_keys = required_keys.difference(dct)
    invalid_keys = (set(dct) - required_keys) - {"id", "externalId", "external_id"}
    if missing_keys or invalid_keys:
        raise ValueError(
            f"Given dictionary failed validation. Invalid key(s): {sorted(invalid_keys)}, required key(s) missing: {sorted(missing_keys)}."
        )
    return id_dct


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


def import_legacy_protobuf():
    from google.protobuf import __version__ as pb_version

    return 4 > int(pb_version.split(".")[0])


def get_current_sdk_version():
    return cognite.client.__version__


@functools.lru_cache(maxsize=1)
def get_user_agent():
    sdk_version = f"CognitePythonSDK/{get_current_sdk_version()}"
    python_version = f"{platform.python_implementation()}/{platform.python_version()} ({platform.python_build()};{platform.python_compiler()})"
    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]
    os_version_info_str = "-".join(os_version_info)
    operating_system = f"{platform.system()}/{os_version_info_str}"
    return f"{sdk_version} {python_version} {operating_system}"


def _check_client_has_newest_major_version():
    version = get_current_sdk_version()
    newest_version = get_newest_version_in_major_release("cognite-sdk", version)
    if newest_version != version:
        warnings.warn(
            f"""You are using version={version} of the SDK, however version='{newest_version}' is available. To suppress this warning, either upgrade or do the following:
>>> from cognite.client.config import global_config
>>> global_config.disable_pypi_version_check = True""",
            stacklevel=3,
        )


def random_string(size=100, sample_from=(string.ascii_uppercase + string.digits)):
    return "".join(random.choices(sample_from, k=size))


class PriorityQueue:
    def __init__(self):
        self.__heap = []
        self.__id = 0

    def add(self, elem, priority):
        heapq.heappush(self.__heap, ((-priority), self.__id, elem))
        self.__id += 1

    def get(self):
        (_, _, elem) = heapq.heappop(self.__heap)
        return elem

    def __bool__(self):
        return len(self.__heap) > 0


def split_into_n_parts(seq, n):
    (yield from (seq[i::n] for i in range(n)))


def split_into_chunks(collection, chunk_size):
    chunks = []
    if isinstance(collection, list):
        for i in range(0, len(collection), chunk_size):
            chunks.append(collection[i : (i + chunk_size)])
        return chunks
    if isinstance(collection, dict):
        collection = list(collection.items())
        for i in range(0, len(collection), chunk_size):
            chunks.append({k: v for (k, v) in collection[i : (i + chunk_size)]})
        return chunks
    raise ValueError("Can only split list or dict")


def convert_true_match(true_match):
    if isinstance(true_match, Sequence) and (len(true_match) == 2):
        converted_true_match = {}
        for (i, fromto) in enumerate(["source", "target"]):
            if isinstance(true_match[i], str):
                converted_true_match[(fromto + "ExternalId")] = true_match[i]
            else:
                converted_true_match[(fromto + "Id")] = true_match[i]
        return converted_true_match
    elif isinstance(true_match, dict):
        return true_match
    else:
        raise ValueError(f"true_matches should be a dictionary or a two-element list: found {true_match}")


def find_duplicates(seq):
    seen: Set[THashable] = set()
    add = seen.add
    return {x for x in seq if ((x in seen) or add(x))}


def exactly_one_is_not_none(*args):
    return sum((1 if (a is not None) else 0) for a in args) == 1
