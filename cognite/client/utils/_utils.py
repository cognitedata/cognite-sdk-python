"""Utilites for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import functools
import heapq
import importlib
import logging
import platform
import random
import re
import string
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Tuple, Union
from urllib.parse import quote

import cognite.client
from cognite.client.exceptions import CogniteAPIError, CogniteImportError

_unit_in_ms_without_week = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
_unit_in_ms = {**_unit_in_ms_without_week, "w": 604800000}


def datetime_to_ms(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def ms_to_datetime(ms: Union[int, float]) -> datetime:
    """Converts milliseconds since epoch to datetime object.

    Args:
        ms (Union[int, float]): Milliseconds since epoch

    Returns:
        datetime: Datetime object.

    """
    return datetime.utcfromtimestamp(ms / 1000)


def time_string_to_ms(pattern, string, unit_in_ms):
    pattern = pattern.format("|".join(unit_in_ms))
    res = re.fullmatch(pattern, string)
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        return magnitude * unit_in_ms[unit]
    return None


def granularity_to_ms(granularity: str) -> int:
    ms = time_string_to_ms(r"(\d+)({})", granularity, _unit_in_ms_without_week)
    if ms is None:
        raise ValueError(
            "Invalid granularity format: `{}`. Must be on format <integer>(s|m|h|d). E.g. '5m', '3h' or '1d'.".format(
                granularity
            )
        )
    return ms


def granularity_unit_to_ms(granularity: str) -> int:
    granularity = re.sub(r"^\d+", "1", granularity)
    return granularity_to_ms(granularity)


def time_ago_to_ms(time_ago_string: str) -> int:
    """Returns millisecond representation of time-ago string"""
    if time_ago_string == "now":
        return 0
    ms = time_string_to_ms(r"(\d+)({})-ago", time_ago_string, _unit_in_ms)
    if ms is None:
        raise ValueError(
            "Invalid time-ago format: `{}`. Must be on format <integer>(s|m|h|d|w)-ago or 'now'. E.g. '3d-ago' or '1w-ago'.".format(
                time_ago_string
            )
        )
    return ms


def timestamp_to_ms(timestamp: Union[int, float, str, datetime]) -> int:
    """Returns the ms representation of some timestamp given by milliseconds, time-ago format or datetime object

    Args:
        timestamp (Union[int, float, str, datetime]): Convert this timestamp to ms.

    Returns:
        int: Milliseconds since epoch representation of timestamp
    """
    if isinstance(timestamp, int):
        ms = timestamp
    elif isinstance(timestamp, float):
        ms = int(timestamp)
    elif isinstance(timestamp, str):
        ms = int(round(time.time() * 1000)) - time_ago_to_ms(timestamp)
    elif isinstance(timestamp, datetime):
        ms = datetime_to_ms(timestamp)
    else:
        raise TypeError(
            "Timestamp `{}` was of type {}, but must be int, float, str or datetime,".format(timestamp, type(timestamp))
        )

    if ms < 0:
        raise ValueError(
            "Timestamps can't be negative - they must represent a time after 1.1.1970, but {} was provided".format(ms)
        )

    return ms


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
    try:
        if isinstance(x, local_import("numpy").integer):
            return int(x)
    except CogniteImportError:
        pass
    if isinstance(x, Decimal):
        return float(x)
    if hasattr(x, "__dict__"):
        return x.__dict__
    return x


class TasksSummary:
    def __init__(self, successful_tasks, unknown_tasks, failed_tasks, results, exceptions):
        self.successful_tasks = successful_tasks
        self.unknown_tasks = unknown_tasks
        self.failed_tasks = failed_tasks
        self.results = results
        self.exceptions = exceptions

    def joined_results(self, unwrap_fn: Callable = None):
        unwrap_fn = unwrap_fn or (lambda x: x)
        joined_results = []
        for result in self.results:
            unwrapped = unwrap_fn(result)
            if isinstance(unwrapped, list):
                joined_results.extend(unwrapped)
            else:
                joined_results.append(unwrapped)
        return joined_results

    def raise_compound_exception_if_failed_tasks(
        self,
        task_unwrap_fn: Callable = None,
        task_list_element_unwrap_fn: Callable = None,
        str_format_element_fn: Callable = None,
    ):
        if self.exceptions:
            unwrap_fn = task_unwrap_fn or (lambda x: x)
            if task_list_element_unwrap_fn is not None:
                el_unwrap = task_list_element_unwrap_fn
                successful = []
                for t in self.successful_tasks:
                    successful.extend([el_unwrap(el) for el in unwrap_fn(t)])
                unknown = []
                for t in self.unknown_tasks:
                    unknown.extend([el_unwrap(el) for el in unwrap_fn(t)])
                failed = []
                for t in self.failed_tasks:
                    failed.extend([el_unwrap(el) for el in unwrap_fn(t)])
            else:
                successful = [unwrap_fn(t) for t in self.successful_tasks]
                unknown = [unwrap_fn(t) for t in self.unknown_tasks]
                failed = [unwrap_fn(t) for t in self.failed_tasks]
            if isinstance(self.exceptions[0], CogniteAPIError):
                exc = self.exceptions[0]
                raise CogniteAPIError(
                    exc.message,
                    exc.code,
                    exc.x_request_id,
                    exc.extra,
                    successful,
                    failed,
                    unknown,
                    str_format_element_fn,
                )
            raise self.exceptions[0]


def execute_tasks_concurrently(func: Callable, tasks: Union[List[Tuple], List[Dict]], max_workers: int):
    assert max_workers > 0, "Number of workers should be >= 1, was {}".format(max_workers)
    with ThreadPoolExecutor(max_workers) as p:
        futures = []
        for task in tasks:
            if isinstance(task, dict):
                futures.append(p.submit(func, **task))
            elif isinstance(task, tuple):
                futures.append(p.submit(func, *task))

        successful_tasks = []
        failed_tasks = []
        unknown_result_tasks = []
        results = []
        exceptions = []
        for i, f in enumerate(futures):
            try:
                res = f.result()
                successful_tasks.append(tasks[i])
                results.append(res)
            except Exception as e:
                exceptions.append(e)
                if isinstance(e, CogniteAPIError):
                    if e.code < 500:
                        failed_tasks.append(tasks[i])
                    else:
                        unknown_result_tasks.append(tasks[i])
                else:
                    failed_tasks.append(tasks[i])

        return TasksSummary(successful_tasks, unknown_result_tasks, failed_tasks, results, exceptions)


def assert_exactly_one_of_id_or_external_id(id, external_id):
    assert_type(id, "id", [int], allow_none=True)
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
    assert_type(id, "id", [int], allow_none=True)
    assert_type(external_id, "external_id", [str], allow_none=True)
    has_id = id is not None
    has_external_id = external_id is not None
    assert has_id or has_external_id, "At least one of id and external id must be specified"
    if has_id:
        return {"id": id}
    elif has_external_id:
        return {"external_id": external_id}


def unwrap_identifer(identifier: Dict):
    if "externalId" in identifier:
        return identifier["externalId"]
    if "id" in identifier:
        return identifier["id"]
    raise ValueError("{} does not contain 'id' or 'externalId'".format(identifier))


def assert_timestamp_not_in_jan_in_1970(timestamp: Union[int, float, str, datetime]):
    dt = ms_to_datetime(timestamp_to_ms(timestamp))
    assert dt > datetime(
        1970, 2, 1
    ), "You are attempting to post data in January 1970. Have you forgotten to multiply your timestamps by 1000?"


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


class DebugLogFormatter(logging.Formatter):
    RESERVED_ATTRS = (
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    )

    def __init__(self):
        fmt = "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(threadName)s. %(message)s (%(filename)s:%(lineno)s)"
        datefmt = "%Y-%m-%d %H:%M:%S"
        super().__init__(fmt, datefmt)

    def format(self, record):

        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        s_extra_objs = []
        for attr, value in record.__dict__.items():
            if attr not in self.RESERVED_ATTRS:
                s_extra_objs.append("\n    - {}: {}".format(attr, value))
        for s_extra in s_extra_objs:
            s += s_extra
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        return s


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


def _convert_time_attributes_in_dict(item: Dict) -> Dict:
    TIME_ATTRIBUTES = ["start_time", "end_time", "last_updated_time", "created_time", "timestamp"]
    new_item = {}
    for k, v in item.items():
        if k in TIME_ATTRIBUTES:
            v = ms_to_datetime(v).strftime("%Y-%m-%d %H:%M:%S")
        new_item[k] = v
    return new_item


def convert_time_attributes_to_datetime(item: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
    if isinstance(item, dict):
        return _convert_time_attributes_in_dict(item)
    if isinstance(item, list):
        new_items = []
        for el in item:
            new_items.append(_convert_time_attributes_in_dict(el))
        return new_items
    raise TypeError("item must be dict or list of dicts")
