"""Utilites for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import platform
import re
import time
from collections import namedtuple
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple, Union

import cognite.client

_unit_in_ms_without_week = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
_unit_in_ms = {**_unit_in_ms_without_week, "w": 604800000}


def datetime_to_ms(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def ms_to_datetime(ms):
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


def timestamp_to_ms(t: Union[int, float, str, datetime]):
    """Returns the ms representation of some timestamp given by milliseconds, time-ago format or datetime object"""
    if isinstance(t, int):
        ms = t
    elif isinstance(t, float):
        ms = int(t)
    elif isinstance(t, str):
        ms = int(round(time.time() * 1000)) - time_ago_to_ms(t)
    elif isinstance(t, datetime):
        ms = datetime_to_ms(t)
    else:
        raise TypeError("Timestamp `{}` was of type {}, but must be int, str or datetime,".format(t, type(t)))

    if ms < 0:
        raise ValueError(
            "Timestamps can't be negative - they must represent a time after 1.1.1970, but {} was provided".format(ms)
        )

    return ms


def to_camel_case(snake_case_string: str):
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def to_snake_case(camel_case_string: str):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def execute_tasks_concurrently(func: Callable, tasks: Union[List[Tuple], List[Dict]], max_workers: int):
    assert max_workers > 0, "Number of workers should be >= 1, was {}".format(max_workers)
    with ThreadPoolExecutor(max_workers) as p:
        futures = []
        for task in tasks:
            if isinstance(task, dict):
                futures.append(p.submit(func, **task))
            elif isinstance(task, tuple):
                futures.append(p.submit(func, *task))
        return [future.result() for future in futures]


def assert_exactly_one_of_id_or_external_id(id, external_id):
    assert_type(id, "id", [int], allow_none=True)
    assert_type(external_id, "external_id", [str], allow_none=True)
    assert (id or external_id) and not (id and external_id), "Exactly one of id and external id must be specified"


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


def get_user_agent():
    sdk_version = "CognitePythonSDK/{}".format(cognite.client.__version__)

    python_version = "{}/{} ({};{})".format(
        platform.python_implementation(), platform.python_version(), platform.python_build(), platform.python_compiler()
    )

    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]  # Ignore empty strings
    os_version_info = "-".join(os_version_info)
    operating_system = "{}/{}".format(platform.system(), os_version_info)

    return "{} {} {}".format(sdk_version, python_version, operating_system)
