import re
import time
from datetime import datetime
from typing import Dict, List, Union

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
    if ms < 0:
        raise ValueError("ms must be greater than or equal to zero.")
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


def _convert_time_attributes_in_dict(item: Dict) -> Dict:
    TIME_ATTRIBUTES = ["start_time", "end_time", "last_updated_time", "created_time", "timestamp"]
    new_item = {}
    for k, v in item.items():
        if k in TIME_ATTRIBUTES:
            try:
                v = ms_to_datetime(v).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError):
                pass
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
