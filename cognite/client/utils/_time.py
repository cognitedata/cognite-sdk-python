import numbers
import re
import time
import warnings
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

_unit_in_ms_without_week = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
_unit_in_ms = {**_unit_in_ms_without_week, "w": 604800000}

MIN_TIMESTAMP_MS = -2208988800000


def datetime_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        warnings.warn(
            "Interpreting given naive datetime as UTC instead of local time (against Python default behaviour). "
            "This will change in the next major release (4.0.0). Please use (timezone) aware datetimes "
            "or convert it yourself to integer (number of milliseconds since epoch, leap seconds excluded).",
            FutureWarning,
        )
        dt = dt.replace(tzinfo=timezone.utc)
    return int(1000 * dt.timestamp())


def ms_to_datetime(ms: Union[int, float]) -> datetime:
    """Converts milliseconds since epoch to datetime object.

    Args:
        ms (Union[int, float]): Milliseconds since epoch

    Returns:
        datetime: Naive datetime object in UTC.

    """
    if ms < 0:
        raise ValueError("ms must be greater than or equal to zero.")

    warnings.warn(
        "This function, `ms_to_datetime` returns a naive datetime object in UTC. This is against "
        "the default interpretation of naive datetimes in Python (i.e. local time). This behaviour will "
        "change to returning timezone-aware datetimes in UTC in the next major release (4.0.0).",
        FutureWarning,
    )
    return datetime.utcfromtimestamp(ms / 1000)


def time_string_to_ms(pattern: str, string: str, unit_in_ms: Dict[str, int]) -> Optional[int]:
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
    if isinstance(timestamp, numbers.Number):  # float, int, int64 etc
        ms = int(timestamp)  # type: ignore[arg-type]
    elif isinstance(timestamp, str):
        ms = int(round(time.time() * 1000)) - time_ago_to_ms(timestamp)
    elif isinstance(timestamp, datetime):
        ms = datetime_to_ms(timestamp)
    else:
        raise TypeError(
            "Timestamp `{}` was of type {}, but must be int, float, str or datetime,".format(timestamp, type(timestamp))
        )

    if ms < MIN_TIMESTAMP_MS:
        raise ValueError("Timestamps must represent a time after 1.1.1900, but {} was provided".format(ms))

    return ms


def _convert_time_attributes_in_dict(item: Dict) -> Dict:
    TIME_ATTRIBUTES = [
        "start_time",
        "end_time",
        "last_updated_time",
        "created_time",
        "timestamp",
        "scheduled_execution_time",
        "source_created_time",
        "source_modified_time",
    ]
    new_item = {}
    for k, v in item.items():
        if k in TIME_ATTRIBUTES:
            try:
                v = str(datetime.utcfromtimestamp(v / 1000))
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
