from __future__ import annotations

import numbers
import re
import sys
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast, overload

from cognite.client.utils._importing import local_import
from cognite.client.utils._text import to_camel_case

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
else:
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from datetime import tzinfo


UTC = ZoneInfo("UTC")  # type: ignore [abstract]
UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999
_GRANULARITY_UNIT_LOOKUP: dict[str, str] = {
    "s": "s",
    "sec": "s",
    "second": "s",
    "seconds": "s",
    "t": "m",
    "m": "m",
    "min": "m",
    "minute": "m",
    "minutes": "m",
    "h": "h",
    "hour": "h",
    "hours": "h",
    "d": "d",
    "day": "d",
    "days": "d",
    "w": "w",
    "week": "w",
    "weeks": "w",
    "mo": "month",
    "month": "month",
    "months": "month",
    "q": "quarter",
    "quarter": "quarter",
    "quarters": "quarter",
    "y": "year",
    "year": "year",
    "years": "year",
}
_GRANULARITY_CONVERSION = {
    "s": (1, "s"),
    "m": (1, "m"),
    "h": (1, "h"),
    "d": (1, "d"),
    "w": (7, "d"),
    "month": (1, "mo"),
    "quarter": (3, "mo"),
    "year": (12, "mo"),
}


def parse_str_timezone_offset(tz: str) -> timezone:
    """
    This function attempts to accept and convert all valid fixed-offset timezone input that the API
    supports for datapoints endpoints. The backend is using native java class TimeZone with some
    added restrictions on ambiguous names/ids.
    """
    prefix, tz = "", tz.replace(" ", "")
    if match := re.match("^(UTC?|GMT)?", tz):
        tz = tz.replace(prefix := match.group(), "")
    if prefix and not tz:
        return timezone.utc
    elif re.match(r"^(-|\+)\d\d?$", tz) and abs(hours_offset := int(tz)) <= 18:
        return timezone(timedelta(hours=hours_offset))
    return cast(timezone, datetime.strptime(tz, "%z").tzinfo)


def parse_str_timezone(tz: str) -> timezone | ZoneInfo:
    try:
        return ZoneInfo(tz)  # type: ignore [abstract]
    except ZoneInfoNotFoundError:
        try:
            return parse_str_timezone_offset(tz)
        except ValueError:
            raise ValueError(
                f"Unable to parse string timezone {tz!r}, expected an UTC offset like UTC-02, UTC+01:30, +0400 "
                "or an IANA timezone on the format Region/Location like Europe/Oslo, Asia/Tokyo or America/Los_Angeles"
            )


def convert_timezone_to_str(tz: timezone | ZoneInfo) -> str:
    if isinstance(tz, timezone):
        # Built-in timezones can only represent fixed UTC offsets (i.e. we do not allow arbitrary
        # tzinfo subclasses). We could do str(tz), but if the user has passed a name, that is
        # returned instead so we have to first get the utc offset:
        return str(timezone(tz.utcoffset(None)))
    elif isinstance(tz, ZoneInfo):
        if tz.key is not None:
            return tz.key
        else:
            raise ValueError("timezone of type ZoneInfo does not have the required 'key' attribute set")
    raise TypeError(f"timezone must be datetime.timezone or zoneinfo.ZoneInfo, not {type(tz)}")


def datetime_to_ms(dt: datetime) -> int:
    """Converts a datetime object to milliseconds since epoch.

    Args:
        dt (datetime): Naive or aware datetime object. Naive datetimes are interpreted as local time.

    Returns:
        int: Milliseconds since epoch (negative for time prior to 1970-01-01)
    """
    try:
        return int(1000 * dt.timestamp())
    except OSError as e:
        # OSError is raised if dt.timestamp() is called before 1970-01-01 on Windows for naive datetime.
        raise ValueError(
            "Failed to convert datetime to epoch. This likely because you are using a naive datetime. "
            "Try using a timezone aware datetime instead."
        ) from e


def ms_to_datetime(ms: int | float) -> datetime:
    """Converts valid Cognite timestamps, i.e. milliseconds since epoch, to datetime object.

    Args:
        ms (int | float): Milliseconds since epoch.

    Raises:
        ValueError: On invalid Cognite timestamps.

    Returns:
        datetime: Aware datetime object in UTC.
    """
    if not (MIN_TIMESTAMP_MS <= ms <= MAX_TIMESTAMP_MS):
        raise ValueError(f"Input {ms=} does not satisfy: {MIN_TIMESTAMP_MS} <= ms <= {MAX_TIMESTAMP_MS}")

    # Note: We don't use fromtimestamp because it typically fails for negative values on Windows
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)


def datetime_to_ms_iso_timestamp(dt: datetime) -> str:
    """Converts a datetime object to a string representing a timestamp in the ISO-format expected by the Cognite GraphQL API.

    Args:
        dt (datetime): Naive or aware datetime object. Naive datetimes are interpreted as local time.

    Raises:
        TypeError: If dt is not a datetime object

    Returns:
        str: Timestamp string in ISO 8601 format with milliseconds
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return dt.isoformat(timespec="milliseconds")
    raise TypeError(f"Expected datetime object, got {type(dt)}")


def split_granularity_into_quantity_and_normalized_unit(granularity: str) -> tuple[int, str]:
    """A normalized unit is any unit accepted by the DatapointsAPI"""
    if match := re.match(r"(\d+)(.*)", granularity):
        quantity, unit = match.groups()
        # We accept a whole range of different formats like s, sec, second
        if normalized_unit := _GRANULARITY_UNIT_LOOKUP.get(unit):
            multiplier, normalized_unit = _GRANULARITY_CONVERSION[normalized_unit]
            return int(quantity) * multiplier, normalized_unit
    raise ValueError(
        f"Invalid granularity format: `{granularity}`. Must be on format <quantity><unit>, e.g. 5m, 3h, 1d, or 2w. "
        "Tip: Unit can be spelled out for clarity, e.g. week(s), month(s), quarter(s), or year(s)."
    )


def time_string_to_ms(pattern: str, string: str, unit_in_ms: dict[str, int]) -> int | None:
    pattern = pattern.format("|".join(unit_in_ms))
    if res := re.fullmatch(pattern, string):
        magnitude = int(res[1])
        unit = res[2]
        return magnitude * unit_in_ms[unit]
    return None


def granularity_to_ms(granularity: str, as_unit: bool = False) -> int:
    ms = time_string_to_ms(
        r"(\d+)({})",
        re.sub(r"^\d+", "1", granularity) if as_unit else granularity,
        UNIT_IN_MS_WITHOUT_WEEK,
    )
    if ms is None:
        raise ValueError(
            f"Invalid granularity format: `{granularity}`. Must be on format <integer>(s|m|h|d). "
            "E.g. '5m', '3h' or '1d'."
        )
    return ms


def time_ago_to_ms(time_ago_string: str) -> int:
    """Returns millisecond representation of time-ago string"""
    if time_ago_string == "now":
        return 0
    ms = time_string_to_ms(r"(\d+)({})-ago", time_ago_string, UNIT_IN_MS)
    if ms is None:
        raise ValueError(
            f"Invalid time-ago format: `{time_ago_string}`. Must be on format <integer>(s|m|h|d|w)-ago or 'now'. "
            "E.g. '3d-ago' or '1w-ago'."
        )
    return ms


def timestamp_to_ms(timestamp: int | float | str | datetime) -> int:
    """Returns the ms representation of some timestamp given by milliseconds, time-ago format or datetime object

    Args:
        timestamp (int | float | str | datetime): Convert this timestamp to ms.

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
            f"Timestamp `{timestamp}` was of type {type(timestamp)}, but must be int, float, str or datetime,"
        )

    if ms < MIN_TIMESTAMP_MS:
        raise ValueError(f"Timestamps must represent a time after 1.1.1900, but {ms} was provided")

    return ms


TIME_ATTRIBUTES = {
    "created_time",
    "creation_time",
    "deleted_time",
    "end_time",
    "expiration_time",
    "last_failure",
    "last_indexed_time",
    "last_seen",
    "last_success",
    "last_updated_time",
    "modified_time",
    "scheduled_execution_time",
    "source_created_time",
    "source_modified_time",
    "start_time",
    "timestamp",
    "uploaded_time",
}
TIME_ATTRIBUTES |= set(map(to_camel_case, TIME_ATTRIBUTES))


def convert_and_isoformat_timestamp(ts: int, tz: timezone | ZoneInfo | None) -> str:
    """Used in datapoints classes that are fetched with a 'timezone'"""
    dt = ms_to_datetime(ts)
    if tz is not None:
        dt = dt.astimezone(tz)
    return dt.isoformat(sep=" ", timespec="milliseconds")


def _convert_and_isoformat_time_attrs_in_dict(item: dict) -> dict:
    for k in TIME_ATTRIBUTES.intersection(item):
        try:
            item[k] = ms_to_datetime(item[k]).isoformat(sep=" ", timespec="milliseconds")
        except ValueError:
            pass
    return item


@overload
def convert_and_isoformat_time_attrs(item: dict) -> dict: ...


@overload
def convert_and_isoformat_time_attrs(item: list[dict]) -> list[dict]: ...


def convert_and_isoformat_time_attrs(item: dict | list[dict]) -> dict | list[dict]:
    if isinstance(item, dict):
        return _convert_and_isoformat_time_attrs_in_dict(item)
    if isinstance(item, list):
        return [_convert_and_isoformat_time_attrs_in_dict(it) for it in item]
    raise TypeError("item must be dict or list of dicts")


def align_start_and_end_for_granularity(start: int, end: int, granularity: str) -> tuple[int, int]:
    # Note the API always aligns `start` with 1s, 1m, 1h or 1d (even when given e.g. 73h)
    if remainder := start % granularity_to_ms(granularity, as_unit=True):
        # Floor `start` when not exactly at boundary
        start -= remainder
    gms = granularity_to_ms(granularity)
    if remainder := (end - start) % gms:
        # Ceil `end` when not exactly at boundary decided by `start + N * granularity`
        end += gms - remainder
    return start, end


def split_time_range(start: int, end: int, n_splits: int, granularity_in_ms: int) -> list[int]:
    if n_splits < 1:
        raise ValueError(f"Cannot split into less than 1 piece, got {n_splits=}")
    tot_ms = end - start
    if n_splits * granularity_in_ms > tot_ms:
        raise ValueError(
            f"Given time interval ({tot_ms=}) could not be split as `{n_splits=}` times `{granularity_in_ms=}` "
            "is larger than the interval itself."
        )
    # Find a `delta_ms` that's a multiple of granularity in ms (trivial for raw queries).
    delta_ms = granularity_in_ms * round(tot_ms / n_splits / granularity_in_ms)
    return [*(start + delta_ms * i for i in range(n_splits)), end]


def _timezones_are_equal(start_tz: tzinfo, end_tz: tzinfo) -> bool:
    """There are unfortunately several ways to pass/represent the same timezone (without it being a user error).
    For example pandas uses 'pytz' under the hood -except- for UTC, then it uses the built-in `datetime.timezone.utc`
    -except- when given something concrete like pytz.UTC or ZoneInfo(...).

    To make sure we don't raise something silly like 'UTC != UTC', we convert both to ZoneInfo for comparison
    via str(). This is safe as all return the lookup key (for the IANA timezone database).

    Note:
        We do not consider timezones with different keys, but equal fixed offsets from UTC to be equal. An example
        would be Zulu Time (which is +00:00 ahead of UTC) and UTC.
    """
    if start_tz is end_tz:
        return True
    with suppress(ValueError, ZoneInfoNotFoundError):
        # ValueError is raised for non-conforming keys (ZoneInfoNotFoundError is self-explanatory)
        if ZoneInfo(str(start_tz)) is ZoneInfo(str(end_tz)):  # type: ignore [abstract]
            return True
    return False


def validate_timezone(start: datetime, end: datetime) -> ZoneInfo:
    if (start_tz := start.tzinfo) is None or (end_tz := end.tzinfo) is None:
        missing = [name for name, timestamp in zip(("start", "end"), (start, end)) if not timestamp.tzinfo]
        names = " and ".join(missing)
        end_sentence = " do not have timezones." if len(missing) >= 2 else " does not have a timezone."
        raise ValueError(f"All times must be timezone aware, {names}{end_sentence}")

    if not _timezones_are_equal(start_tz, end_tz):
        raise ValueError(f"'start' and 'end' represent different timezones: '{start_tz}' and '{end_tz}'.")

    if isinstance(start_tz, ZoneInfo):
        return start_tz

    pd = local_import("pandas")
    if isinstance(start, pd.Timestamp):
        return ZoneInfo(str(start_tz))  # type: ignore [abstract]

    raise ValueError("Only tz-aware pandas.Timestamp and datetime (must be using ZoneInfo) are supported.")


__all__ = ["ZoneInfo", "ZoneInfoNotFoundError"]  # Fix: Module does not explicitly export attribute "ZoneInfo
