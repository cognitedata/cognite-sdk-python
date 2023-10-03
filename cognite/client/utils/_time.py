from __future__ import annotations

import math
import numbers
import re
import sys
import time
from abc import ABC, abstractmethod
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast, overload

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._auxiliary import local_import

if TYPE_CHECKING:
    from datetime import tzinfo

    import pandas

    if sys.version_info >= (3, 9):
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    else:
        from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
VARIABLE_LENGTH_UNITS = {"month", "quarter", "year"}
GRANULARITY_IN_HOURS = {"w": 168, "d": 24, "h": 1}
GRANULARITY_IN_TIMEDELTA_UNIT = {"w": "weeks", "d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


def import_zoneinfo() -> type[ZoneInfo]:
    try:
        if sys.version_info >= (3, 9):
            from zoneinfo import ZoneInfo
        else:
            from backports.zoneinfo import ZoneInfo
        return ZoneInfo

    except ImportError as e:
        raise CogniteImportError(
            "ZoneInfo is part of the standard library starting with Python >=3.9. In earlier versions "
            "you need to install a backport. This is done automatically for you when installing with the pandas "
            "group: 'cognite-sdk[pandas]', or with poetry: 'poetry install -E pandas'"
        ) from e


def _import_zoneinfo_not_found_error() -> type[ZoneInfoNotFoundError]:
    if sys.version_info >= (3, 9):
        from zoneinfo import ZoneInfoNotFoundError
    else:
        from backports.zoneinfo import ZoneInfoNotFoundError
    return ZoneInfoNotFoundError


def get_utc_zoneinfo() -> ZoneInfo:
    return import_zoneinfo()("UTC")


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


def time_string_to_ms(pattern: str, string: str, unit_in_ms: dict[str, int]) -> int | None:
    pattern = pattern.format("|".join(unit_in_ms))
    if res := re.fullmatch(pattern, string):
        magnitude = int(res[1])
        unit = res[2]
        return magnitude * unit_in_ms[unit]
    return None


def granularity_to_ms(granularity: str) -> int:
    ms = time_string_to_ms(r"(\d+)({})", granularity, UNIT_IN_MS_WITHOUT_WEEK)
    if ms is None:
        raise ValueError(
            f"Invalid granularity format: `{granularity}`. Must be on format <integer>(s|m|h|d). "
            "E.g. '5m', '3h' or '1d'."
        )
    return ms


def granularity_unit_to_ms(granularity: str) -> int:
    granularity = re.sub(r"^\d+", "1", granularity)
    return granularity_to_ms(granularity)


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
    "start_time",
    "end_time",
    "last_updated_time",
    "created_time",
    "timestamp",
    "scheduled_execution_time",
    "source_created_time",
    "source_modified_time",
}


def _convert_time_attributes_in_dict(item: dict) -> dict:
    new_item = {}
    for k, v in item.items():
        if k in TIME_ATTRIBUTES:
            try:
                v = str(ms_to_datetime(v).replace(tzinfo=None))
            except ValueError:
                pass
        new_item[k] = v
    return new_item


@overload
def convert_time_attributes_to_datetime(item: dict) -> dict:
    ...


@overload
def convert_time_attributes_to_datetime(item: list[dict]) -> list[dict]:
    ...


def convert_time_attributes_to_datetime(item: dict | list[dict]) -> dict | list[dict]:
    if isinstance(item, dict):
        return _convert_time_attributes_in_dict(item)
    if isinstance(item, list):
        return list(map(_convert_time_attributes_in_dict, item))
    raise TypeError("item must be dict or list of dicts")


def align_start_and_end_for_granularity(start: int, end: int, granularity: str) -> tuple[int, int]:
    # Note the API always aligns `start` with 1s, 1m, 1h or 1d (even when given e.g. 73h)
    if remainder := start % granularity_unit_to_ms(granularity):
        # Floor `start` when not exactly at boundary
        start -= remainder
    gms = granularity_to_ms(granularity)
    if remainder := (end - start) % gms:
        # Ceil `end` when not exactly at boundary decided by `start + N * granularity`
        end += gms - remainder
    return start, end


class DateTimeAligner(ABC):
    @staticmethod
    def normalize(date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    @abstractmethod
    def ceil(cls, date: datetime) -> datetime:
        ...

    @classmethod
    @abstractmethod
    def floor(cls, date: datetime) -> datetime:
        ...

    @classmethod
    @abstractmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        ...

    @classmethod
    @abstractmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        ...


class DayAligner(DateTimeAligner):
    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return cls.normalize(date)

    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        return cls.normalize(date) + timedelta(days=1)

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return math.floor((end - start) / timedelta(days=1))

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date + timedelta(days=units)


class WeekAligner(DateTimeAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        """
        Ceils the date to the next monday
        >>> WeekAligner.ceil(datetime(2023, 4, 9 ))
        datetime.datetime(2023, 4, 10, 0, 0)
        """
        date = cls.normalize(date)
        if (weekday := date.weekday()) != 0:
            return date + timedelta(days=7 - weekday)
        return date

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        """
        Floors the date to the nearest monday
        >>> WeekAligner.floor(datetime(2023, 4, 9))
        datetime.datetime(2023, 4, 3, 0, 0)
        """
        date = cls.normalize(date)
        if (weekday := date.weekday()) != 0:
            return date - timedelta(days=weekday)
        return date

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return math.floor((end - start) / timedelta(days=7))

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date + timedelta(days=units * 7)


class MonthAligner(DateTimeAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        """
        Ceils the date to the first of the next month.
        >>> from datetime import datetime
        >>> MonthAligner.ceil(datetime(2023, 11, 1))
        datetime.datetime(2023, 11, 1, 0, 0)
        >>> MonthAligner.ceil(datetime(2023, 10, 15))
        datetime.datetime(2023, 11, 1, 0, 0)
        >>> MonthAligner.ceil(datetime(2023, 12, 15))
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> MonthAligner.ceil(datetime(2024, 1, 10))
        datetime.datetime(2024, 2, 1, 0, 0)
        """
        if date == datetime(year=date.year, month=date.month, day=1, tzinfo=date.tzinfo):
            return date
        extra, month = divmod(date.month + 1, 12)
        return cls.normalize(date.replace(year=date.year + extra, month=month, day=1))

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return cls.normalize(date.replace(day=1))

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return (end.year - start.year) * 12 + end.month - start.month

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        extra_years, month = divmod(date.month + units, 12)
        return date.replace(year=date.year + extra_years, month=month)


class QuarterAligner(DateTimeAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        """
        Ceils the months to the start of the next quarter
        >>> from datetime import datetime
        >>> QuarterAligner.ceil(datetime(2023, 2, 1))
        datetime.datetime(2023, 4, 1, 0, 0)
        >>> QuarterAligner.ceil(datetime(2023, 11, 15))
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> QuarterAligner.ceil(datetime(2023, 1, 1))
        datetime.datetime(2023, 1, 1, 0, 0)
        """
        if any(date == datetime(date.year, month, 1, tzinfo=date.tzinfo) for month in [1, 4, 7, 10]):
            return date
        month = 3 * ((date.month - 1) // 3 + 1) + 1
        add_years, month = divmod(month, 12)
        return cls.normalize(date.replace(year=date.year + add_years, month=month, day=1))

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        """
        Floors the months to start of quarter.
        >>> QuarterAligner.floor(datetime(2023, 3, 1))
        datetime.datetime(2023, 1, 1, 0, 0)
        >>> QuarterAligner.floor(datetime(2023, 8, 1))
        datetime.datetime(2023, 7, 1, 0, 0)
        >>> QuarterAligner.floor(datetime(2023, 12, 1))
        datetime.datetime(2023, 10, 1, 0, 0)
        """
        month = 3 * ((date.month - 1) // 3) + 1
        return cls.normalize(date.replace(month=month, day=1))

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return (end.year - start.year) * 4 + (end.month - start.month) // 3

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        extra_years, month = divmod(date.month + 3 * units, 12)
        return date.replace(year=date.year + extra_years, month=month)


class YearAligner(DateTimeAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        if date == datetime(date.year, 1, 1, tzinfo=date.tzinfo):
            return date
        return cls.normalize(date.replace(year=date.year + 1, month=1, day=1))

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return cls.normalize(date.replace(month=1, day=1))

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return end.year - start.year

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date.replace(year=date.year + units)


def align_large_granularity(start: datetime, end: datetime, granularity: str) -> tuple[datetime, datetime]:
    """
    Aligns the granularity by flooring the start wrt to the granularity unit, and ceiling the end.
    This is done to get consistent behavior with the Cognite Datapoints API.

    Args:
        start (datetime): Start time
        end (datetime): End time
        granularity (str): The large granularity, day|week|month|quarter|year.

    Returns:
        tuple[datetime, datetime]: start and end aligned with granularity
    """
    multiplier, unit = get_granularity_multiplier_and_unit(granularity)
    # Can be replaced by a single dispatch pattern, but kept more explicit for readability.
    try:
        aligner = {
            "d": DayAligner,
            "w": WeekAligner,
            "month": MonthAligner,
            "quarter": QuarterAligner,
            "year": YearAligner,
        }[unit]
    except KeyError as e:
        raise ValueError(f"Unit {unit} is not supported.") from e
    start = aligner.floor(start)
    end = aligner.ceil(end)
    unit_count = aligner.units_between(start, end)
    if remainder := unit_count % multiplier:
        end = aligner.add_units(end, multiplier - remainder)
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


def get_granularity_multiplier_and_unit(granularity: str, standardize: bool = True) -> tuple[int, str]:
    if not granularity[0].isdigit():
        granularity = f"1{granularity}"
    _, number, unit = re.split(r"(\d+)", granularity)
    if standardize:
        unit = standardize_unit(unit)
    return int(number), unit


def standardize_unit(unit: str) -> str:
    unit = unit.lower()
    # First three use one letter to be consistent with CDF API.
    if unit in {"seconds", "second", "s"}:
        return "s"
    elif unit in {"minutes", "minute", "m", "min", "t"}:
        return "m"
    elif unit in {"hours", "hour", "h"}:
        return "h"
    elif unit in {"day", "days", "d"}:
        return "d"
    elif unit in {"weeks", "w", "week"}:
        return "w"
    elif unit in {"months", "month"}:
        return "month"
    elif unit in {"quarters", "quarter", "q"}:
        return "quarter"
    elif unit in {"year", "years", "y"}:
        return "year"
    raise ValueError(f"Not supported unit {unit}")


def to_fixed_utc_intervals(start: datetime, end: datetime, granularity: str) -> list[dict[str, datetime | str]]:
    multiplier, unit = get_granularity_multiplier_and_unit(granularity, standardize=True)
    if unit in {"h", "m", "s"}:
        # UTC is always fixed for these intervals
        return [{"start": start, "end": end, "granularity": f"{multiplier}{unit}"}]
    start, end = align_large_granularity(start, end, granularity)
    if unit in VARIABLE_LENGTH_UNITS:
        return _to_fixed_utc_intervals_variable_unit_length(start, end, multiplier, unit)
    else:  # unit in {"day", "week"}:
        return _to_fixed_utc_intervals_fixed_unit_length(start, end, multiplier, unit)


def _to_fixed_utc_intervals_variable_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str
) -> list[dict[str, datetime | str]]:
    freq = to_pandas_freq(f"{multiplier}{unit}", start)
    index = pandas_date_range_tz(start, end, freq)
    utc = get_utc_zoneinfo()
    return [
        {
            "start": start.to_pydatetime().astimezone(utc),
            "end": end.to_pydatetime().astimezone(utc),
            "granularity": f"{(end-start)/timedelta(hours=1):.0f}h",
        }
        for start, end in zip(index[:-1], index[1:])
    ]


def _to_fixed_utc_intervals_fixed_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str
) -> list[dict[str, datetime | str]]:
    pd = cast(Any, local_import("pandas"))
    utc = get_utc_zoneinfo()

    freq = multiplier * GRANULARITY_IN_HOURS[unit]
    index = pandas_date_range_tz(start, end, to_pandas_freq(f"{multiplier}{unit}", start))
    utc_offsets = pd.Series([t.utcoffset() for t in index], index=index)
    transition_raw = index[(utc_offsets != utc_offsets.shift(-1)) | (utc_offsets != utc_offsets.shift(1))]

    hour, zero = pd.Timedelta(hours=1), pd.Timedelta(0)
    transitions = []
    for t_start, t_end in zip(transition_raw[:-1], transition_raw[1:]):
        if t_start.dst() == t_end.dst():
            dst_adjustment = 0
        elif t_start.dst() == hour and t_end.dst() == zero:
            # Fall, going away from summer (above the equator).
            dst_adjustment = 1
        elif t_start.dst() == zero and t_end.dst() == hour:
            # Spring, going to summer (above the equator).
            dst_adjustment = -1
        else:
            raise ValueError(f"Invalid dst, {t_start} and {t_end}")

        transitions.append(
            {
                "start": t_start.to_pydatetime().astimezone(utc),
                "end": t_end.to_pydatetime().astimezone(utc),
                "granularity": f"{freq+dst_adjustment}h",
            }
        )
    return transitions


def pandas_date_range_tz(start: datetime, end: datetime, freq: str, inclusive: str = "both") -> pandas.DatetimeIndex:
    """
    Pandas date_range struggles with time zone aware datetimes.
    This function overcomes that limitation.

    Assumes that start and end have the same timezone.
    """
    pd = cast(Any, local_import("pandas"))
    # There is a bug in date_range which makes it fail to handle ambiguous timestamps when you use time zone aware
    # datetimes. This is a workaround by passing the time zone as an argument to the function.
    # In addition, pandas struggle with ZoneInfo objects, so we convert them to string so that pandas can use its own
    # tzdata implementation.

    # An ambiguous timestamp is for example 1916-10-01 00:00:00 Europe/Oslo, as this corresponds to two different points in time,
    # 1916-10-01 00:00:00+02:00 and 1916-10-01 00:00:00+01:00; before and after the DST transition.
    # (Back in 1916 they did not consider the needs of software engineers in 2023 :P).
    # Setting ambiguous=True will make pandas ignore the ambiguity and use the DST timestamp. This is what we want;
    # for a user requesting monthly aggregates, we don't want to miss the first hour of the month.
    return pd.date_range(
        start.replace(tzinfo=None),
        end.replace(tzinfo=None),
        tz=str(start.tzinfo),
        freq=freq,
        inclusive=inclusive,
        nonexistent="shift_forward",
        ambiguous=True,
    )


def _timezones_are_equal(start_tz: tzinfo, end_tz: tzinfo) -> bool:
    """There are unfortunately several ways to pass/represent the same timezone (without it being a user error).
    For example pandas uses 'pytz' under the hood -except- for UTC, then it uses the built-in `datetime.timezone.utc`
    -except- when given something concrete like pytz.UTC or ZoneInfo(...).

    To make sure we don't raise something silly like 'UTC != UTC', we convert both to ZoneInfo for comparison
    via str(). This is safe as all return the lookup key (for the IANA time zone database).

    Note:
        We do not consider timezones with different keys, but equal fixed offsets from UTC to be equal. An example
        would be Zulu Time (which is +00:00 ahead of UTC) and UTC.
    """
    if start_tz is end_tz:
        return True
    ZoneInfo, ZoneInfoNotFoundError = import_zoneinfo(), _import_zoneinfo_not_found_error()
    with suppress(ValueError, ZoneInfoNotFoundError):
        # ValueError is raised for non-conforming keys (ZoneInfoNotFoundError is self-explanatory)
        if ZoneInfo(str(start_tz)) is ZoneInfo(str(end_tz)):
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

    ZoneInfo = import_zoneinfo()
    if isinstance(start_tz, ZoneInfo):
        return start_tz

    pd = cast(Any, local_import("pandas"))
    if isinstance(start, pd.Timestamp):
        return ZoneInfo(str(start_tz))

    raise ValueError("Only tz-aware pandas.Timestamp and datetime (must be using ZoneInfo) are supported.")


def to_pandas_freq(granularity: str, start: datetime) -> str:
    multiplier, unit = get_granularity_multiplier_and_unit(granularity, standardize=True)

    unit = {"s": "S", "m": "T", "h": "H", "d": "D", "w": "W-MON", "month": "MS", "quarter": "QS", "year": "AS"}.get(
        unit, unit
    )
    if unit == "QS":
        floored = QuarterAligner.floor(start)
        unit += {
            1: "-JAN",
            4: "-APR",
            7: "-JUL",
            10: "-OCT",
        }[floored.month]

    return f"{multiplier}{unit}"


def _unit_in_days(unit: str, ceil: bool = True) -> float:
    """
    Converts the unit to days.

    **Caveat** Should not be used for precise calculations, as month, quarter, and year
    do not have a precise timespan in days. Instead, the ceil argument is used to select between
    the maximum and minimum length of a year, quarter, and month.
    """
    if unit in {"w", "d", "h", "m", "s"}:
        unit = GRANULARITY_IN_TIMEDELTA_UNIT[unit]
        arg = {unit: 1}
        return timedelta(**arg) / timedelta(hours=1)

    if unit == "month":
        days = 31.0 if ceil else 28.0
    elif unit == "quarter":
        days = 92.0 if ceil else 91.0
    else:  # years
        days = 366.0 if ceil else 365.0
    return days


def in_timedelta(granularity: str, ceil: bool = True) -> timedelta:
    """
    Converts the granularity to a timedelta.

    Args:
        granularity (str): The granularity.
        ceil (bool): In the case the unit is month, quarter or year. Ceil = True will use 31, 92, 366 days for these timespans, and if ceil is false 28, 91, 365

    Returns:
        timedelta: A timespan for the granularity
    """
    multiplier, unit = get_granularity_multiplier_and_unit(granularity, standardize=True)
    if unit in {"w", "d", "h", "m", "s"}:
        unit = GRANULARITY_IN_TIMEDELTA_UNIT[unit]
        arg = {unit: multiplier}
        return timedelta(**arg)
    days = _unit_in_days(unit, ceil)
    return timedelta(days=multiplier * days)
