from __future__ import annotations

import calendar
import functools
import math
import numbers
import re
import time
from abc import ABC, abstractmethod
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from typing import TYPE_CHECKING, cast, overload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cognite.client.utils._importing import local_import
from cognite.client.utils._text import to_camel_case

if TYPE_CHECKING:
    from datetime import tzinfo

    import pandas

UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
VARIABLE_LENGTH_UNITS = {"month", "quarter", "year"}
GRANULARITY_IN_HOURS = {"w": 168, "d": 24, "h": 1}
GRANULARITY_IN_TIMEDELTA_UNIT = {"w": "weeks", "d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
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


@functools.lru_cache(1)
def get_zoneinfo_utc() -> ZoneInfo:
    return ZoneInfo("UTC")


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
        return ZoneInfo(tz)
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


def convert_data_modelling_timestamp(timestamp: str) -> datetime:
    """Converts a timestamp string to a datetime object.

    Args:
        timestamp (str): A timestamp string.

    Returns:
        datetime: A datetime object.
    """
    try:
        return datetime.fromisoformat(timestamp)
    except ValueError:
        # Typically hits if the timestamp has truncated milliseconds,
        # For example, "2021-01-01T00:00:00.17+00:00".
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")


def split_granularity_into_quantity_and_normalized_unit(granularity: str) -> tuple[int, str]:
    """A normalized unit is any unit accepted by the API"""
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


class DateTimeAligner(ABC):
    @staticmethod
    def normalize(date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    @abstractmethod
    def ceil(cls, date: datetime) -> datetime: ...

    @classmethod
    @abstractmethod
    def floor(cls, date: datetime) -> datetime: ...

    @classmethod
    @abstractmethod
    def units_between(cls, start: datetime, end: datetime) -> int: ...

    @classmethod
    @abstractmethod
    def add_units(cls, date: datetime, units: int) -> datetime: ...


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
        extra, month = divmod(date.month, 12)  # this works because month is one-indexed
        return cls.normalize(date.replace(year=date.year + extra, month=month + 1, day=1))

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return cls.normalize(date.replace(day=1))

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return (end.year - start.year) * 12 + end.month - start.month

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        """
        Adds 'units' number of months to 'date', ignoring timezone. The resulting date might not be valid,
        for example Jan 29 + 1 unit in a non-leap year. In such cases, datetime will raise a ValueError.
        """
        extra_years, month = divmod(date.month + units - 1, 12)
        return date.replace(year=date.year + extra_years, month=month + 1)


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
        """
        Adds 'units' number of quarters to 'date', ignoring timezone. The resulting date might not be valid,
        for example Jan 31 + 1 unit, as April only has 30 days. In such cases, datetime will raise a ValueError.
        """
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
        if date.month == 2 and date.day == 29 and not calendar.isleap(date.year + units):
            # Avoid raising ValueError for invalid date
            date = date.replace(day=28)
        return date.replace(year=date.year + units)


_ALIGNER_OPTIONS: dict[str, type[DateTimeAligner]] = {
    "d": DayAligner,
    "w": WeekAligner,
    "month": MonthAligner,
    "quarter": QuarterAligner,
    "year": YearAligner,
}


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
    try:
        aligner = _ALIGNER_OPTIONS[unit]
    except KeyError:
        raise ValueError(f"Unit {unit} is not supported.") from None
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


def get_granularity_multiplier_and_unit(granularity: str) -> tuple[int, str]:
    if granularity and granularity[0].isdigit():
        _, number, unit = re.split(r"(\d+)", granularity)
    else:
        number, unit = 1, granularity
    try:
        return int(number), _GRANULARITY_UNIT_LOOKUP[unit.lower()]
    except KeyError:
        raise ValueError(f"Not supported granularity: {granularity}") from None


def _check_max_granularity_limit(num: int, given_granularity: str) -> int:
    if num > 100_000:
        raise ValueError(
            f"Granularity, {given_granularity!r}, is above the maximum limit of 100k hours equivalent (was {num})."
        )
    return num


def to_fixed_utc_intervals(start: datetime, end: datetime, granularity: str) -> list[dict[str, datetime | str]]:
    multiplier, unit = get_granularity_multiplier_and_unit(granularity)
    if unit in {"h", "m", "s"}:
        # UTC is always fixed for these intervals
        return [{"start": start, "end": end, "granularity": f"{multiplier}{unit}"}]

    start, end = align_large_granularity(start, end, granularity)
    if unit in VARIABLE_LENGTH_UNITS:
        return _to_fixed_utc_intervals_variable_unit_length(start, end, multiplier, unit, granularity)
    else:  # unit in {"day", "week"}:
        return _to_fixed_utc_intervals_fixed_unit_length(start, end, multiplier, unit, granularity)


def _to_fixed_utc_intervals_variable_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str, granularity: str
) -> list[dict[str, datetime | str]]:
    UTC = get_zoneinfo_utc()
    freq = to_pandas_freq(f"{multiplier}{unit}", start)
    index = pandas_date_range_tz(start, end, freq)
    return [
        {
            "start": start.to_pydatetime().astimezone(UTC),
            "end": end.to_pydatetime().astimezone(UTC),
            "granularity": f"{_check_max_granularity_limit((end - start) // timedelta(hours=1), granularity)}h",
        }
        for start, end in pairwise(index)
    ]


def _to_fixed_utc_intervals_fixed_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str, granularity: str
) -> list[dict[str, datetime | str]]:
    pd = local_import("pandas")
    UTC = get_zoneinfo_utc()

    index = pandas_date_range_tz(start, end, to_pandas_freq(f"{multiplier}{unit}", start))
    utc_offsets = pd.Series([t.utcoffset() for t in index], index=index)
    transition_raw = index[(utc_offsets != utc_offsets.shift(-1)) | (utc_offsets != utc_offsets.shift(1))]

    transitions = []
    freq = multiplier * GRANULARITY_IN_HOURS[unit]
    hour, zero = pd.Timedelta(hours=1), pd.Timedelta(0)
    for t_start, t_end in pairwise(transition_raw):
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
                "start": t_start.to_pydatetime().astimezone(UTC),
                "end": t_end.to_pydatetime().astimezone(UTC),
                "granularity": f"{_check_max_granularity_limit(freq + dst_adjustment, granularity)}h",
            }
        )
    return transitions


def pandas_date_range_tz(start: datetime, end: datetime, freq: str, inclusive: str = "both") -> pandas.DatetimeIndex:
    """
    Pandas date_range struggles with timezone aware datetimes.
    This function overcomes that limitation.

    Assumes that start and end have the same timezone.
    """
    pd = local_import("pandas")
    # There is a bug in date_range which makes it fail to handle ambiguous timestamps when you use timezone aware
    # datetimes. This is a workaround by passing the timezone as an argument to the function.
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
    via str(). This is safe as all return the lookup key (for the IANA timezone database).

    Note:
        We do not consider timezones with different keys, but equal fixed offsets from UTC to be equal. An example
        would be Zulu Time (which is +00:00 ahead of UTC) and UTC.
    """
    if start_tz is end_tz:
        return True
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

    if isinstance(start_tz, ZoneInfo):
        return start_tz

    pd = local_import("pandas")
    if isinstance(start, pd.Timestamp):
        return ZoneInfo(str(start_tz))

    raise ValueError("Only tz-aware pandas.Timestamp and datetime (must be using ZoneInfo) are supported.")


_STANDARD_GRANULARITY_TO_PANDAS_LOOKUP: dict[str, str] = {
    "s": "s",
    "m": "min",
    "h": "h",
    "d": "d",
    "w": "W-MON",
    "month": "MS",
    "quarter": "QS",
    "year": "YS",
}


def to_pandas_freq(granularity: str, start: datetime) -> str:
    multiplier, unit = get_granularity_multiplier_and_unit(granularity)
    unit = _STANDARD_GRANULARITY_TO_PANDAS_LOOKUP.get(unit, unit)
    if unit == "QS":
        floored = QuarterAligner.floor(start)
        unit += {1: "-JAN", 4: "-APR", 7: "-JUL", 10: "-OCT"}[floored.month]
    return f"{multiplier}{unit}"


__all__ = ["ZoneInfo", "ZoneInfoNotFoundError"]  # Fix: Module does not explicitly export attribute "ZoneInfo
