from __future__ import annotations

import math
import numbers
import re
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast, overload

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._auxiliary import local_import

if TYPE_CHECKING:
    import pandas

    if sys.version_info >= (3, 9):
        from zoneinfo import ZoneInfo
    else:
        from backports.zoneinfo import ZoneInfo


UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
VARIABLE_LENGTH_UNITS = {"month", "quarter", "year"}
GRANULARITY_IN_HOURS = {"w": 168, "d": 24, "h": 1}
GRANULARITY_IN_TIMEDELTA_UNIT = {"w": "weeks", "d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


def import_zoneinfo() -> ZoneInfo:
    try:
        if sys.version_info >= (3, 9):
            from zoneinfo import ZoneInfo
        else:
            from backports.zoneinfo import ZoneInfo
        return ZoneInfo  # type: ignore [return-value]

    except ImportError as e:
        raise CogniteImportError(
            "ZoneInfo is part of the standard library starting with Python >=3.9. In earlier versions "
            "you need to install a backport. This is done automatically for you when installing with the pandas "
            "group: 'cognite-sdk[pandas]', or with poetry: 'poetry install -E pandas'"
        ) from e


def get_utc_zoneinfo() -> ZoneInfo:
    return import_zoneinfo()("UTC")  # type: ignore [operator]


def datetime_to_ms(dt: datetime) -> int:
    """Converts datetime object to milliseconds since epoch.

    Args:
        dt (datetime): Naive or aware datetime object. Naive datetimes are interpreted as local time.

    Returns:
        ms: Milliseconds since epoch (negative for time prior to 1970-01-01)
    """
    return int(1000 * dt.timestamp())


def ms_to_datetime(ms: Union[int, float]) -> datetime:
    """Converts valid Cognite timestamps, i.e. milliseconds since epoch, to datetime object.

    Args:
        ms (Union[int, float]): Milliseconds since epoch.

    Raises:
        ValueError: On invalid Cognite timestamps.

    Returns:
        datetime: Aware datetime object in UTC.
    """
    if not (MIN_TIMESTAMP_MS <= ms <= MAX_TIMESTAMP_MS):
        raise ValueError(f"Input {ms=} does not satisfy: {MIN_TIMESTAMP_MS} <= ms <= {MAX_TIMESTAMP_MS}")

    # Note: We don't use fromtimestamp because it typically fails for negative values on Windows
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)


def time_string_to_ms(pattern: str, string: str, unit_in_ms: Dict[str, int]) -> Optional[int]:
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


def _convert_time_attributes_in_dict(item: Dict) -> Dict:
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
def convert_time_attributes_to_datetime(item: Dict) -> Dict:
    ...


@overload
def convert_time_attributes_to_datetime(item: List[Dict]) -> List[Dict]:
    ...


def convert_time_attributes_to_datetime(item: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
    if isinstance(item, dict):
        return _convert_time_attributes_in_dict(item)
    if isinstance(item, list):
        return list(map(_convert_time_attributes_in_dict, item))
    raise TypeError("item must be dict or list of dicts")


def align_start_and_end_for_granularity(start: int, end: int, granularity: str) -> Tuple[int, int]:
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
    _zeros_upto_hour = dict(hour=0, minute=0, second=0, microsecond=0)

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
        return date.replace(**cls._zeros_upto_hour)  # type: ignore [arg-type]

    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        return date.replace(**cls._zeros_upto_hour) + timedelta(days=1)  # type: ignore [arg-type]

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
        date = date.replace(**cls._zeros_upto_hour)  # type: ignore [arg-type]
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
        date = date.replace(**cls._zeros_upto_hour)  # type: ignore [arg-type]
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
        return date.replace(year=date.year + extra, month=month, day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return date.replace(day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

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
        return date.replace(year=date.year + add_years, month=month, day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

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
        return date.replace(month=month, day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

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
        return date.replace(year=date.year + 1, month=1, day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return date.replace(month=1, day=1, **cls._zeros_upto_hour)  # type: ignore [arg-type]

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
        start: Start time
        end: End time
        granularity: The large granularity, day|week|month|quarter|year.

    Returns:
        start and end aligned with granularity
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


def split_time_range(start: int, end: int, n_splits: int, granularity_in_ms: int) -> List[int]:
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
    index = pandas_date_range_tz(start, end, f"{freq}H")
    expected_freq = pd.Timedelta(hours=freq)
    next_mask = (index - index.to_series().shift(1)) != expected_freq
    last_mask = (index.to_series().shift(-1) - index) != expected_freq
    index = index[last_mask | next_mask]

    hour, zero = pd.Timedelta(hours=1), pd.Timedelta(0)
    transitions = []
    for start, end in zip(index[:-1], index[1:]):
        if start.dst() == end.dst():
            dst_adjustment = 0
        elif start.dst() == hour and end.dst() == zero:
            # Fall, going away from summer.
            dst_adjustment = 1
        elif start.dst() == zero and end.dst() == hour:
            # Spring, going to summer.
            dst_adjustment = -1
        else:
            raise ValueError(f"Invalid dst, {start} and {end}")

        transitions.append(
            {
                "start": start.to_pydatetime().astimezone(utc),  # type: ignore
                "end": end.to_pydatetime().astimezone(utc),  # type: ignore
                "granularity": f"{freq+dst_adjustment}h",
            }
        )
    return transitions


def pandas_date_range_tz(start: datetime, end: datetime, freq: str, inclusive: str = "both") -> pandas.DatetimeIndex:
    """
    Pandas date_range struggles with time zone aware datetimes.
    This function overcomes that limitation.
    """
    pd = local_import("pandas")
    # Pandas seems to have issues with ZoneInfo object, so removing the timezone and adding it back.
    return pd.date_range(start.replace(tzinfo=None), end.replace(tzinfo=None), freq=freq, inclusive=inclusive).tz_localize(start.tzinfo.key)  # type: ignore [union-attr]


def validate_timezone(start: datetime, end: datetime) -> ZoneInfo:
    ZoneInfo = import_zoneinfo()

    if missing := [name for name, timestamp in zip(("start", "end"), (start, end)) if not timestamp.tzinfo]:
        names = " and ".join(missing)
        end_sentence = " do not have timezones." if len(missing) >= 2 else " does not have a timezone."
        raise ValueError(f"All times must be time zone aware, {names}{end_sentence}")

    if not isinstance(start.tzinfo, ZoneInfo) or not isinstance(end.tzinfo, ZoneInfo):  # type: ignore [arg-type]
        raise ValueError("Only ZoneInfo implementation of tzinfo is supported")

    if start.tzinfo is not end.tzinfo:
        raise ValueError(f"start and end have different timezones, {start.tzinfo.key!r} and {end.tzinfo.key!r}.")  # type: ignore [union-attr]

    return start.tzinfo  # type: ignore [return-value]


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
        granularity: The granularity.
        ceil: In the case the unit is month, quarter or year. Ceil = True will use 31, 92, 366 days for these
              timespans, and if ceil is false 28, 91, 365

    Returns:
        A timespan for the granularity
    """
    multiplier, unit = get_granularity_multiplier_and_unit(granularity, standardize=True)
    if unit in {"w", "d", "h", "m", "s"}:
        unit = GRANULARITY_IN_TIMEDELTA_UNIT[unit]
        arg = {unit: multiplier}
        return timedelta(**arg)
    days = _unit_in_days(unit, ceil)
    return timedelta(days=multiplier * days)
