from __future__ import annotations

import numbers
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union, cast, overload

from cognite.client.utils._auxiliary import local_import

if TYPE_CHECKING:
    import pandas

    try:
        import zoneinfo  # type:ignore
    except ImportError:
        from backports import zoneinfo  # type:ignore


UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
VARIABLE_LENGTH_UNITS = {"month", "quarter", "year"}

MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


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


class DateAligner(ABC):
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


class DayAligner(DateAligner):
    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return int((end - start).total_seconds() // (24 * 3600))

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date + timedelta(days=units)


class WeekAligner(DateAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        """
        Ceils the date to the next monday
        >>> WeekAligner.ceil(datetime(2023, 4, 9 ))
        datetime.datetime(2023, 4, 10, 0, 0)
        """
        date = date.replace(hour=0, minute=0, microsecond=0)
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
        date = date.replace(hour=0, minute=0, microsecond=0)
        if (weekday := date.weekday()) != 0:
            return date - timedelta(days=weekday)
        return date

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return int((end - start).total_seconds() // (24 * 3600)) // 7

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date + timedelta(days=units * 7)


class MonthAligner(DateAligner):
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
        return date.replace(year=date.year + extra, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return (end.year - start.year) * 12 + end.month - start.month

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        extra_years, month = divmod(date.month + units, 12)
        return date.replace(year=date.year + extra_years, month=month)


class QuarterAligner(DateAligner):
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
        return date.replace(year=date.year + add_years, month=month, day=1, hour=0, minute=0, microsecond=0)

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
        return date.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return (end.year - start.year) * 4 + (end.month - start.month) // 3

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        extra_years, month = divmod(date.month + 3 * units, 12)
        return date.replace(year=date.year + extra_years, month=month)


class YearAligner(DateAligner):
    @classmethod
    def ceil(cls, date: datetime) -> datetime:
        if date == datetime(date.year, 1, 1, tzinfo=date.tzinfo):
            return date
        return date.replace(year=date.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def floor(cls, date: datetime) -> datetime:
        return date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def units_between(cls, start: datetime, end: datetime) -> int:
        return end.year - start.year

    @classmethod
    def add_units(cls, date: datetime, units: int) -> datetime:
        return date.replace(year=date.year + units)


def align_large_granularity(start: datetime, end: datetime, granularity: str) -> tuple[datetime, datetime]:
    """
    Aligns the granularity by flooring the start wrt to the granularity unit, and ceiling the end.
    This is done to get consistent behavior with how CDF is doing it at the database level.

    Args:
        start: Start date
        end: End date,
        granularity: The large granularity, day|week|month|quarter|year.

    Returns:
        start and end aligned with granularity
    """
    multiplier, unit = get_granularity_multiplier_and_unit(granularity)
    # Can be replaced by a single dispatch pattern, but kept more explicit for readability.
    try:
        aligner = {
            "day": DayAligner,
            "week": WeekAligner,
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
    _, number, unit = re.split(r"(\d+)", granularity)
    if standardize:
        unit = standardize_unit(unit)
    return int(number), unit


def standardize_unit(unit: str) -> str:
    # First three use one letter to be consistent with CDF API.
    if unit in {"seconds", "second", "s"}:
        return "s"
    elif unit in {"minutes", "minute", "m"}:
        return "m"
    elif unit in {"hours", "hour", "h"}:
        return "h"
    elif unit in {"day", "days", "d"}:
        return "day"
    elif unit in {"weeks", "w", "week"}:
        return "week"
    elif unit in {"months", "month"}:
        return "month"
    elif unit in {"quarters", "quarter", "q"}:
        return "quarter"
    elif unit in {"year", "years", "y"}:
        return "year"
    raise ValueError(f"Not supported unit {unit}")


def granularity_in_hours(multiplier_or_granularity: str | int, unit: str | None = None) -> int:
    """
    Calculates the given granularity in hours.
    >>> granularity_in_hours("1week")
    168
    >>> granularity_in_hours(1, "week")
    168
    >>> granularity_in_hours("3d")
    72
    >>> granularity_in_hours(3, "d")
    72
    >>> granularity_in_hours("2w")
    336
    >>> granularity_in_hours(2, "w")
    336
    """
    if isinstance(multiplier_or_granularity, str):
        number, unit = get_granularity_multiplier_and_unit(multiplier_or_granularity)
    else:
        number = multiplier_or_granularity
        if unit is None:
            raise ValueError("You must pass in unit when only passing in multiplier.")
        unit = standardize_unit(unit)
    unit_in_hours = {
        "week": 168,
        "day": 24,
        "h": 1,
    }
    if unit not in unit_in_hours:
        raise ValueError(f"Unit {unit} is not supported")
    return number * unit_in_hours[unit]


def cdf_aggregate(
    raw_df: pandas.DataFrame,
    aggregate: Literal["average", "sum", "count"],
    granularity: str,
    is_step: bool = False,
    raw_freq: str = None,
) -> pandas.DataFrame:
    """Aggregates the dataframe as CDF is doing it on the database layer.

    **Motivation**: This is used in testing to verify that the correct aggregation is done with
    on the client side when aggregating in given time zone.

    Current assumptions:
        * No step timeseries
        * Uniform index.
        * Known frequency of raw data.

    Args:
        raw_df (pd.DataFrame): Dataframe with the raw datapoints.
        aggregate (str): Single aggregate to calculate, supported average, sum.
        granularity (str): The granularity to aggregates at. e.g. '15s', '2h', '10d'.
        is_step (bool): Whether to use stepwise or continuous interpolation.
        raw_freq (str): The frequency of the raw data. If it is not given, it is attempted inferred from raw_df.
    """
    if is_step:
        raise NotImplementedError()

    pd = cast(Any, local_import("pandas"))
    granularity_pd = granularity.replace("m", "T")
    grouping = raw_df.groupby(pd.Grouper(freq=granularity_pd))
    if aggregate == "sum":
        return grouping.sum()
    elif aggregate == "count":
        return grouping.count().astype("Int64")

    # The average is calculated by the formula '1/(b-a) int_a^b f(t) dt' where f(t) is the continuous function
    # This is weighted average of the sampled version of f(t)
    np = cast(Any, local_import("numpy"))

    def integrate_average(values: pandas.DataFrame) -> pandas.Series:
        return pd.Series(((values.iloc[1:].values + values.iloc[:-1].values) / 2.0).mean(), index=values.columns)

    freq = raw_freq or raw_df.index.inferred_freq
    if freq is None:
        raise ValueError("Failed to infer frequency raw data.")
    if not freq[0].isdigit():
        freq = f"1{freq}"

    # When the frequency of the data is 1 hour and above, the end point is excluded.
    freq = pd.Timedelta(freq)
    if freq >= pd.Timedelta("1hour"):
        return grouping.apply(integrate_average)

    def integrate_average_end_points(values: pandas.Series) -> float:
        dt = values.index[-1] - values.index[0]
        scale = np.diff(values.index) / 2.0 / dt
        return (scale * (values.values[1:] + values.values[:-1])).sum()

    step = pd.Timedelta(granularity_pd) // freq

    return (
        raw_df.rolling(window=pd.Timedelta(granularity_pd), closed="both")
        .apply(integrate_average_end_points)
        .shift(-step)
        .iloc[::step]
    )


def to_fixed_utc_intervals(start: datetime, end: datetime, granularity: str) -> list[dict[str, datetime | str]]:
    try:
        from zoneinfo import ZoneInfo  # type:ignore
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type:ignore
    utc = ZoneInfo("UTC")
    multiplier, unit = get_granularity_multiplier_and_unit(granularity, standardize=True)
    if unit in {"h", "m", "s"}:
        # UTC is always fixed for these intervals
        return [{"start": start, "end": end, "granularity": f"{multiplier}{unit}"}]
    start, end = align_large_granularity(start, end, granularity)
    if unit in VARIABLE_LENGTH_UNITS:
        return _to_fixed_utc_intervals_variable_unit_length(start, end, multiplier, unit, utc)
    elif unit in {"day", "week"}:
        return _to_fixed_utc_intervals_fixed_unit_length(start, end, multiplier, unit, utc)
    raise ValueError(f"Not supported unit {unit}")


def _to_fixed_utc_intervals_variable_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str, utc: zoneinfo.ZoneInfo
) -> list[dict[str, datetime | str]]:
    pd = cast(Any, local_import("pandas"))

    # Pandas seems to have issues with ZoneInfo object, so removing the timezone and adding it back.
    index = pd.date_range(start.replace(tzinfo=None), end.replace(tzinfo=None), freq="1D").tz_localize(start.tzinfo.key)  # type: ignore
    # All units are always using the first of each month, filtering it out here makes index much smaller.
    index = index[index.day == 1]
    if unit == "month":
        month_no = index.month - start.month + 12 * (index.year - start.year)
        index = index[month_no % multiplier == 0].tz_convert(utc)
    elif unit == "quarter":
        quarter_no = (index.month - start.month) // 3 + 4 * (index.year - start.year)
        index = index[index.month.isin({1, 4, 7, 10}) & (quarter_no % multiplier == 0)].tz_convert(utc)
    elif unit == "year":
        year_no = index.year - start.year
        index = index[(index.month == 1) & (year_no % multiplier == 0)].tz_convert(utc)
    else:
        raise ValueError(f"Unit {unit} is not supported.")
    return [
        {
            "start": start.to_pydatetime(),
            "end": end.to_pydatetime(),
            "granularity": f"{(end-start).total_seconds()//3600:.0f}h",
        }
        for start, end in zip(index[:-1], index[1:])
    ]


def _to_fixed_utc_intervals_fixed_unit_length(
    start: datetime, end: datetime, multiplier: int, unit: str, utc: zoneinfo.ZoneInfo
) -> list[dict[str, datetime | str]]:
    pd = cast(Any, local_import("pandas"))

    freq = granularity_in_hours(multiplier, unit)
    # Pandas seems to have issues with ZoneInfo object, so removing the timezone and adding it back.
    index = pd.date_range(start.replace(tzinfo=None), end.replace(tzinfo=None), freq=f"{freq}H").tz_localize(
        start.tzinfo.key  # type: ignore
    )
    expected_freq = pd.Timedelta(f"{freq}H")
    next_diff = index - index.to_series().shift(1)
    last_diff = index - index.to_series().shift(-1)
    index = index[(last_diff.abs() != expected_freq) | (next_diff.abs() != expected_freq)]

    hour, zero = pd.Timedelta("1hour"), pd.Timedelta("0hour")
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
