from __future__ import annotations

import numbers
import re
import time
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


def cdf_aggregate(
    raw_df: pandas.DataFrame,
    aggregate: Literal["average", "sum"],
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


def dst_transition_dates(tz: zoneinfo.ZoneInfo, year: int) -> None | tuple[datetime, datetime]:
    """
    Convenience function for getting datetime saving time (DST) transition dates for a given timezone and year.
    It returns None if the timezone does not have a DST for the given year.

    Apparently this is not included in any python library
    https://stackoverflow.com/questions/59938388/convenience-function-to-get-daylight-saving-time-dates-for-a-certain-year

    This is implemented with a binary search algorithm.

    """
    try:
        import zoneinfo  # type:ignore
    except ImportError:
        from backports import zoneinfo  # type:ignore

    utc = zoneinfo.ZoneInfo("UTC")

    start = datetime(year, 3, 31, tzinfo=utc)
    spring = _binary_search_dst_transition(
        datetime(year, 1, 1, tzinfo=utc), datetime(year, 6, 30, tzinfo=utc), start, tz
    )
    if spring is None:
        return None
    start = datetime(year, 10, 1, tzinfo=utc)
    fall = _binary_search_dst_transition(
        datetime(year, 7, 1, tzinfo=utc), datetime(year, 12, 31, tzinfo=utc), start, tz
    )
    return None if fall is None else (spring, fall)


def _binary_search_dst_transition(
    low_utc: datetime, high_utc: datetime, today_utc: datetime, tz: zoneinfo.ZoneInfo
) -> datetime | None:
    """
    Binary search algorithm to find transition from std to dst or dst to std.

    Note all comparison is done in local time, while arithmetic is done in UTC to avoid mistakes due to DST.
    """
    low_tz = low_utc.astimezone(tz)
    high_tz = high_utc.astimezone(tz)
    if high_tz.dst() == low_tz.dst():
        # No daylight savings
        return None

    today_tz = today_utc.astimezone(tz)
    tomorrow_tz = (today_utc + timedelta(days=1)).astimezone(tz)
    if today_tz.dst() != tomorrow_tz.dst():
        # Today will switch to or away from DST.
        return today_utc

    if today_tz.dst() == high_tz.dst():
        # New high limit -> transition is between low and today.
        new_today = low_utc + (today_utc - low_utc) / 2
        return _binary_search_dst_transition(low_utc, today_utc, new_today.replace(hour=0, minute=0), tz)

    assert today_tz.dst() == low_tz.dst()
    # New low limit -> transition is between today and high.
    new_today = today_utc + (high_utc - today_utc) / 2
    return _binary_search_dst_transition(today_utc, high_utc, new_today.replace(hour=0, minute=0), tz)


def to_fixed_utc_intervals(start: datetime, end: datetime, granularity: str) -> list[dict[str, datetime | str]]:
    if granularity != "1day":
        raise NotImplementedError("Currently granularity is hard coded to one day")
    try:
        from zoneinfo import ZoneInfo  # type:ignore
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type:ignore

    start_ms, end_ms = align_start_and_end_for_granularity(datetime_to_ms(start), datetime_to_ms(end), "1h")
    utc = ZoneInfo("UTC")
    start_utc, end_utc = ms_to_datetime(start_ms).astimezone(utc), ms_to_datetime(end_ms).astimezone(utc)
    tz = start.tzinfo
    hour = timedelta(hours=1)
    last_end = start_utc
    out: list[dict[str, datetime | str]] = []
    for year in range(start.year, end.year + 1):
        transition = dst_transition_dates(start.tzinfo, year)
        if transition is None:
            # No daylight savings this year
            continue
        spring, fall = transition
        spring = spring.replace(tzinfo=tz).astimezone(utc)
        fall = fall.replace(tzinfo=tz).astimezone(utc)
        out.extend(
            [
                {"start": last_end, "end": spring, "granularity": "24h"},
                {"start": spring, "end": spring + hour, "granularity": "23h"},
                {"start": spring + timedelta(hours=23), "end": fall, "granularity": "24h"},
                {"start": fall, "end": fall + hour, "granularity": "25h"},
            ]
        )
        last_end = fall + timedelta(hours=25)
    out.append(
        {"start": last_end, "end": end_utc, "granularity": "24h"},
    )

    return out
