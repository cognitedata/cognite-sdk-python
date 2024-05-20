from __future__ import annotations

import platform
import re
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Iterable
from unittest import mock

import pytest
from _pytest.mark import ParameterSet

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    MonthAligner,
    align_large_granularity,
    align_start_and_end_for_granularity,
    convert_and_isoformat_time_attrs,
    datetime_to_gql_timestamp,
    datetime_to_ms,
    granularity_to_ms,
    import_zoneinfo,
    ms_to_datetime,
    pandas_date_range_tz,
    split_time_range,
    timestamp_to_ms,
    to_fixed_utc_intervals,
    to_pandas_freq,
    validate_timezone,
)
from tests.utils import cdf_aggregate, tmp_set_envvar

if TYPE_CHECKING:
    import pandas


class TestDatetimeToGqlTimestamp:
    def test_datetime_to_gql_timestamp_correct_type(self):
        assert datetime_to_gql_timestamp(datetime(2021, 1, 1, 0, 0, 0, 0)) == "2021-01-01T00:00:00.000"

    def test_datetime_to_gql_timestamp_incorrect_type(self):
        with pytest.raises(TypeError):
            datetime_to_gql_timestamp("2021-01-01T00:00:00.000")


class TestDatetimeToMs:
    @pytest.mark.skipif(platform.system() == "Windows", reason="Overriding timezone is too much hassle on Windows")
    @pytest.mark.parametrize(
        "local_tz, expected_ms",
        [
            ("Europe/Oslo", 1517353200000),
            ("UTC", 1517356800000),
            ("America/Los_Angeles", 1517385600000),
        ],
    )
    def test_naive_datetime_to_ms_unix(self, local_tz, expected_ms):
        with tmp_set_envvar("TZ", local_tz):
            time.tzset()
            assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=None)) == expected_ms
            assert timestamp_to_ms(datetime(2018, 1, 31, tzinfo=None)) == expected_ms

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows is typically unable to handle negative epochs.")
    def test_naive_datetime_to_ms_windows(self):
        with pytest.raises(
            ValueError,
            match="Failed to convert datetime to epoch. "
            "This likely because you are using a naive datetime. "
            "Try using a timezone aware datetime instead.",
        ):
            datetime_to_ms(datetime(1925, 8, 3))

    def test_aware_datetime_to_ms(self):
        # TODO: Starting from PY39 we should also add tests using:
        # from zoneinfo import ZoneInfo
        # datetime(2020, 10, 31, 12, tzinfo=ZoneInfo("America/Los_Angeles"))
        utc = timezone.utc
        assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=utc)) == 1517356800000
        assert datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11, tzinfo=utc)) == 1517397071000
        assert datetime_to_ms(datetime(100, 1, 31, tzinfo=utc)) == -59008867200000

    @pytest.mark.dsl
    def test_aware_datetime_to_ms_zoneinfo(self):
        ZoneInfo = import_zoneinfo()
        # The correct answer was obtained using: https://dencode.com/en/date/unix-time
        assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=ZoneInfo("Europe/Oslo"))) == 1517353200000
        assert datetime_to_ms(datetime(1900, 1, 1, tzinfo=ZoneInfo("Europe/Oslo"))) == -2208992400000
        assert datetime_to_ms(datetime(1900, 1, 1, tzinfo=ZoneInfo("America/New_York"))) == -2208970800000

    def test_ms_to_datetime__valid_input(self):  # TODO: Many tests here could benefit from parametrize
        utc = timezone.utc
        assert ms_to_datetime(1517356800000) == datetime(2018, 1, 31, tzinfo=utc)
        assert ms_to_datetime(1517397071000) == datetime(2018, 1, 31, 11, 11, 11, tzinfo=utc)
        assert ms_to_datetime(MIN_TIMESTAMP_MS) == datetime(1900, 1, 1, 0, 0, tzinfo=utc)
        assert ms_to_datetime(MAX_TIMESTAMP_MS) == datetime(2099, 12, 31, 23, 59, 59, 999000, tzinfo=utc)

    def test_ms_to_datetime__bad_input(self):
        with pytest.raises(TypeError):
            ms_to_datetime(None)
        err_msg = "Input ms={} does not satisfy: -2208988800000 <= ms <= 4102444799999"
        too_low, too_high = MIN_TIMESTAMP_MS - 1, MAX_TIMESTAMP_MS + 1
        with pytest.raises(ValueError, match=re.escape(err_msg.format(too_low))):
            ms_to_datetime(too_low)
        with pytest.raises(ValueError, match=re.escape(err_msg.format(too_high))):
            ms_to_datetime(too_high)


class TestTimestampToMs:
    @pytest.mark.parametrize("t", [None, [], {}])
    def test_invalid_type(self, t):
        with pytest.raises(TypeError, match="must be"):
            timestamp_to_ms(t)

    def test_ms(self):
        assert 1514760000000 == timestamp_to_ms(1514760000000)
        assert 1514764800000 == timestamp_to_ms(1514764800000)
        assert -1514764800000 == timestamp_to_ms(-1514764800000)

    @pytest.mark.skipif(platform.system() == "Windows", reason="Overriding timezone is too much hassle on Windows")
    def test_datetime(self):
        # Note: See also `TestDatetimeToMs.test_naive_datetime_to_ms`
        with tmp_set_envvar("TZ", "UTC"):
            time.tzset()
            assert 1514764800000 == timestamp_to_ms(datetime(2018, 1, 1))
            assert 1546300800000 == timestamp_to_ms(datetime(2019, 1, 1))
            assert MIN_TIMESTAMP_MS == timestamp_to_ms(datetime(1900, 1, 1))
            assert MAX_TIMESTAMP_MS == timestamp_to_ms(datetime(2100, 1, 1)) - 1

    def test_float(self):
        assert 1514760000000 == timestamp_to_ms(1514760000000.0)
        assert 1514764800000 == timestamp_to_ms(1514764800000.0)
        assert -1514764800000 == timestamp_to_ms(-1514764800000.0)

    @mock.patch("cognite.client.utils._time.time.time")
    @pytest.mark.parametrize(
        "time_ago_string, expected_timestamp",
        [
            ("now", 10**12),
            ("1s-ago", 10**12 - 1 * 1000),
            ("13s-ago", 10**12 - 13 * 1000),
            ("1m-ago", 10**12 - 1 * 60 * 1000),
            ("13m-ago", 10**12 - 13 * 60 * 1000),
            ("1h-ago", 10**12 - 1 * 60 * 60 * 1000),
            ("13h-ago", 10**12 - 13 * 60 * 60 * 1000),
            ("1d-ago", 10**12 - 1 * 24 * 60 * 60 * 1000),
            ("13d-ago", 10**12 - 13 * 24 * 60 * 60 * 1000),
            ("1w-ago", 10**12 - 1 * 7 * 24 * 60 * 60 * 1000),
            ("13w-ago", 10**12 - 13 * 7 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_time_ago(self, time_mock, time_ago_string, expected_timestamp):
        time_mock.return_value = 10**9

        assert timestamp_to_ms(time_ago_string) == expected_timestamp

    @pytest.mark.parametrize("time_ago_string", ["1s", "4h", "13m-ag", "13m ago", "bla"])
    def test_invalid(self, time_ago_string):
        with pytest.raises(ValueError, match=time_ago_string):
            timestamp_to_ms(time_ago_string)

    def test_time_ago_real_time(self):
        expected_time_now = datetime.now().timestamp() * 1000
        time_now = timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) < 15

        time.sleep(0.2)

        time_now = timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) > 190

    @pytest.mark.parametrize("t", [MIN_TIMESTAMP_MS - 1, datetime(1899, 12, 31, tzinfo=timezone.utc), "100000000w-ago"])
    def test_negative(self, t):
        with pytest.raises(ValueError, match="must represent a time after 1.1.1900"):
            timestamp_to_ms(t)


class TestGranularityToMs:
    @pytest.mark.parametrize(
        "granularity, expected_ms",
        [
            ("1s", 1 * 1000),
            ("13s", 13 * 1000),
            ("1m", 1 * 60 * 1000),
            ("13m", 13 * 60 * 1000),
            ("1h", 1 * 60 * 60 * 1000),
            ("13h", 13 * 60 * 60 * 1000),
            ("1d", 1 * 24 * 60 * 60 * 1000),
            ("13d", 13 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_to_ms(self, granularity, expected_ms):
        assert granularity_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match=granularity):
            granularity_to_ms(granularity)

    @pytest.mark.parametrize(
        "granularity, expected_ms",
        [
            ("1s", 1 * 1000),
            ("13s", 1 * 1000),
            ("1m", 1 * 60 * 1000),
            ("13m", 1 * 60 * 1000),
            ("1h", 1 * 60 * 60 * 1000),
            ("13h", 1 * 60 * 60 * 1000),
            ("1d", 1 * 24 * 60 * 60 * 1000),
            ("13d", 1 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_to_ms_as_unit(self, granularity, expected_ms):
        assert granularity_to_ms(granularity, as_unit=True) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_as_unit_invalid(self, granularity):
        with pytest.raises(ValueError, match=rf"Invalid granularity format: `{granularity}`"):
            granularity_to_ms(granularity, as_unit=True)


class TestObjectTimeConversion:
    @pytest.mark.parametrize(
        "item, expected_output",
        [
            ({"created_time": 0}, {"created_time": "1970-01-01 00:00:00.000+00:00"}),
            ({"last_updated_time": 0}, {"last_updated_time": "1970-01-01 00:00:00.000+00:00"}),
            ({"start_time": 0}, {"start_time": "1970-01-01 00:00:00.000+00:00"}),
            ({"end_time": 0}, {"end_time": "1970-01-01 00:00:00.000+00:00"}),
            ({"not_a_time": 0}, {"not_a_time": 0}),
            ({"expirationTime": -41103211}, {"expirationTime": "1969-12-31 12:34:56.789+00:00"}),
            ({"lastSuccess": -1}, {"lastSuccess": "1969-12-31 23:59:59.999+00:00"}),
            ({"scheduledExecutionTime": 1}, {"scheduledExecutionTime": "1970-01-01 00:00:00.001+00:00"}),
            ({"uploaded_time": 123456789}, {"uploaded_time": "1970-01-02 10:17:36.789+00:00"}),
            ([{"created_time": 0}], [{"created_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"last_updated_time": 0}], [{"last_updated_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"start_time": 0}], [{"start_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"end_time": 0}], [{"end_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"source_created_time": 0}], [{"source_created_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"source_modified_time": 0}], [{"source_modified_time": "1970-01-01 00:00:00.000+00:00"}]),
            ([{"not_a_time": 0}], [{"not_a_time": 0}]),
            ([{"created_time": int(1e15)}], [{"created_time": int(1e15)}]),
        ],
    )
    def test_convert_and_isoformat_time_attrs(self, item, expected_output):
        assert expected_output == convert_and_isoformat_time_attrs(item)


class TestSplitTimeDomain:
    def test_split_time_range__zero_splits(self):
        with pytest.raises(ValueError, match="Cannot split"):
            split_time_range(-100, 100, 0, 1)

    def test_split_time_range__too_large_delta_ms(self):
        with pytest.raises(ValueError, match="is larger than the interval itself"):
            split_time_range(-100, 100, 1, 201)

    @pytest.mark.parametrize(
        "n_splits, expected",
        [
            (1, [-100, 100]),
            (2, [-100, 0, 100]),
            (3, [-100, -33, 34, 100]),
            (4, [-100, -50, 0, 50, 100]),
        ],
    )
    def test_split_time_range__raw_granularity(self, n_splits, expected):
        assert expected == split_time_range(-100, 100, n_splits, 1)

    @pytest.mark.parametrize(
        "n_splits, granularity, expected",
        [
            (24, "5s", 39600000),
            (12, "4m", 79200000),
            (11, "3h", 86400000),
            (1, "1d", 950400000),
        ],
    )
    def test_split_time_range__agg_granularity(self, n_splits, granularity, expected):
        one_day_ms, gran_ms = 86_400_000, granularity_to_ms(granularity)
        res = split_time_range(-2 * one_day_ms, 9 * one_day_ms, n_splits, gran_ms)
        assert n_splits == len(res) - 1
        (single_diff,) = {next - prev for next, prev in zip(res[1:], res[:-1])}
        assert expected == single_diff
        assert all(val % gran_ms == 0 for val in res)


class TestAlignToGranularity:
    @pytest.mark.parametrize("granularity", ["2s", "3m", "4h", "5d"])
    def test_exactly_on_granularity_boundary(self, granularity):
        gran_ms = granularity_to_ms(granularity)
        start, end = gran_ms, 2 * gran_ms
        assert start, end == align_start_and_end_for_granularity(start, end, granularity)

    @pytest.mark.parametrize(
        "granularity, expected",
        [
            ("2s", (1000, 5000)),
            ("3m", (120000, 480000)),
            ("4h", (10800000, 39600000)),
            ("5d", (345600000, 1209600000)),
        ],
    )
    def test_start_not_on_granularity_boundary(self, granularity, expected):
        gran_ms = granularity_to_ms(granularity)
        start, end = gran_ms - 1, 2 * gran_ms
        assert expected == align_start_and_end_for_granularity(start, end, granularity)

    @staticmethod
    @pytest.mark.parametrize(
        "start, end, granularity, expected_start, expected_end",
        [
            (datetime(2023, 3, 4, 12), datetime(2023, 3, 9), "2days", datetime(2023, 3, 4), datetime(2023, 3, 10)),
            (datetime(2023, 4, 9), datetime(2023, 4, 12), "2weeks", datetime(2023, 4, 3), datetime(2023, 4, 17)),
            (datetime(2023, 1, 10), datetime(2023, 1, 11), "2months", datetime(2023, 1, 1), datetime(2023, 3, 1)),
            (datetime(2023, 2, 10), datetime(2023, 6, 15), "2quarters", datetime(2023, 1, 1), datetime(2023, 7, 1)),
            (datetime(2023, 1, 8), datetime(2023, 1, 8, second=1), "1year", datetime(2023, 1, 1), datetime(2024, 1, 1)),
            (datetime(2022, 12, 10), datetime(2023, 1, 11), "3months", datetime(2022, 12, 1), datetime(2023, 3, 1)),
            (datetime(2022, 12, 10), datetime(2023, 1, 11), "25months", datetime(2022, 12, 1), datetime(2025, 1, 1)),
            (datetime(2023, 2, 10), datetime(2025, 8, 10), "10quarters", datetime(2023, 1, 1), datetime(2028, 1, 1)),
            (datetime(2023, 4, 9), datetime(2023, 4, 12), "1week", datetime(2023, 4, 3), datetime(2023, 4, 17)),
            (datetime(2023, 1, 10), datetime(2023, 2, 11), "1month", datetime(2023, 1, 1), datetime(2023, 3, 1)),
        ],
    )
    def test_align_with_granularity(
        start: datetime, end: datetime, granularity: str, expected_start: datetime, expected_end: datetime
    ):
        actual_start, actual_end = align_large_granularity(start, end, granularity)

        assert actual_start == expected_start, "Start is not aligning"
        assert actual_end == expected_end, "End is not aligning"


def cdf_aggregate_test_data():
    try:
        import pandas as pd
    except ModuleNotFoundError:
        return []
    start, end = "2023-03-20", "2023-04-09 23:59:59"

    yield pytest.param(
        start,
        end,
        "1h",
        "7d",
        # Nullable int to match the output of retrieve_dataframe
        pd.DataFrame(index=pd.date_range(start, end, freq="7d"), data=[168, 168, 168], dtype="Int64"),
        id="1week aggregation",
    )


class TestCDFAggregation:
    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize("start, end, raw_freq, granularity, expected_aggregate", list(cdf_aggregate_test_data()))
    def test_cdf_aggregation(
        start: str, end: str, raw_freq: str, granularity: str, expected_aggregate: pandas.DataFrame
    ):
        import pandas as pd

        index = pd.date_range(start, end, freq=raw_freq)
        raw_df = pd.DataFrame(data=range(len(index)), index=index)

        actual_aggregate = cdf_aggregate(raw_df=raw_df, aggregate="count", granularity=granularity, is_step=False)

        pd.testing.assert_frame_equal(actual_aggregate, expected_aggregate)


def to_fixed_utc_intervals_data() -> Iterable[ParameterSet]:
    try:
        ZoneInfo = import_zoneinfo()
    except CogniteImportError:
        return []

    oslo = ZoneInfo("Europe/Oslo")
    utc = dict(tzinfo=ZoneInfo("UTC"))
    oslo_dst = datetime(2023, 3, 25, 23, **utc)
    oslo_std = datetime(2023, 10, 28, 22, **utc)

    yield pytest.param(
        datetime(2023, 1, 1, tzinfo=oslo),
        datetime(2023, 12, 31, 23, 59, 59, tzinfo=oslo),
        "1day",
        [
            {"start": datetime(2022, 12, 31, 23, **utc), "end": oslo_dst, "granularity": "24h"},
            {"start": oslo_dst, "end": oslo_dst + timedelta(hours=23), "granularity": "23h"},
            {"start": oslo_dst + timedelta(hours=23), "end": oslo_std, "granularity": "24h"},
            {"start": oslo_std, "end": oslo_std + timedelta(hours=25), "granularity": "25h"},
            {"start": oslo_std + timedelta(hours=25), "end": datetime(2023, 12, 31, 23, **utc), "granularity": "24h"},
        ],
        id="Year 2023 at daily granularity",
    )

    yield pytest.param(
        datetime(2023, 3, 10, tzinfo=oslo),
        datetime(2023, 4, 9, tzinfo=oslo),
        "2weeks",
        [
            {"start": datetime(2023, 3, 5, 23, **utc), "end": datetime(2023, 3, 19, 23, **utc), "granularity": "336h"},
            # Passing DST
            {"start": datetime(2023, 3, 19, 23, **utc), "end": datetime(2023, 4, 2, 22, **utc), "granularity": "335h"},
            {"start": datetime(2023, 4, 2, 22, **utc), "end": datetime(2023, 4, 16, 22, **utc), "granularity": "336h"},
        ],
        id="Passed DST with 2 weeks granularity",
    )

    yield pytest.param(
        datetime(2023, 1, 10, tzinfo=oslo),
        datetime(2023, 12, 12, tzinfo=oslo),
        "1quarter",
        [
            {
                "start": datetime(2022, 12, 31, 23, **utc),
                "end": datetime(2023, 3, 31, 22, **utc),
                "granularity": "2159h",
            },
            {
                "start": datetime(2023, 3, 31, 22, **utc),
                "end": datetime(2023, 6, 30, 22, **utc),
                "granularity": "2184h",
            },
            {
                "start": datetime(2023, 6, 30, 22, **utc),
                "end": datetime(2023, 9, 30, 22, **utc),
                "granularity": "2208h",
            },
            {
                "start": datetime(2023, 9, 30, 22, **utc),
                "end": datetime(2023, 12, 31, 23, **utc),
                "granularity": "2209h",
            },
        ],
        id="2023 in quarters",
    )

    yield pytest.param(
        datetime(2022, 10, 13, tzinfo=oslo),
        datetime(2023, 4, 9, tzinfo=oslo),
        "2months",
        [
            {
                "start": datetime(2022, 9, 30, 22, **utc),
                "end": datetime(2022, 11, 30, 23, **utc),
                "granularity": "1465h",
            },
            {
                "start": datetime(2022, 11, 30, 23, **utc),
                "end": datetime(2023, 1, 31, 23, **utc),
                "granularity": "1488h",
            },
            {
                "start": datetime(2023, 1, 31, 23, **utc),
                "end": datetime(2023, 3, 31, 22, **utc),
                "granularity": "1415h",
            },
            {
                "start": datetime(2023, 3, 31, 22, **utc),
                "end": datetime(2023, 5, 31, 22, **utc),
                "granularity": "1464h",
            },
        ],
        id="October 2022 to April 2023 in 2 month steps",
    )

    yield pytest.param(
        datetime(2020, 1, 19, tzinfo=oslo),
        datetime(2024, 1, 1, tzinfo=oslo),
        "2years",
        [
            {
                "start": datetime(2019, 12, 31, 23, **utc),
                "end": datetime(2021, 12, 31, 23, **utc),
                "granularity": "17544h",
            },
            {
                "start": datetime(2021, 12, 31, 23, **utc),
                "end": datetime(2023, 12, 31, 23, **utc),
                "granularity": "17520h",
            },
        ],
        id="From 2020 to 2024 in 2 year steps",
    )


class TestToFixedUTCIntervals:
    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "start, end, granularity, expected_intervals",
        list(to_fixed_utc_intervals_data()),
    )
    def test_to_fixed_utc_intervals(
        start: datetime, end: datetime, granularity: str, expected_intervals: list[dict[str, int | str]]
    ):
        actual_intervals = to_fixed_utc_intervals(start, end, granularity)

        assert actual_intervals == expected_intervals


def validate_time_zone_invalid_arguments_data() -> list[ParameterSet]:
    try:
        ZoneInfo = import_zoneinfo()
    except CogniteImportError:
        return []

    oslo = ZoneInfo("Europe/Oslo")
    new_york = ZoneInfo("America/New_York")

    return [
        pytest.param(
            datetime(2023, 1, 1, tzinfo=oslo),
            datetime(2023, 1, 10, tzinfo=new_york),
            "'start' and 'end' represent different timezones: 'Europe/Oslo' and 'America/New_York'.",
            id="Different timezones",
        ),
        pytest.param(
            datetime(2023, 1, 1),
            datetime(2023, 1, 10, tzinfo=new_york),
            "All times must be timezone aware, start does not have a timezone",
            id="Missing start timezone",
        ),
        pytest.param(
            datetime(2023, 1, 1),
            datetime(2023, 1, 10),
            "All times must be timezone aware, start and end do not have timezones",
            id="Missing start and end timezone",
        ),
        pytest.param(
            datetime(2023, 1, 1, tzinfo=oslo),
            datetime(2023, 1, 10),
            "All times must be timezone aware, end does not have a timezone",
            id="Missing end timezone",
        ),
    ]


def validate_time_zone_valid_arguments_data() -> list[ParameterSet]:
    try:
        ZoneInfo = import_zoneinfo()
        import pandas as pd
        import pytz  # hard pandas dependency
    except (ImportError, CogniteImportError):
        return []

    utc = ZoneInfo("UTC")
    oslo = ZoneInfo("Europe/Oslo")
    new_york = ZoneInfo("America/New_York")
    return [
        pytest.param(
            datetime(2023, 1, 1, tzinfo=oslo),
            datetime(2023, 1, 10, tzinfo=oslo),
            oslo,
            id="Oslo Timezone",
        ),
        pytest.param(
            datetime(2023, 1, 1, tzinfo=new_york),
            datetime(2023, 1, 10, tzinfo=new_york),
            new_york,
            id="New York Timezone",
        ),
        pytest.param(
            pd.Timestamp(2020, 1, 1, tzinfo=oslo),
            pd.Timestamp("2023", tz="Europe/Oslo"),
            oslo,
            id="Oslo Timezone via pandas: zoneinfo + parse string",
        ),
        pytest.param(
            pd.Timestamp("2020", tzinfo=pytz.timezone("Europe/Oslo")),
            pd.Timestamp("2023", tz="Europe/Oslo"),
            oslo,
            id="Oslo Timezone via pandas: pytz + parse string",
        ),
        pytest.param(
            pd.Timestamp(2020, 1, 1, tzinfo=oslo),
            pd.Timestamp("2023", tzinfo=pytz.timezone("Europe/Oslo")),
            oslo,
            id="Oslo Timezone via pandas: zoneinfo + pytz",
        ),
        pytest.param(
            pd.Timestamp(2020, 1, 1, tzinfo=timezone.utc),
            pd.Timestamp("2023", tz="UTC"),
            utc,
            id="UTC via pandas: built-in timezone.utc + string parse",
        ),
        pytest.param(
            pd.Timestamp(2020, 1, 1, tzinfo=ZoneInfo("UTC")),
            pd.Timestamp("2023", tz="utc"),
            utc,
            id="UTC via pandas: zoneinfo + string parse",
        ),
        pytest.param(
            pd.Timestamp(2020, 1, 1, tzinfo=timezone.utc),
            pd.Timestamp("2023", tz=pytz.UTC),
            utc,
            id="UTC via pandas: built-in timezone.utc + pytz",
        ),
    ]


class TestValidateTimeZone:
    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize("start, end, expected_message", validate_time_zone_invalid_arguments_data())
    def test_raise_value_error_invalid_arguments(start: datetime, end: datetime, expected_message: str):
        with pytest.raises(ValueError, match=expected_message):
            _ = validate_timezone(start, end)

    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize("start, end, expected_tz", validate_time_zone_valid_arguments_data())
    def test_infer_timezone(start: datetime, end: datetime, expected_tz):
        actual_tz = validate_timezone(start, end)

        ZoneInfo = import_zoneinfo()
        assert isinstance(actual_tz, ZoneInfo)
        assert actual_tz is expected_tz


class TestToPandasFreq:
    @staticmethod
    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "granularity, start, expected_first_step",
        [
            ("week", "2023-01-02", "2023-01-09"),
            ("quarter", "2023-01-01", "2023-04-01"),
            ("quarter", "2022-10-01", "2023-01-01"),
            ("quarter", "2022-07-01", "2022-10-01"),
            ("quarter", "2022-04-01", "2022-07-01"),
            ("year", "2023-01-01", "2024-01-01"),
            ("d", "2023-01-01", "2023-01-02"),
            ("2years", "2023-01-01", "2025-01-01"),
            ("3quarters", "2023-01-01", "2023-10-01"),
            ("m", "2023-01-01", "2023-01-01 00:01:00"),
            ("s", "2023-01-01", "2023-01-01 00:00:01"),
            ("2d", "2023-01-01", "2023-01-03"),
            ("2weeks", "2023-01-02", "2023-01-16"),
        ],
    )
    def test_to_pandas_freq(granularity: str, start: str, expected_first_step: str):
        import pandas as pd

        start = pd.Timestamp(start)
        expected_index = pd.DatetimeIndex([start, expected_first_step])

        freq = to_pandas_freq(granularity, start.to_pydatetime())

        actual_index = pd.date_range(start, periods=2, freq=freq)
        pd.testing.assert_index_equal(actual_index, expected_index)


class TestPandasDateRangeTz:
    @staticmethod
    @pytest.mark.dsl
    def test_pandas_date_range_tz_ambiguous_time_error():
        ZoneInfo = import_zoneinfo()
        oslo = ZoneInfo("Europe/Oslo")
        start = datetime(1916, 8, 1, tzinfo=oslo)
        end = datetime(1916, 12, 1, tzinfo=oslo)
        expected_length = 5
        freq = to_pandas_freq("1month", start)

        index = pandas_date_range_tz(start, end, freq)

        assert len(index) == expected_length


class TestDateTimeAligner:
    # TODO: DayAligner
    # TODO: WeekAligner
    # TODO: MonthAligner
    # TODO: QuarterAligner
    # TODO: YearAligner

    @pytest.mark.parametrize(
        "dt, expected",
        (
            (datetime(2023, 11, 1), datetime(2023, 11, 1)),
            (datetime(2023, 10, 15), datetime(2023, 11, 1)),
            (datetime(2023, 12, 15), datetime(2024, 1, 1)),
            (datetime(2024, 1, 10), datetime(2024, 2, 1)),
            # Bug prior to 7.5.7 would cause this to raise:
            (datetime(2023, 11, 2), datetime(2023, 12, 1)),
        ),
    )
    def test_month_aligner__ceil(self, dt, expected):
        assert expected == MonthAligner.ceil(dt)

    def test_month_aligner_ceil__invalid_date(self):
        with pytest.raises(ValueError, match="^day is out of range for month$"):
            MonthAligner.add_units(datetime(2023, 7, 31), 2)  # sept has 30 days

    @pytest.mark.parametrize(
        "dt, n_units, expected",
        (
            (datetime(2023, 7, 2), 12, datetime(2024, 7, 2)),
            (datetime(2023, 7, 2), 12 * 12, datetime(2035, 7, 2)),
            (datetime(2023, 7, 2), -12 * 2, datetime(2021, 7, 2)),
            # Bug prior to 7.5.7 would cause these to raise:
            (datetime(2023, 11, 15), 1, datetime(2023, 12, 15)),
            (datetime(2023, 12, 15), 0, datetime(2023, 12, 15)),
            (datetime(2024, 1, 15), -1, datetime(2023, 12, 15)),
        ),
    )
    def test_month_aligner__add_unites(self, dt, n_units, expected):
        assert expected == MonthAligner.add_units(dt, n_units)

    def test_month_aligner_add_unites__invalid_date(self):
        with pytest.raises(ValueError, match="^day is out of range for month$"):
            MonthAligner.add_units(datetime(2023, 1, 29), 1)  # 2023 = non-leap year
