import platform
import re
import time
from datetime import datetime, timezone
from unittest import mock

import pytest

from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    align_start_and_end_for_granularity,
    convert_time_attributes_to_datetime,
    datetime_to_ms,
    granularity_to_ms,
    granularity_unit_to_ms,
    ms_to_datetime,
    split_time_range,
    timestamp_to_ms,
)
from tests.utils import tmp_set_envvar


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
    def test_naive_datetime_to_ms(self, local_tz, expected_ms):
        with tmp_set_envvar("TZ", local_tz):
            time.tzset()
            assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=None)) == expected_ms
            assert timestamp_to_ms(datetime(2018, 1, 31, tzinfo=None)) == expected_ms

    def test_aware_datetime_to_ms(self):
        # TODO: Starting from PY39 we should also add tests using:
        # from zoneinfo import ZoneInfo
        # datetime(2020, 10, 31, 12, tzinfo=ZoneInfo("America/Los_Angeles"))
        utc = timezone.utc
        assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=utc)) == 1517356800000
        assert datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11, tzinfo=utc)) == 1517397071000
        assert datetime_to_ms(datetime(100, 1, 31, tzinfo=utc)) == -59008867200000

    def test_ms_to_datetime(self):
        utc = timezone.utc
        assert ms_to_datetime(1517356800000) == datetime(2018, 1, 31, tzinfo=utc)
        assert ms_to_datetime(1517397071000) == datetime(2018, 1, 31, 11, 11, 11, tzinfo=utc)
        assert ms_to_datetime(MIN_TIMESTAMP_MS) == datetime(1900, 1, 1, 0, 0, tzinfo=utc)
        assert ms_to_datetime(MAX_TIMESTAMP_MS) == datetime(2050, 12, 31, 23, 59, 59, 999000, tzinfo=utc)
        with pytest.raises(
            ValueError,
            match=re.escape("Input ms=-59008867200000 does not satisfy: -2208988800000 <= ms <= 2556143999999"),
        ):
            ms_to_datetime(-59008867200000)
        with pytest.raises(TypeError):
            ms_to_datetime(None)


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
            assert MAX_TIMESTAMP_MS == timestamp_to_ms(datetime(2051, 1, 1)) - 1

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
        assert abs(expected_time_now - time_now) < 10

        time.sleep(0.2)

        time_now = timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) > 190

    @pytest.mark.parametrize("t", [MIN_TIMESTAMP_MS - 1, datetime(1899, 12, 31), "100000000w-ago"])
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


class TestGranularityUnitToMs:
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
    def test_to_ms(self, granularity, expected_ms):
        assert granularity_unit_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match="format"):
            granularity_unit_to_ms(granularity)


class TestObjectTimeConversion:
    @pytest.mark.parametrize(
        "item, expected_output",
        [
            ({"created_time": 0}, {"created_time": "1970-01-01 00:00:00"}),
            ({"last_updated_time": 0}, {"last_updated_time": "1970-01-01 00:00:00"}),
            ({"start_time": 0}, {"start_time": "1970-01-01 00:00:00"}),
            ({"end_time": 0}, {"end_time": "1970-01-01 00:00:00"}),
            ({"not_a_time": 0}, {"not_a_time": 0}),
            ([{"created_time": 0}], [{"created_time": "1970-01-01 00:00:00"}]),
            ([{"last_updated_time": 0}], [{"last_updated_time": "1970-01-01 00:00:00"}]),
            ([{"start_time": 0}], [{"start_time": "1970-01-01 00:00:00"}]),
            ([{"end_time": 0}], [{"end_time": "1970-01-01 00:00:00"}]),
            ([{"source_created_time": 0}], [{"source_created_time": "1970-01-01 00:00:00"}]),
            ([{"source_modified_time": 0}], [{"source_modified_time": "1970-01-01 00:00:00"}]),
            ([{"not_a_time": 0}], [{"not_a_time": 0}]),
            ([{"created_time": int(1e15)}], [{"created_time": int(1e15)}]),
        ],
    )
    def test_convert_time_attributes_to_datetime(self, item, expected_output):
        assert expected_output == convert_time_attributes_to_datetime(item)


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
        (single_diff,) = set(next - prev for next, prev in zip(res[1:], res[:-1]))
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
