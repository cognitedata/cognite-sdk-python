from __future__ import annotations

import platform
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    ZoneInfo,
    convert_and_isoformat_time_attrs,
    convert_data_modeling_timestamp,
    datetime_to_ms,
    datetime_to_ms_iso_timestamp,
    granularity_to_ms,
    ms_to_datetime,
    parse_str_timezone,
    parse_str_timezone_offset,
    split_time_range,
    timed_cache,
    timestamp_to_ms,
)
from tests.utils import tmp_set_envvar


@pytest.mark.parametrize(
    "offset_inp, expected",
    (
        ("", timedelta(0)),
        ("01:15", timedelta(seconds=4500)),
        ("01:15:12", timedelta(seconds=4512)),
        ("23:59", timedelta(seconds=86340)),
        ("0", timedelta(0)),
        ("01", timedelta(seconds=3600)),
        ("1", timedelta(seconds=3600)),
        ("7", timedelta(seconds=25200)),
        ("18", timedelta(seconds=64800)),
    ),
)
def test_parse_str_timezone_offset(offset_inp, expected):
    for pm in "+-":
        for prefix in ["", "UTC", "UT", "GMT"]:
            if not prefix and not offset_inp:
                continue
            inp = prefix + pm + offset_inp if offset_inp else prefix
            res = parse_str_timezone_offset(inp)
            assert res == timezone(int(pm + "1") * expected)


@pytest.mark.parametrize(
    "inp, expected",
    (
        ("Europe/Oslo", ZoneInfo("Europe/Oslo")),
        ("Asia/Tokyo", ZoneInfo("Asia/Tokyo")),
        ("GMT", ZoneInfo("GMT")),
        ("UTC-0", timezone.utc),
        ("UTC+01:15", timezone(timedelta(seconds=4500))),
    ),
)
def test_parse_str_timezone(inp, expected):
    assert expected == parse_str_timezone(inp)


class TestDatetimeToMsIsoTimestamp:
    @pytest.mark.skipif(platform.system() == "Windows", reason="Overriding timezone is too much hassle on Windows")
    def test_timezone_unaware(self):
        input_datetime = datetime(2021, 1, 1, 0, 0, 0, 0)
        with tmp_set_envvar("TZ", "CET"):
            time.tzset()
            assert datetime_to_ms_iso_timestamp(input_datetime) == "2021-01-01T00:00:00.000+01:00"

    @pytest.mark.dsl
    def test_timezone_cet(self):
        input_datetime = datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_datetime = input_datetime.astimezone(timezone.utc)
        assert datetime_to_ms_iso_timestamp(input_datetime) == "2021-01-01T00:00:00.000+01:00"
        assert datetime_to_ms_iso_timestamp(utc_datetime) == "2020-12-31T23:00:00.000+00:00"

    @pytest.mark.dsl
    @pytest.mark.skipif(platform.system() == "Windows", reason="Overriding timezone is too much hassle on Windows")
    def test_timezone_cet_in_local_tz(self):
        input_datetime = datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=ZoneInfo("CET"))
        with tmp_set_envvar("TZ", "UTC"):
            time.tzset()
            assert datetime_to_ms_iso_timestamp(input_datetime) == "2021-01-01T00:00:00.000+01:00"

    def test_incorrect_type(self):
        with pytest.raises(TypeError, match="Expected datetime object, got <class 'str'>"):
            datetime_to_ms_iso_timestamp("2021-01-01T00:00:00.000")


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
        utc = timezone.utc
        assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=utc)) == 1517356800000
        assert datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11, tzinfo=utc)) == 1517397071000
        assert datetime_to_ms(datetime(100, 1, 31, tzinfo=utc)) == -59008867200000

    def test_aware_datetime_to_ms_zoneinfo(self):
        # The correct answer was obtained using: https://dencode.com/en/date/unix-time
        assert datetime_to_ms(datetime(2018, 1, 31, tzinfo=ZoneInfo("Europe/Oslo"))) == 1517353200000
        assert datetime_to_ms(datetime(1900, 1, 1, tzinfo=ZoneInfo("Europe/Oslo"))) == -2208992400000
        assert datetime_to_ms(datetime(1900, 1, 1, tzinfo=ZoneInfo("America/New_York"))) == -2208970800000
        assert datetime_to_ms(datetime(2020, 10, 31, 12, tzinfo=ZoneInfo("America/Los_Angeles"))) == 1604170800000

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
        "time_shift_string, expected_timestamp",
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
            ("1s-ahead", 10**12 + 1 * 1000),
            ("13s-ahead", 10**12 + 13 * 1000),
            ("1m-ahead", 10**12 + 1 * 60 * 1000),
            ("13m-ahead", 10**12 + 13 * 60 * 1000),
            ("1h-ahead", 10**12 + 1 * 60 * 60 * 1000),
            ("13h-ahead", 10**12 + 13 * 60 * 60 * 1000),
            ("1d-ahead", 10**12 + 1 * 24 * 60 * 60 * 1000),
            ("13d-ahead", 10**12 + 13 * 24 * 60 * 60 * 1000),
            ("1w-ahead", 10**12 + 1 * 7 * 24 * 60 * 60 * 1000),
            ("13w-ahead", 10**12 + 13 * 7 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_time_shift(self, time_mock, time_shift_string, expected_timestamp):
        time_mock.return_value = 10**9

        assert timestamp_to_ms(time_shift_string) == expected_timestamp

    @pytest.mark.parametrize("time_shift_string", ["1s", "4h", "13m-ag", "13m-ahe", "13m ago", "13m ahead", "bla"])
    def test_invalid(self, time_shift_string):
        with pytest.raises(ValueError, match=time_shift_string):
            timestamp_to_ms(time_shift_string)

    def test_time_shift_real_time(self):
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


class TestConvertDataModelingTimestamp:
    @pytest.mark.parametrize(
        "timestamp_str, expected",
        [
            ("2021-01-01T00:00:00.000+00:00", datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
            ("2021-01-01T00:00:00.000+01:00", datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=timezone(timedelta(hours=1)))),
            (
                "2021-01-01T00:00:00.000+01:15",
                datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=timezone(timedelta(hours=1, minutes=15))),
            ),
            (
                "2021-01-01T00:00:00.000-01:15",
                datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=timezone(timedelta(hours=-1, minutes=-15))),
            ),
            ("2024-09-03T09:36:01.17+00:00", datetime(2024, 9, 3, 9, 36, 1, 170000, tzinfo=timezone.utc)),
        ],
    )
    def test_valid_timestamp_str(self, timestamp_str: str, expected: datetime) -> None:
        assert expected == convert_data_modeling_timestamp(timestamp_str)


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


class TestTimedCache:
    def test_is_thread_safe(self):
        hello = []

        @timed_cache(ttl=2)
        def the_function(i):
            hello.append(i)

        def entry(i):
            the_function(i)
            return i

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = sorted(executor.map(entry, range(10)))

        assert len(hello) == 1
        assert results == list(range(10))

        # Wait for TTL and retry
        time.sleep(2.01)
        the_function(42)
        assert len(hello) == 2 and hello[-1] == 42
