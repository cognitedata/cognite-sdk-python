from datetime import datetime
from time import sleep
from unittest import mock

import pytest

from cognite.client import utils


class TestDatetimeToMs:
    def test_datetime_to_ms(self):
        from datetime import datetime

        assert utils._time.datetime_to_ms(datetime(2018, 1, 31)) == 1517356800000
        assert utils._time.datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11)) == 1517397071000
        assert utils._time.datetime_to_ms(datetime(100, 1, 31)) == -59008867200000
        with pytest.raises(TypeError):
            utils._time.datetime_to_ms(None)

    def test_ms_to_datetime(self):
        from datetime import datetime

        assert utils._time.ms_to_datetime(1517356800000) == datetime(2018, 1, 31)
        assert utils._time.ms_to_datetime(1517397071000) == datetime(2018, 1, 31, 11, 11, 11)
        with pytest.raises(ValueError, match="greater than"):
            utils._time.ms_to_datetime(-59008867200000)
        with pytest.raises(TypeError):
            utils._time.ms_to_datetime(None)


class TestTimestampToMs:
    @pytest.mark.parametrize("t", [None, [], {}])
    def test_invalid_type(self, t):
        with pytest.raises(TypeError, match="must be"):
            utils._time.timestamp_to_ms(t)

    def test_ms(self):
        assert 1514760000000 == utils._time.timestamp_to_ms(1514760000000)
        assert 1514764800000 == utils._time.timestamp_to_ms(1514764800000)

    def test_datetime(self):
        assert 1514764800000 == utils._time.timestamp_to_ms(datetime(2018, 1, 1))
        assert 1546300800000 == utils._time.timestamp_to_ms(datetime(2019, 1, 1))

    def test_float(self):
        assert 1514760000000 == utils._time.timestamp_to_ms(1514760000000.0)
        assert 1514764800000 == utils._time.timestamp_to_ms(1514764800000.0)

    @mock.patch("cognite.client.utils._time.time.time")
    @pytest.mark.parametrize(
        "time_ago_string, expected_timestamp",
        [
            ("now", 10 ** 12),
            ("1s-ago", 10 ** 12 - 1 * 1000),
            ("13s-ago", 10 ** 12 - 13 * 1000),
            ("1m-ago", 10 ** 12 - 1 * 60 * 1000),
            ("13m-ago", 10 ** 12 - 13 * 60 * 1000),
            ("1h-ago", 10 ** 12 - 1 * 60 * 60 * 1000),
            ("13h-ago", 10 ** 12 - 13 * 60 * 60 * 1000),
            ("1d-ago", 10 ** 12 - 1 * 24 * 60 * 60 * 1000),
            ("13d-ago", 10 ** 12 - 13 * 24 * 60 * 60 * 1000),
            ("1w-ago", 10 ** 12 - 1 * 7 * 24 * 60 * 60 * 1000),
            ("13w-ago", 10 ** 12 - 13 * 7 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_time_ago(self, time_mock, time_ago_string, expected_timestamp):
        time_mock.return_value = 10 ** 9

        assert utils._time.timestamp_to_ms(time_ago_string) == expected_timestamp

    @pytest.mark.parametrize("time_ago_string", ["1s", "4h", "13m-ag", "13m ago", "bla"])
    def test_invalid(self, time_ago_string):
        with pytest.raises(ValueError, match=time_ago_string):
            utils._time.timestamp_to_ms(time_ago_string)

    def test_time_ago_real_time(self):
        expected_time_now = datetime.now().timestamp() * 1000
        time_now = utils._time.timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) < 10

        sleep(0.2)

        time_now = utils._time.timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) > 190

    @pytest.mark.parametrize("t", [-1, datetime(1969, 12, 31), "100000000w-ago"])
    def test_negative(self, t):
        with pytest.raises(ValueError, match="negative"):
            utils._time.timestamp_to_ms(t)


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
        assert utils._time.granularity_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match=granularity):
            utils._time.granularity_to_ms(granularity)


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
        assert utils._time.granularity_unit_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match="format"):
            utils._time.granularity_unit_to_ms(granularity)


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
        assert expected_output == utils._time.convert_time_attributes_to_datetime(item)
