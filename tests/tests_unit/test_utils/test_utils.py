import json
from datetime import datetime, timedelta
from decimal import Decimal
from time import sleep
from unittest import mock

import pytest

from cognite.client._utils import utils
from cognite.client.exceptions import CogniteImportError


class TestDatetimeToMs:
    def test_datetime_to_ms(self):
        from datetime import datetime

        assert utils.datetime_to_ms(datetime(2018, 1, 31)) == 1517356800000
        assert utils.datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11)) == 1517397071000
        assert utils.datetime_to_ms(datetime(100, 1, 31)) == -59008867200000
        with pytest.raises(TypeError):
            utils.datetime_to_ms(None)

    def test_ms_to_datetime(self):
        from datetime import datetime

        assert utils.ms_to_datetime(1517356800000) == datetime(2018, 1, 31)
        assert utils.ms_to_datetime(1517397071000) == datetime(2018, 1, 31, 11, 11, 11)
        assert utils.ms_to_datetime(-59008867200000) == datetime(100, 1, 31)
        with pytest.raises(TypeError):
            utils.ms_to_datetime(None)


class TestTimestampToMs:
    @pytest.mark.parametrize("t", [None, [], {}])
    def test_invalid_type(self, t):
        with pytest.raises(TypeError, match="must be"):
            utils.timestamp_to_ms(t)

    def test_ms(self):
        assert 1514760000000 == utils.timestamp_to_ms(1514760000000)
        assert 1514764800000 == utils.timestamp_to_ms(1514764800000)

    def test_datetime(self):
        assert 1514764800000 == utils.timestamp_to_ms(datetime(2018, 1, 1))
        assert 1546300800000 == utils.timestamp_to_ms(datetime(2019, 1, 1))

    def test_float(self):
        assert 1514760000000 == utils.timestamp_to_ms(1514760000000.0)
        assert 1514764800000 == utils.timestamp_to_ms(1514764800000.0)

    @mock.patch("cognite.client._utils.utils.time.time")
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

        assert utils.timestamp_to_ms(time_ago_string) == expected_timestamp

    @pytest.mark.parametrize("time_ago_string", ["1s", "4h", "13m-ag", "13m ago", "bla"])
    def test_invalid(self, time_ago_string):
        with pytest.raises(ValueError, match=time_ago_string):
            utils.timestamp_to_ms(time_ago_string)

    def test_time_ago_real_time(self):
        expected_time_now = datetime.now().timestamp() * 1000
        time_now = utils.timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) < 10

        sleep(0.2)

        time_now = utils.timestamp_to_ms("now")
        assert abs(expected_time_now - time_now) > 190

    @pytest.mark.parametrize("t", [-1, datetime(1969, 12, 31), "100000000w-ago"])
    def test_negative(self, t):
        with pytest.raises(ValueError, match="negative"):
            utils.timestamp_to_ms(t)


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
        assert utils.granularity_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match=granularity):
            utils.granularity_to_ms(granularity)


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
        assert utils.granularity_unit_to_ms(granularity) == expected_ms

    @pytest.mark.parametrize("granularity", ["2w", "-3h", "13m-ago", "13", "bla"])
    def test_to_ms_invalid(self, granularity):
        with pytest.raises(ValueError, match="format"):
            utils.granularity_unit_to_ms(granularity)


class TestCaseConversion:
    def test_to_camel_case(self):
        assert "camelCase" == utils.to_camel_case("camel_case")
        assert "camelCase" == utils.to_camel_case("camelCase")
        assert "a" == utils.to_camel_case("a")

    def test_to_snake_case(self):
        assert "snake_case" == utils.to_snake_case("snakeCase")
        assert "snake_case" == utils.to_snake_case("snake_case")
        assert "a" == utils.to_snake_case("a")


class TestLocalImport:
    @pytest.mark.dsl
    def test_local_import_single_ok(self):
        import pandas

        assert pandas == utils.local_import("pandas")

    @pytest.mark.dsl
    def test_local_import_multiple_ok(self):
        import pandas, numpy

        assert (pandas, numpy) == utils.local_import("pandas", "numpy")

    def test_local_import_single_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            utils.local_import("not-a-module")

    @pytest.mark.dsl
    def test_local_import_multiple_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            utils.local_import("pandas", "not-a-module")


class TestJsonDumpDefault:
    def test_json_serializable_Decimal(self):
        with pytest.raises(TypeError):
            json.dumps(Decimal(1))

        assert json.dumps(Decimal(1), default=utils.json_dump_default)

    @pytest.mark.dsl
    def test_json_serialiable_numpy_integer(self):
        import numpy as np

        inputs = [np.int32(1), np.int64(1)]
        for input in inputs:
            assert json.dumps(input, default=utils.json_dump_default)


class TestAssertions:
    @pytest.mark.parametrize("timestamp", [utils.timestamp_to_ms(datetime(2018, 1, 1)), datetime(1970, 2, 2), "now"])
    def test_assert_timestamp_not_in_jan_in_1970_ok(self, timestamp):
        utils.assert_timestamp_not_in_jan_in_1970(timestamp)

    @pytest.mark.parametrize("timestamp", [utils.timestamp_to_ms(datetime(2018, 1, 1)) / 1000, datetime(1970, 2, 1)])
    def test_assert_timestamp_not_in_jan_in_1970_fail(self, timestamp):
        with pytest.raises(AssertionError, match="You are attempting to post data in January 1970"):
            utils.assert_timestamp_not_in_jan_in_1970(timestamp)

    @pytest.mark.parametrize("var, var_name, types", [(1, "var1", [int]), ("1", "var2", [int, str])])
    def test_assert_type_ok(self, var, var_name, types):
        utils.assert_type(var, var_name, types=types)

    @pytest.mark.parametrize("var, var_name, types", [("1", "var", [int, float]), ((1,), "var2", [dict, list])])
    def test_assert_type_fail(self, var, var_name, types):
        with pytest.raises(TypeError, match=str(types)):
            utils.assert_type(var, var_name, types)

    def test_assert_exactly_one_of_id_and_external_id(self):
        with pytest.raises(AssertionError):
            utils.assert_exactly_one_of_id_or_external_id(1, "1")
        utils.assert_exactly_one_of_id_or_external_id(1, None)
        utils.assert_exactly_one_of_id_or_external_id(None, "1")
