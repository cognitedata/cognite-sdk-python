from datetime import datetime

import pytest

from cognite.client._utils import utils
from cognite.client.api.datapoints import Datapoint, TimeseriesWithDatapoints


class TestConversions:
    def test_datetime_to_ms(self):
        from datetime import datetime

        assert utils.datetime_to_ms(datetime(2018, 1, 31)) == 1517356800000
        assert utils.datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11)) == 1517397071000
        assert utils.datetime_to_ms(datetime(100, 1, 31)) == -59008867200000
        with pytest.raises(AttributeError):
            utils.datetime_to_ms(None)

    def test_round_to_nearest(self):
        assert utils._round_to_nearest(12, 10) == 10
        assert utils._round_to_nearest(8, 10) == 10

    def test_granularity_to_ms(self):
        assert utils.granularity_to_ms("10s") == 10000
        assert utils.granularity_to_ms("10m") == 600000

    def test_interval_to_ms(self):
        assert isinstance(utils.interval_to_ms(None, None)[0], int)
        assert isinstance(utils.interval_to_ms(None, None)[1], int)
        assert isinstance(utils.interval_to_ms("1w-ago", "1d-ago")[0], int)
        assert isinstance(utils.interval_to_ms("1w-ago", "1d-ago")[1], int)
        assert isinstance(utils.interval_to_ms(datetime(2018, 2, 1), datetime(2018, 3, 1))[0], int)
        assert isinstance(utils.interval_to_ms(datetime(2018, 2, 1), datetime(2018, 3, 1))[1], int)

    def test_time_ago_to_ms(self):
        assert utils._time_ago_to_ms("3w-ago") == 1814400000
        assert utils._time_ago_to_ms("1d-ago") == 86400000
        assert utils._time_ago_to_ms("1s-ago") == 1000
        assert utils
        assert utils._time_ago_to_ms("not_correctly_formatted") is None


class TestFirstFit:
    def test_with_timeserieswithdatapoints(self):
        timeseries_with_100_datapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(100)]
        )
        timeseries_with_200_datapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(200)]
        )
        timeseries_with_300_datapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(300)]
        )

        all_timeseries = [
            timeseries_with_100_datapoints,
            timeseries_with_200_datapoints,
            timeseries_with_300_datapoints,
        ]

        result = utils.first_fit(list_items=all_timeseries, max_size=300, get_count=lambda x: len(x.datapoints))

        assert len(result) == 2


class TestMisc:
    @pytest.mark.parametrize(
        "start, end, granularity, num_of_workers, expected_output",
        [
            (1550241236999, 1550244237001, "1d", 1, [{"start": 1550241236999, "end": 1550244237001}]),
            (0, 10000, "1s", 10, [{"start": i, "end": i + 1000} for i in range(0, 10000, 1000)]),
            (0, 2500, "1s", 3, [{"start": 0, "end": 1000}, {"start": 1000, "end": 2500}]),
        ],
    )
    def test_get_datapoints_windows(self, start, end, granularity, num_of_workers, expected_output):
        res = utils.get_datapoints_windows(start=start, end=end, granularity=granularity, num_of_workers=num_of_workers)
        assert expected_output == res

    def test_to_camel_case(self):
        assert "camelCase" == utils.to_camel_case("camel_case")
        assert "camelCase" == utils.to_camel_case("camelCase")
        assert "a" == utils.to_camel_case("a")

    def test_to_snake_case(self):
        assert "snake_case" == utils.to_snake_case("snakeCase")
        assert "snake_case" == utils.to_snake_case("snake_case")
        assert "a" == utils.to_snake_case("a")
