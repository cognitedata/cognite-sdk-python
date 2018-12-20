from datetime import datetime

import pytest

import cognite.client._utils as utils
from cognite.client.v0_5.datapoints import Datapoint, TimeseriesWithDatapoints


class TestConversions:
    def test_datetime_to_ms(self):
        from datetime import datetime

        assert utils.datetime_to_ms(datetime(2018, 1, 31)) == 1517356800000
        assert utils.datetime_to_ms(datetime(2018, 1, 31, 11, 11, 11)) == 1517397071000
        assert utils.datetime_to_ms(datetime(100, 1, 31)) == -59008867200000
        with pytest.raises(AttributeError):
            utils.datetime_to_ms(None)

    def test_round_to_nearest(self):
        assert utils.round_to_nearest(12, 10) == 10
        assert utils.round_to_nearest(8, 10) == 10

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
        from typing import List

        timeseries_with_100_datapoints: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(100)]
        )
        timeseries_with_200_datapoints: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(200)]
        )
        timeseries_with_300_datapoints: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(300)]
        )

        all_timeseries: List[TimeseriesWithDatapoints] = [
            timeseries_with_100_datapoints,
            timeseries_with_200_datapoints,
            timeseries_with_300_datapoints,
        ]

        result: List[List[TimeseriesWithDatapoints]] = utils.first_fit(
            list_items=all_timeseries, max_size=300, get_count=lambda x: len(x.datapoints)
        )

        assert len(result) == 2
