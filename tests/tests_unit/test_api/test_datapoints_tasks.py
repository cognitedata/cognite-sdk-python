import math
import random
from unittest.mock import patch

import pytest

from cognite.client._api.datapoint_tasks import _SingleTSQueryValidator
from cognite.client.data_classes import DatapointsQuery
from tests.utils import random_aggregates, random_granularity, set_max_workers

DATAPOINTS_API = "cognite.client._api.datapoints.{}"


class Test_SingleTSQueryValidator:
    @pytest.mark.parametrize("limit, exp_limit", [(0, 0), (1, 1), (-1, None), (math.inf, None), (None, None)])
    def test_query_valid_limits(self, limit, exp_limit):
        ts_query = _SingleTSQueryValidator(DatapointsQuery(id=0, limit=limit)).validate_and_create_single_queries()
        assert len(ts_query) == 1
        assert ts_query[0].limit == exp_limit

    @pytest.mark.parametrize(
        "max_workers, n_ts, mock_out_eager_or_chunk",
        [
            (1, 1, "ChunkingDpsFetcher"),
            (5, 5, "ChunkingDpsFetcher"),
            (1, 2, "EagerDpsFetcher"),
            (1, 10, "EagerDpsFetcher"),
        ],
    )
    def test_retrieve_aggregates__include_outside_points_raises(
        self,
        max_workers,
        n_ts,
        mock_out_eager_or_chunk,
        cognite_client,
        retrieve_endpoints,
        weekly_dps_ts,
    ):
        num_ts, str_ts = weekly_dps_ts
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            ts_chunk = random.sample(num_ts + str_ts, k=n_ts)
            id_dct_lst = [
                {"id": ts.id, "granularity": random_granularity(), "aggregates": random_aggregates()} for ts in ts_chunk
            ]
            # Only one time series is configured wrong and will raise:
            id_dct_lst[-1]["include_outside_points"] = True
            for endpoint in retrieve_endpoints:
                with pytest.raises(ValueError, match="'Include outside points' is not supported for aggregates."):
                    endpoint(
                        granularity=None,
                        aggregates=None,
                        id=id_dct_lst,
                        include_outside_points=False,
                        limit=random.choice([10, None]),  # Using weekly data so unlimited is no prob
                    )

    # TODO: WIP
