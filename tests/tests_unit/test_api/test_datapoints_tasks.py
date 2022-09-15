import math

import pytest

from cognite.client._api.datapoint_tasks import _SingleTSQueryValidator
from cognite.client.data_classes import DatapointsQuery


class Test_SingleTSQueryValidator:
    @pytest.mark.parametrize("limit, exp_limit", [(-1, None), (0, 0), (1, 1), (math.inf, None), (None, None)])
    def test_query_valid_limits(self, limit, exp_limit):
        ts_query = _SingleTSQueryValidator(DatapointsQuery(id=0, limit=limit)).validate_and_create_single_queries()
        assert len(ts_query) == 1
        assert ts_query[0].limit == exp_limit

    # TODO: WIP
