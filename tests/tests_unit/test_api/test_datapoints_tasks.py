from __future__ import annotations

import math
import random
import re
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from cognite.client._api.datapoint_tasks import _DpsQueryValidator, _FullDatapointsQuery
from cognite.client.utils._text import random_string
from tests.utils import random_aggregates, random_cognite_ids, random_granularity

LIMIT_KWS = dict(dps_limit_raw=1234, dps_limit_agg=5678)


@pytest.fixture
def query_validator():
    return _DpsQueryValidator(**LIMIT_KWS)


class TestSingleTSQueryValidator:
    @pytest.mark.parametrize("ids", (None, []))
    @pytest.mark.parametrize("xids", (None, []))
    @pytest.mark.parametrize("inst_id", (None, []))
    def test_no_identifiers_raises(self, ids, xids, inst_id):
        with pytest.raises(
            ValueError, match=re.escape("Pass at least one time series `id`, `external_id` or `instance_id`!")
        ):
            _FullDatapointsQuery(id=ids, external_id=xids, instance_id=inst_id).parse_into_queries()

    @pytest.mark.parametrize(
        "ids, xids, exp_attr_to_fail",
        (
            ({123}, None, "id"),
            (None, {"foo"}, "external_id"),
            ({123}, {"foo"}, "id"),
        ),
    )
    def test_wrong_identifier_type_raises(self, ids, xids, exp_attr_to_fail):
        err_msg = f"Got unsupported type {type(ids or xids)}, as, or part of argument `{exp_attr_to_fail}`."

        with pytest.raises(TypeError, match=re.escape(err_msg)):
            _FullDatapointsQuery(id=ids, external_id=xids).parse_into_queries()

    @pytest.mark.parametrize(
        "ids, xids, exp_attr_to_fail",
        (
            ({"iid": 123}, None, "id"),
            (None, {"extern-id": "foo"}, "external_id"),
            ([{"iid": 123}], None, "id"),
            (None, [{"extern-id": "foo"}], "external_id"),
            ({"iid": 123}, {"extern-id": "foo"}, "id"),
        ),
    )
    def test_missing_identifier_in_dict_raises(self, ids, xids, exp_attr_to_fail):
        err_msg = f"Missing required key `{exp_attr_to_fail}` in dict:"

        with pytest.raises(KeyError, match=re.escape(err_msg)):
            _FullDatapointsQuery(id=ids, external_id=xids).parse_into_queries()

    @pytest.mark.parametrize(
        "ids, xids, exp_wrong_type, exp_attr_to_fail",
        (
            ({"id": "123"}, None, str, "id"),
            ({"id": 42 + 0j}, None, complex, "id"),
            ({"id": Decimal("123")}, None, Decimal, "id"),
            (None, {"external_id": 123}, int, "external_id"),
            (None, {"externalId": ["foo"]}, list, "external_id"),
            ([{"id": "123"}], None, str, "id"),
            (None, [{"external_id": 123}], int, "external_id"),
            (None, [{"externalId": b"foo"}], bytes, "external_id"),
        ),
    )
    def test_identifier_in_dict_has_wrong_type(self, ids, xids, exp_wrong_type, exp_attr_to_fail):
        exp_type = "int" if exp_attr_to_fail == "id" else "str"
        err_msg = f"Invalid {exp_attr_to_fail}, expected {exp_type}, got {exp_wrong_type}"

        with pytest.raises(TypeError, match=re.escape(err_msg)):
            _FullDatapointsQuery(id=ids, external_id=xids).parse_into_queries()

    @pytest.mark.parametrize("identifier_dct", ({"id": 123}, {"external_id": "foo"}, {"externalId": "bar"}))
    def test_identifier_dicts_has_wrong_keys(self, identifier_dct):
        good_keys = random.choices(
            ["start", "end", "aggregates", "granularity", "include_outside_points", "limit"],
            k=random.randint(0, 6),
        )
        bad_keys = [random_string(20) for _ in range(random.randint(1, 3))]
        identifier_dct.update(dict.fromkeys(good_keys + bad_keys))
        if "id" in identifier_dct:
            identifier = "id"
            query = _FullDatapointsQuery(id=identifier_dct, external_id=None)
        else:
            identifier = "external_id"
            query = _FullDatapointsQuery(id=None, external_id=identifier_dct)

        match = re.escape(f"Dict provided by argument `{identifier}` included key(s) not understood")
        with pytest.raises(KeyError, match=match):
            query.parse_into_queries()

    @pytest.mark.parametrize("limit, exp_limit", [(0, 0), (1, 1), (-1, None), (math.inf, None), (None, None)])
    def test_valid_limits(self, limit, exp_limit, query_validator):
        query = _FullDatapointsQuery(id=1, limit=limit)
        ts_queries = query.parse_into_queries()
        query_validator(ts_queries)
        assert len(ts_queries) == 1
        assert ts_queries[0].limit == exp_limit

    @pytest.mark.parametrize("limit", (-2, -math.inf, math.nan, ..., "5000"))
    def test_limits_not_allowed_values(self, limit, query_validator):
        with pytest.raises(TypeError, match=re.escape("Parameter `limit` must be a non-negative integer -OR-")):
            query = _FullDatapointsQuery(id=1, limit=limit)
            query_validator(query.parse_into_queries())

    @pytest.mark.parametrize(
        "granularity, aggregates, outside, exp_err, exp_err_msg_idx",
        (
            (4000, ["min"], None, TypeError, 0),
            ("4h", {"min"}, None, TypeError, 1),
            ("4h", None, None, ValueError, 2),
            ("4h", [], None, ValueError, 3),
            (None, ["min"], None, ValueError, 4),
            ("4h", ["min"], True, ValueError, 5),
        ),
    )
    def test_function_validate_and_create_query(
        self, granularity, aggregates, outside, exp_err, exp_err_msg_idx, query_validator
    ):
        err_msgs = [
            f"Expected `granularity` to be of type `str` or None, not {type(granularity)}",
            f"Expected `aggregates` to be of type `str`, `list[str]` or None, not {type(aggregates)}",
            "When passing `granularity`, argument `aggregates` is also required.",
            "Empty list of `aggregates` passed, expected at least one!",
            "When passing `aggregates`, argument `granularity` is also required.",
            "'Include outside points' is not supported for aggregates.",
        ]
        queries = _FullDatapointsQuery(
            id=1, granularity=granularity, aggregates=aggregates, include_outside_points=outside
        ).parse_into_queries()
        with pytest.raises(exp_err, match=re.escape(err_msgs[exp_err_msg_idx])):
            query_validator(queries)

    @pytest.mark.parametrize(
        "start, end",
        (
            (None, None),
            (None, 123),
            (123, None),
            (-123, "now"),
            (-123, -12),
            (1, datetime.now()),
            (1, datetime.now(timezone.utc)),
        ),
    )
    def test_function__verify_time_range__valid_inputs(self, start, end, query_validator):
        gran_dct = {"granularity": random_granularity(), "aggregates": random_aggregates()}
        for kwargs in [{}, gran_dct]:
            ts_query = _FullDatapointsQuery(id=1, start=start, end=end, **kwargs).parse_into_queries()
            query_validator(ts_query)
            assert isinstance(ts_query[0].start, int)
            assert isinstance(ts_query[0].end, int)

    @pytest.mark.parametrize(
        "start, end",
        (
            (0, -1),
            (50, 50),
            (-50, -50),
            (None, 0),
            ("now", -123),
            ("now", 123),
            (datetime.now(), 123),
            (datetime.now(timezone.utc), 123),
        ),
    )
    def test_function__verify_time_range__raises(self, start, end, query_validator):
        gran_dct = {"granularity": random_granularity(), "aggregates": random_aggregates()}
        for kwargs in [{}, gran_dct]:
            full_query = _FullDatapointsQuery(id=1, start=start, end=end, **kwargs)
            all_queries = full_query.parse_into_queries()
            with pytest.raises(ValueError, match="Invalid time range"):
                query_validator(all_queries)

    def test_retrieve_aggregates__include_outside_points_raises(self, query_validator):
        id_dct_lst = [
            {"id": ts_id, "granularity": random_granularity(), "aggregates": random_aggregates()}
            for ts_id in random_cognite_ids(10)
        ]
        # Only one time series is configured wrong and will raise:
        id_dct_lst[-1]["include_outside_points"] = True

        full_query = _FullDatapointsQuery(id=id_dct_lst, include_outside_points=False)
        all_queries = full_query.parse_into_queries()
        with pytest.raises(ValueError, match="'Include outside points' is not supported for aggregates."):
            query_validator(all_queries)
