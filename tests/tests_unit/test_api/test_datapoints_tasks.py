from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any

import pytest

from cognite.client._api.datapoint_tasks import _DpsQueryValidator, _FullDatapointsQuery
from cognite.client.data_classes import DatapointsQuery
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils.useful_types import SequenceNotStr
from tests.utils import random_aggregates, random_cognite_ids, random_granularity

LIMIT_KWS = dict(dps_limit_raw=1234, dps_limit_agg=5678)


@pytest.fixture
def query_validator() -> _DpsQueryValidator:
    return _DpsQueryValidator(**LIMIT_KWS)


class TestSingleTSQueryValidator:
    @pytest.mark.parametrize("ids", (None, []))
    @pytest.mark.parametrize("xids", (None, []))
    @pytest.mark.parametrize("inst_id", (None, []))
    def test_no_identifiers_raises(
        self, ids: list[int] | None, xids: list[str] | None, inst_id: list[NodeId] | None
    ) -> None:
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
    def test_wrong_identifier_type_raises(
        self, ids: Sequence[int] | None, xids: SequenceNotStr[str] | None, exp_attr_to_fail: str
    ) -> None:
        err_msg = f"Got unsupported type {type(ids or xids)}, as, or part of argument `{exp_attr_to_fail}`."

        with pytest.raises(TypeError, match=re.escape(err_msg)):
            _FullDatapointsQuery(id=ids, external_id=xids).parse_into_queries()

    @pytest.mark.parametrize(
        "ids, xids, iids, exp_attr_to_fail, exp_wrong_type",
        (
            ({"id": 123}, None, None, "id", "int"),
            (None, {"external_id": "foo"}, None, "external_id", "str"),
            (None, None, {"instance_id": "bar"}, "instance_id", "NodeId"),
        ),
    )
    def test_passing_dict_for_identifier_raises(
        self, ids: dict | None, xids: dict | None, iids: dict | None, exp_attr_to_fail: str, exp_wrong_type: str
    ) -> None:
        err_msg = (
            f"Got unsupported type <class 'dict'>, as, or part of argument `{exp_attr_to_fail}`. Expected one "
            f"of {exp_wrong_type}, DatapointsQuery, or a (mixed) list of these"
        )

        with pytest.raises(TypeError, match=re.escape(err_msg)):
            _FullDatapointsQuery(id=ids, external_id=xids, instance_id=iids).parse_into_queries()  # type: ignore[arg-type]

    @pytest.mark.parametrize("limit, exp_limit", [(0, 0), (1, 1), (-1, None), (math.inf, None), (None, None)])
    def test_valid_limits(self, limit: int | None, exp_limit: int | None, query_validator: _DpsQueryValidator) -> None:
        query = _FullDatapointsQuery(id=1, limit=limit)
        ts_queries = query.parse_into_queries()
        query_validator(ts_queries)
        assert len(ts_queries) == 1
        assert ts_queries[0].limit == exp_limit

    @pytest.mark.parametrize("limit", (-2, -math.inf, math.nan, ..., "5000"))
    def test_limits_not_allowed_values(self, limit: Any, query_validator: _DpsQueryValidator) -> None:
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
        self,
        granularity: str | None,
        aggregates: Iterable[str] | None,
        outside: bool | None,
        exp_err: type[Exception],
        exp_err_msg_idx: int,
        query_validator: _DpsQueryValidator,
    ) -> None:
        err_msgs = [
            f"Expected `granularity` to be of type `str` or None, not {type(granularity)}",
            f"Expected `aggregates` to be of type `str`, `list[str]` or None, not {type(aggregates)}",
            "When passing `granularity`, argument `aggregates` is also required.",
            "Empty list of `aggregates` passed, expected at least one!",
            "When passing `aggregates`, argument `granularity` is also required.",
            "'Include outside points' is not supported for aggregates.",
        ]
        queries = _FullDatapointsQuery(
            id=1,
            granularity=granularity,
            aggregates=aggregates,  # type: ignore[arg-type]
            include_outside_points=outside,  # type: ignore[arg-type]
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
    def test_function__verify_time_range__valid_inputs(
        self, start: int | None, end: int | str | None, query_validator: _DpsQueryValidator
    ) -> None:
        gran_dct: dict = {"granularity": random_granularity(), "aggregates": random_aggregates()}
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
    def test_function__verify_time_range__raises(
        self, start: int | str | datetime | None, end: int, query_validator: _DpsQueryValidator
    ) -> None:
        gran_dct: dict = {"granularity": random_granularity(), "aggregates": random_aggregates()}
        for kwargs in [{}, gran_dct]:
            full_query = _FullDatapointsQuery(id=1, start=start, end=end, **kwargs)
            all_queries = full_query.parse_into_queries()
            with pytest.raises(ValueError, match="Invalid time range"):
                query_validator(all_queries)

    def test_retrieve_aggregates__include_outside_points_raises(self, query_validator: _DpsQueryValidator) -> None:
        id_dct_lst: list[DatapointsQuery] = [
            DatapointsQuery(id=ts_id, granularity=random_granularity(), aggregates=random_aggregates())
            for ts_id in random_cognite_ids(10)
        ]
        # Only one time series is configured wrong and will raise:
        id_dct_lst[-1].include_outside_points = True

        full_query = _FullDatapointsQuery(id=id_dct_lst, include_outside_points=False)
        all_queries = full_query.parse_into_queries()
        with pytest.raises(ValueError, match=r"'Include outside points' is not supported for aggregates\."):
            query_validator(all_queries)
