from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock

import pytest

from cognite.client import CogniteClient
from cognite.client._api.data_modeling.time_series import _build_filter
from cognite.client._cognite_client import AsyncCogniteClient
from cognite.client.data_classes.data_modeling import NodeList
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.time_series import TimeSeriesType

TYPE_PROPERTY = ["cdf_cdm", "CogniteTimeSeries/v1", "type"]
TYPE_IS_STATE = {"equals": {"property": TYPE_PROPERTY, "value": "state"}}
TYPE_IN_NUMERIC_STRING = {"in": {"property": TYPE_PROPERTY, "values": ["numeric", "string"]}}
SPACE_FILTER = {"equals": {"property": ["node", "space"], "value": "sp"}}


def as_sent(flt: Filter | None) -> dict[str, Any] | None:
    # Properties are loaded as tuples of strings, but end up as lists after json has serialized.
    # Thus we have this small helper to convert it to the expected format.
    if flt is None:
        return None
    else:
        return json.loads(json.dumps(flt.dump(camel_case_property=False)))


class TestBuildFilter:
    @pytest.mark.parametrize("filter_as_dict", [True, False])
    @pytest.mark.parametrize(
        "filter, time_series_type, expected",
        [
            (None, None, None),
            (None, "state", TYPE_IS_STATE),  # single -> equals
            (None, ["state"], TYPE_IS_STATE),  # ...also in a sequence
            (None, ("state",), TYPE_IS_STATE),
            (None, ["numeric", "string"], TYPE_IN_NUMERIC_STRING),  # multiple -> in
            (None, ("numeric", "string"), TYPE_IN_NUMERIC_STRING),  # any sequence works
            (SPACE_FILTER, None, SPACE_FILTER),
            (SPACE_FILTER, "state", {"and": [TYPE_IS_STATE, SPACE_FILTER]}),
            (SPACE_FILTER, ["numeric", "string"], {"and": [TYPE_IN_NUMERIC_STRING, SPACE_FILTER]}),
        ],
    )
    def test_build_filter(
        self,
        filter: dict[str, Any] | None,
        time_series_type: TimeSeriesType | Sequence[TimeSeriesType] | None,
        expected: dict[str, Any] | None,
        filter_as_dict: bool,
    ) -> None:
        given: Filter | dict | None = filter
        if not (filter is None or filter_as_dict):
            given = Filter.load(filter)

        assert expected == as_sent(_build_filter(given, time_series_type=time_series_type))

    @pytest.mark.parametrize("time_series_type", [[], ()])
    def test_build_filter_raises_on_empty_time_series_types(self, time_series_type: Sequence[TimeSeriesType]) -> None:
        with pytest.raises(ValueError, match="'time_series_type' must not be empty, pass None"):
            _build_filter(None, time_series_type=time_series_type)


class TestDMTimeSeriesListTimeSeriesTypes:
    @pytest.fixture
    def list_mock(self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
        mock = AsyncMock(return_value=NodeList([]))
        monkeypatch.setattr(async_client.data_modeling.instances, "list", mock)
        return mock

    @staticmethod
    def sent_filter(list_mock: AsyncMock) -> dict[str, Any] | None:
        flt = list_mock.call_args.kwargs["filter"]
        if flt is None or isinstance(flt, dict):
            return flt
        return json.loads(json.dumps(flt.dump(camel_case_property=False)))

    @pytest.mark.parametrize(
        "kwargs, expected",
        [
            ({}, None),
            ({"time_series_type": "state"}, TYPE_IS_STATE),
            ({"time_series_type": ["numeric", "string"]}, TYPE_IN_NUMERIC_STRING),
            ({"filter": SPACE_FILTER}, SPACE_FILTER),
            ({"time_series_type": "state", "filter": SPACE_FILTER}, {"and": [TYPE_IS_STATE, SPACE_FILTER]}),
        ],
    )
    def test_time_series_types_is_combined_with_filter(
        self, cognite_client: CogniteClient, list_mock: AsyncMock, kwargs: dict[str, Any], expected: dict | None
    ) -> None:
        cognite_client.data_modeling.time_series.list(**kwargs)
        assert self.sent_filter(list_mock) == expected
