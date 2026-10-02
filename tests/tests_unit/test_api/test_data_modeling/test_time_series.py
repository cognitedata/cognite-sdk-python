from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from cognite.client import CogniteClient
from cognite.client._api.data_modeling.time_series import _build_filter
from cognite.client._cognite_client import AsyncCogniteClient
from cognite.client.data_classes.data_modeling import NodeList
from cognite.client.data_classes.filters import Filter

TYPE_IS_STATE = {"equals": {"property": ["cdf_cdm", "CogniteTimeSeries/v1", "type"], "value": "state"}}
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
        "filter, is_state, expected",
        [
            (None, None, None),
            (None, True, TYPE_IS_STATE),
            (None, False, {"not": TYPE_IS_STATE}),
            (SPACE_FILTER, None, SPACE_FILTER),
            (SPACE_FILTER, True, {"and": [TYPE_IS_STATE, SPACE_FILTER]}),
            (SPACE_FILTER, False, {"and": [{"not": TYPE_IS_STATE}, SPACE_FILTER]}),
        ],
    )
    def test_build_filter(
        self,
        filter: dict[str, Any] | None,
        is_state: bool | None,
        expected: dict[str, Any] | None,
        filter_as_dict: bool,
    ) -> None:
        given: Filter | dict | None = filter
        if not (filter is None or filter_as_dict):
            given = Filter.load(filter)

        assert expected == as_sent(_build_filter(given, is_state=is_state))


class TestDMTimeSeriesListIsState:
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
            ({"is_state": True}, TYPE_IS_STATE),
            ({"is_state": False}, {"not": TYPE_IS_STATE}),
            ({"filter": SPACE_FILTER}, SPACE_FILTER),
            ({"is_state": True, "filter": SPACE_FILTER}, {"and": [TYPE_IS_STATE, SPACE_FILTER]}),
            ({"is_state": False, "filter": SPACE_FILTER}, {"and": [{"not": TYPE_IS_STATE}, SPACE_FILTER]}),
        ],
    )
    def test_is_state_is_combined_with_filter(
        self, cognite_client: CogniteClient, list_mock: AsyncMock, kwargs: dict[str, Any], expected: dict | None
    ) -> None:
        cognite_client.data_modeling.time_series.list(**kwargs)
        assert self.sent_filter(list_mock) == expected
