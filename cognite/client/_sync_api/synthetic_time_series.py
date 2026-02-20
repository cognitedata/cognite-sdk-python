"""
===============================================================================
d5dfa84b4d5e61943efef506246e1da8
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, overload
from zoneinfo import ZoneInfo

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import Datapoints, DatapointsList, TimeSeries, TimeSeriesWrite
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import sympy


class SyncSyntheticDatapointsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def query(
        self,
        expressions: SequenceNotStr[str] | SequenceNotStr[sympy.Basic],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
    ) -> DatapointsList: ...

    @overload
    def query(
        self,
        expressions: str | sympy.Basic,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
    ) -> Datapoints: ...

    def query(
        self,
        expressions: str | sympy.Basic | Sequence[str] | Sequence[sympy.Basic],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
    ) -> Datapoints | DatapointsList:
        """
        `Calculate the result of a function on time series. <https://developer.cognite.com/api#tag/Synthetic-Time-Series/operation/querySyntheticTimeseries>`_

        Info:
            You can read the guide to synthetic time series in our `documentation <https://docs.cognite.com/dev/concepts/resource_types/synthetic_timeseries>`_.

        Args:
            expressions: Functions to be calculated. Supports both strings and sympy expressions. Strings can have either the API `ts{}` syntax, or contain variable names to be replaced using the `variables` parameter.
            start: Inclusive start.
            end: Exclusive end.
            limit: Number of datapoints per expression to retrieve.
            variables: An optional map of symbol replacements.
            aggregate: use this aggregate when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            granularity: use this granularity with the aggregate.
            target_unit: use this target_unit when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            target_unit_system: Same as target_unit, but with unit system (e.g. SI). Only one of target_unit and target_unit_system can be specified.
            timezone: The timezone to use when aggregating datapoints. For aggregates of granularity 'hour' and longer, which time zone should we align to. Align to the start of the hour, start of the day or start of the month. For time zones of type Region/Location, the aggregate duration can vary, typically due to daylight saving time. For time zones of type UTC+/-HH:MM, use increments of 15 minutes. Default: "UTC" (None)

        Returns:
            A DatapointsList object containing the calculated data.

        Examples:

            Execute a synthetic time series query with an expression. Here we sum three time series plus a constant. The first is referenced by ID,
            the second by external ID, and the third by instance ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> expression = '''
                ...     123
                ...     + ts{id:123}
                ...     + ts{externalId:'abc'}
                ...     + ts{space:'my-space',externalId:'my-ts-xid'}
                ... '''
                >>> dps = client.time_series.data.synthetic.query(
                ...     expressions=expression,
                ...     start="2w-ago",
                ...     end="now")

            You can also specify variables for an easier query syntax:

                >>> from cognite.client.data_classes.data_modeling.ids import NodeId
                >>> ts = client.time_series.retrieve(id=123)
                >>> variables = {
                ...     "A": ts,
                ...     "B": "my_ts_external_id",
                ...     "C": NodeId("my-space", "my-ts-xid"),
                ... }
                >>> dps = client.time_series.data.synthetic.query(
                ...     expressions="A+B+C", start="2w-ago", end="2w-ahead", variables=variables)

            Use sympy to build complex expressions:

                >>> from sympy import symbols, cos, sin
                >>> x, y = symbols("x y")
                >>> dps = client.time_series.data.synthetic.query(
                ...     [sin(x), y*cos(x)],
                ...     start="2w-ago",
                ...     end="now",
                ...     variables={x: "foo", y: "bar"},
                ...     aggregate="interpolation",
                ...     granularity="15m",
                ...     target_unit="temperature:deg_c",
                ...     timezone="Europe/Oslo",  # can also use this format: 'UTC+05:30'
                ... )
        """
        return run_sync(
            self.__async_client.time_series.data.synthetic.query(
                expressions=expressions,
                start=start,
                end=end,
                limit=limit,
                variables=variables,
                aggregate=aggregate,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                timezone=timezone,
            )
        )
