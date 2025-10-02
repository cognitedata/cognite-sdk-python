"""
===============================================================================
50524e5d63347bb7b8a42341922171f9
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, overload

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

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    @overload
    def query(
        self,
        expressions: SequenceNotStr[str] | SequenceNotStr[sympy.Basic],
        start: int | str | datetime,
        end: int | str | datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> DatapointsList: ...

    @overload
    def query(
        self,
        expressions: str | sympy.Basic,
        start: int | str | datetime,
        end: int | str | datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> Datapoints: ...

    def query(
        self,
        expressions: str | sympy.Basic | Sequence[str] | Sequence[sympy.Basic],
        start: int | str | datetime,
        end: int | str | datetime,
        limit: int | None = None,
        variables: Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> Datapoints | DatapointsList:
        """
        `Calculate the result of a function on time series. <https://developer.cognite.com/api#tag/Synthetic-Time-Series/operation/querySyntheticTimeseries>`_

        Args:
            expressions (str | sympy.Basic | Sequence[str] | Sequence[sympy.Basic]): Functions to be calculated. Supports both strings and sympy expressions. Strings can have either the API `ts{}` syntax, or contain variable names to be replaced using the `variables` parameter.
            start (int | str | datetime): Inclusive start.
            end (int | str | datetime): Exclusive end.
            limit (int | None): Number of datapoints per expression to retrieve.
            variables (Mapping[str | sympy.Symbol, str | NodeId | TimeSeries | TimeSeriesWrite] | None): An optional map of symbol replacements.
            aggregate (str | None): use this aggregate when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            granularity (str | None): use this granularity with the aggregate.
            target_unit (str | None): use this target_unit when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            target_unit_system (str | None): Same as target_unit, but with unit system (e.g. SI). Only one of target_unit and target_unit_system can be specified.

        Returns:
            Datapoints | DatapointsList: A DatapointsList object containing the calculated data.

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
                ...     target_unit="temperature:deg_c")
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
            )
        )
