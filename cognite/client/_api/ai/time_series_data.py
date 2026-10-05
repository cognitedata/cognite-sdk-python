from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints
from cognite.client.data_classes.ai import TimeSeriesForecast, TimeSeriesForecastList
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._forecasting import AlignmentProblem, SourceSeries, build_inputs
from cognite.client.utils._text import to_snake_case
from cognite.client.utils._time import granularity_to_ms
from cognite.client.utils.useful_types import SequenceNotStr


class AITimeSeriesDataAPI(APIClient):
    @overload
    async def forecast(
        self,
        *,
        id: int,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    async def forecast(
        self,
        *,
        id: Sequence[int],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    async def forecast(
        self,
        *,
        external_id: str,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    async def forecast(
        self,
        *,
        external_id: SequenceNotStr[str],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    async def forecast(
        self,
        *,
        instance_id: NodeId,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    async def forecast(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    async def forecast(
        self,
        *,
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None,
        instance_id: NodeId | Sequence[NodeId] | None,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    async def forecast(
        self,
        *,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast | TimeSeriesForecastList:
        """Forecast time series 512 steps past their last datapoint in `[start, end)`.

        The forecasting model needs evenly spaced history, and this method never resamples or truncates the data.
        With `granularity` and `aggregate`, the history is aligned using CDF aggregates, and empty buckets are sent as
        missing. Without them, raw datapoints are used, and they must already be evenly spaced; gaps are allowed.

        Args:
            id (int | Sequence[int] | None): Id(s) of the time series.
            external_id (str | SequenceNotStr[str] | None): External id(s) of the time series.
            instance_id (NodeId | Sequence[NodeId] | None): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "30d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "1h".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to forecast, for example "average" or "interpolation".
            cohort (str | None): Forecast all the requested time series jointly, aligned on one common grid. Omit it
                to forecast each time series independently.

        Returns:
            TimeSeriesForecast | TimeSeriesForecastList: A single forecast if a single identifier was passed, otherwise
            a list you can look up with `.get(id=..., external_id=..., instance_id=...)`.

        Examples:

            Forecast a time series from 30 days of hourly averages:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> fc = client.ai.time_series.data.forecast(
                ...     external_id="21-PT-1019", start="30d-ago", granularity="1h", aggregate="average"
                ... )

            Forecast the discharge pressures of three compressors on one compression train jointly:

                >>> res = client.ai.time_series.data.forecast(
                ...     external_id=["23-PT-1101", "23-PT-1201", "23-PT-1301"],
                ...     start="14d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     cohort="compression-train-a",
                ... )
                >>> fc = res.get(external_id="23-PT-1201")
        """
        dps_list, is_single = await self._retrieve_history(
            id, external_id, instance_id, start, end, granularity, aggregate
        )
        inputs = build_inputs(
            _to_source_series(dps_list, aggregate),
            step_ms=None if granularity is None else granularity_to_ms(granularity),
            cohort=cohort,
            hints=_ALIGNMENT_HINTS,
        )
        results = await self._cognite_client.ai.time_series.forecast(inputs)
        forecasts = [
            TimeSeriesForecast(res.forecast, dps.id, dps.external_id, dps.instance_id, res.cohort)
            for dps, res in zip(dps_list, results)
        ]
        return forecasts[0] if is_single else TimeSeriesForecastList(forecasts, results.quantile_levels)

    async def _retrieve_history(
        self,
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None,
        instance_id: NodeId | Sequence[NodeId] | None,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None,
        granularity: str | None,
        aggregate: Aggregate | str | None,
    ) -> tuple[list[Datapoints], bool]:
        if (granularity is None) != (aggregate is None):
            raise ValueError("Pass `granularity` and `aggregate` together, or neither to use raw datapoints.")
        res = await self._cognite_client.time_series.data.retrieve(
            id=id,
            external_id=external_id,
            instance_id=instance_id,
            start=start,
            end=end,
            aggregates=aggregate,
            granularity=granularity,
        )
        if isinstance(res, Datapoints):
            return [res], True
        if not res:
            raise ValueError("Pass at least one of `id`, `external_id` or `instance_id`.")
        return list(res), False


_ALIGNMENT_HINTS: dict[AlignmentProblem, str] = {
    "irregular": "Pass `granularity=` and `aggregate=` to align it with CDF aggregates, "
    "e.g. `granularity='1h', aggregate='average'`.",
    "mixed_spacing": "Pass `granularity=` and `aggregate=` so all time series share one grid.",
    "off_grid": "Pass `granularity=` and `aggregate=` so all time series share one grid.",
    "too_long": "Narrow `start`/`end` or use a coarser `granularity`.",
    "too_short": "Widen `start`/`end`.",
}


def _to_source_series(dps_list: Sequence[Datapoints], aggregate: Aggregate | str | None) -> list[SourceSeries]:
    attribute = "value" if aggregate is None else to_snake_case(aggregate)
    sources, seen = [], set()
    for i, dps in enumerate(dps_list):
        name = _label(dps)
        if dps.is_string:
            raise ValueError(f"Time series '{name}' is a string time series; only numeric time series can be forecast.")
        label = name if name not in seen else f"{name}#{i}"
        seen.add(label)
        sources.append(SourceSeries(label, dps.timestamp, getattr(dps, attribute) or []))
    return sources


def _label(dps: Datapoints) -> str:
    if dps.external_id is not None:
        return dps.external_id
    if dps.instance_id is not None:
        return f"{dps.instance_id.space}:{dps.instance_id.external_id}"
    return str(dps.id)
