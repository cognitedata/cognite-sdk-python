from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api.data_modeling._node_id_utils import as_node_id, as_node_ids
from cognite.client._api.data_modeling.datapoints import DataModelingDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import TimeSeries, TimeSeriesList
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Exists

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client._api.time_series import TimeSeriesAPI
    from cognite.client.config import ClientConfig

_HAS_INSTANCE_ID = Exists(["instanceId", "space"])


class DataModelingTimeSeriesAPI(APIClient):
    """Access time series via Data Modeling instance IDs.

    This API mirrors a subset of client.time_series but restricts identifiers to
    instance_id (NodeId) only, making the DM-native workflow explicit.
    """

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._time_series_api: TimeSeriesAPI = cognite_client.time_series
        self.data = DataModelingDatapointsAPI(config, api_version, cognite_client)

    @overload
    async def retrieve(self, instance_id: NodeId | tuple[str, str]) -> TimeSeries | None: ...

    @overload
    async def retrieve(
        self, instance_id: Sequence[NodeId | tuple[str, str]], *, ignore_unknown_ids: bool = ...
    ) -> TimeSeriesList: ...

    async def retrieve(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeries | TimeSeriesList | None:
        """`Retrieve one or more time series by instance ID. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Single instance ID or a list of instance IDs. Each identifier can be a :class:`~cognite.client.data_classes.data_modeling.NodeId` or a ``(space, external_id)`` tuple.
            ignore_unknown_ids (bool): Ignore IDs that are not found rather than throw an exception. Only used when a sequence is passed.

        Returns:
            TimeSeries | TimeSeriesList | None: A single ``TimeSeries`` (or ``None`` if not found) when given a single identifier, or a ``TimeSeriesList`` when given a sequence.

        Examples:

            Get a single time series by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.retrieve(NodeId("my-space", "my-ts"))

            Using a tuple shorthand:

                >>> res = client.data_modeling.time_series.retrieve(("my-space", "my-ts"))

            Get multiple time series:

                >>> res = client.data_modeling.time_series.retrieve(
                ...     [NodeId("my-space", "ts-1"), ("my-space", "ts-2")]
                ... )
        """
        if isinstance(instance_id, (NodeId, tuple)):
            return await self._time_series_api.retrieve(instance_id=as_node_id(instance_id))
        return await self._time_series_api.retrieve_multiple(
            instance_ids=as_node_ids(instance_id),
            ignore_unknown_ids=ignore_unknown_ids,
        )

    # Note: Intentionally a small surface — extra filters can be added later if requested.
    async def list(
        self,
        is_string: bool | None = None,
        is_step: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`List time series that are backed by a Data Modeling node. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/listTimeSeries>`_

        Only time series with an instance ID are returned.

        Args:
            is_string (bool | None): Filter by whether the time series is a string time series.
            is_step (bool | None): Filter by whether the time series is a step (piecewise constant) time series.
            limit (int | None): Maximum number of time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TimeSeriesList: The requested time series, all of which have an instance ID.

        Examples:

            List all DM-backed time series:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.list()

            List only step time series:

                >>> res = client.data_modeling.time_series.list(is_step=True)
        """
        return await self._time_series_api.list(
            is_string=is_string,
            is_step=is_step,
            limit=limit,
            advanced_filter=_HAS_INSTANCE_ID,
        )
