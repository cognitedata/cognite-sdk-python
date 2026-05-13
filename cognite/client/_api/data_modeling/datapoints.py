from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from cognite.client._api.data_modeling._node_id_utils import as_node_id
from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoints import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapoint,
    LatestDatapointList,
)
from cognite.client.utils._identifier import InstanceId

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import AsyncCogniteClient
    from cognite.client._api.datapoints import DatapointsAPI
    from cognite.client.config import ClientConfig


def _require_instance_id(query: DatapointsQuery) -> None:
    if not isinstance(query.identifier.as_primitive(), InstanceId):
        raise ValueError(
            f"DatapointsQuery must have instance_id set, got identifier: {query.identifier.as_primitive()!r}"
        )


class DataModelingDatapointsAPI(APIClient):
    """Access datapoints via Data Modeling instance IDs.

    This API mirrors a subset of client.time_series.data but restricts identifiers to
    instance_id (NodeId) only, making the DM-native workflow explicit.
    """

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._dps_api: DatapointsAPI = cognite_client.time_series.data

    async def retrieve(
        self,
        query: DatapointsQuery | Sequence[DatapointsQuery],
    ) -> Datapoints | DatapointsList | None:
        """`Retrieve datapoints for one or more time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.
        All query options (start, end, aggregates, granularity, limit, …) are set on the query object.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            Datapoints | DatapointsList | None: A ``Datapoints`` object for a single query, or ``DatapointsList`` for multiple. Returns ``None`` if ``ignore_unknown_ids=True`` and the single query was not found.

        Examples:

            Retrieve raw datapoints:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> dps = client.data_modeling.time_series.data.retrieve(
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts"), start="2w-ago")
                ... )

            Retrieve hourly averages for multiple time series:

                >>> dps = client.data_modeling.time_series.data.retrieve(
                ...     [
                ...         DatapointsQuery(instance_id=NodeId("my-space", "ts-1"), aggregates="average", granularity="1h"),
                ...         DatapointsQuery(instance_id=NodeId("my-space", "ts-2"), aggregates="average", granularity="1h"),
                ...     ]
                ... )
        """
        if isinstance(query, DatapointsQuery):
            _require_instance_id(query)
        else:
            for q in query:
                _require_instance_id(q)
        return await self._dps_api.retrieve(instance_id=query)

    async def retrieve_arrays(
        self,
        query: DatapointsQuery | Sequence[DatapointsQuery],
    ) -> DatapointsArray | DatapointsArrayList | None:
        """`Retrieve datapoints as numpy arrays. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.

        Note:
            This method requires ``numpy`` to be installed.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            DatapointsArray | DatapointsArrayList | None: A ``DatapointsArray`` for a single query, or ``DatapointsArrayList`` for multiple.

        Examples:

            Retrieve raw datapoints as numpy arrays:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> arr = client.data_modeling.time_series.data.retrieve_arrays(
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts"), start="1d-ago")
                ... )
        """
        if isinstance(query, DatapointsQuery):
            _require_instance_id(query)
        else:
            for q in query:
                _require_instance_id(q)
        return await self._dps_api.retrieve_arrays(instance_id=query)

    async def retrieve_dataframe(
        self,
        query: DatapointsQuery | Sequence[DatapointsQuery],
    ) -> pd.DataFrame:
        """Get datapoints as a pandas DataFrame.

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.

        Note:
            This method requires ``pandas`` to be installed.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            pd.DataFrame: DataFrame with a DatetimeIndex and one column per (instance_id, aggregate) combination.

        Examples:

            Retrieve daily averages as a DataFrame:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> df = client.data_modeling.time_series.data.retrieve_dataframe(
                ...     DatapointsQuery(
                ...         instance_id=NodeId("my-space", "my-ts"),
                ...         aggregates="average",
                ...         granularity="1d",
                ...         start="30d-ago",
                ...     )
                ... )
        """
        if isinstance(query, DatapointsQuery):
            _require_instance_id(query)
        else:
            for q in query:
                _require_instance_id(q)
        return await self._dps_api.retrieve_dataframe(instance_id=query)

    async def retrieve_latest(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | LatestDatapointList | None:
        """`Get the latest datapoint for one or more time series by instance ID. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
            before (None | int | str | datetime.datetime): Get latest datapoint before this time. Default: None ("now").
            target_unit (str | None): Unit to convert returned datapoints to. Cannot be used with target_unit_system.
            target_unit_system (str | None): Unit system to convert returned datapoints to. Cannot be used with target_unit.
            include_status (bool): Include status code for the datapoint. Default: False.
            ignore_bad_datapoints (bool): Ignore datapoints with bad status codes. Default: True.
            treat_uncertain_as_bad (bool): Treat uncertain status codes as bad. Default: True.
            ignore_unknown_ids (bool): Ignore unknown instance IDs instead of raising. Default: False.

        Returns:
            LatestDatapoint | LatestDatapointList | None: The latest datapoint, or a list if multiple IDs were given.

        Examples:

            Get the latest datapoint:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.data.retrieve_latest(
                ...     NodeId("my-space", "my-ts")
                ... )
        """
        return await self._dps_api.retrieve_latest(
            instance_id=as_node_id(instance_id) if isinstance(instance_id, (NodeId, tuple)) else [as_node_id(iid) for iid in instance_id],
            before=before,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            include_status=include_status,
            ignore_bad_datapoints=ignore_bad_datapoints,
            treat_uncertain_as_bad=treat_uncertain_as_bad,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def insert(
        self,
        datapoints: Datapoints
        | DatapointsArray
        | Sequence[dict[str, Any]]
        | Sequence[tuple[int | float | datetime.datetime, int | float | str]],
        instance_id: NodeId | tuple[str, str],
    ) -> None:
        """Insert datapoints into a time series by instance ID.

        Args:
            datapoints (Datapoints | DatapointsArray | Sequence[dict] | Sequence[tuple]): Datapoints to insert. Each element is either a ``(timestamp, value)`` tuple, a ``(timestamp, value, status_code)`` tuple, or a dict with ``"timestamp"`` and ``"value"`` keys.
            instance_id (NodeId | tuple[str, str]): Instance ID of the target time series.

        Examples:

            Insert datapoints using tuples:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> datapoints = [
                ...     (datetime(2024, 1, 1, tzinfo=timezone.utc), 1.0),
                ...     (datetime(2024, 1, 2, tzinfo=timezone.utc), 2.0),
                ... ]
                >>> client.data_modeling.time_series.data.insert(
                ...     datapoints, NodeId("my-space", "my-ts")
                ... )
        """
        await self._dps_api.insert(datapoints, instance_id=as_node_id(instance_id))

    async def delete_range(
        self,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        instance_id: NodeId | tuple[str, str],
    ) -> None:
        """Delete a range of datapoints from a time series by instance ID.

        Args:
            start (int | str | datetime.datetime): Inclusive start of the range to delete.
            end (int | str | datetime.datetime): Exclusive end of the range to delete.
            instance_id (NodeId | tuple[str, str]): Instance ID of the time series.

        Examples:

            Delete the last week of datapoints:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.time_series.data.delete_range(
                ...     start="1w-ago", end="now", instance_id=NodeId("my-space", "my-ts")
                ... )
        """
        await self._dps_api.delete_range(start, end, instance_id=as_node_id(instance_id))
