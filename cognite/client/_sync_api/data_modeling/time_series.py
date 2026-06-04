"""
===============================================================================
c785440f6657cb93d7d732c42f4df832
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeries
from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeList
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

COGNITE_TIME_SERIES_VIEW_ID = CogniteTimeSeries.get_source()


class SyncDataModelingTimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def retrieve(
        self,
        node_ids: NodeId | tuple[str, str],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> Node | None: ...

    @overload
    def retrieve(
        self,
        node_ids: Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> NodeList[Node]: ...

    def retrieve(
        self,
        node_ids: NodeId | tuple[str, str] | Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> Node | NodeList[Node] | None:
        """
        `Retrieve one or more time series by instance ID <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_.

        Only nodes that are time series (i.e. have data in the CogniteTimeSeries view) will be returned.
        If a single instance ID is requested and it is not found, ``None`` is returned.

        Args:
            node_ids (NodeId | tuple[str, str] | Sequence[NodeId] | Sequence[tuple[str, str]]): Single instance ID or a list of instance IDs.
            source (View | ViewId | tuple[str, str, str]): The view to fetch properties from. Defaults to CogniteTimeSeries.

        Returns:
            Node | NodeList[Node] | None: A single ``Node`` (or ``None`` if not found) when given a single identifier, or a ``NodeList`` when given a sequence.

        Examples:

            Retrieve a single time series by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.retrieve(NodeId("my-space", "my-ts"))

            Using a tuple shorthand:

                >>> res = client.data_modeling.time_series.retrieve(("my-space", "my-ts"))

            Retrieve multiple time series nodes:

                >>> res = client.data_modeling.time_series.retrieve(
                ...     [("my-space", "ts-1"), ("my-space", "ts-2")]
                ... )

            Fetch properties from a custom view (note, only time series will be returned):

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> res = client.data_modeling.time_series.retrieve(
                ...     NodeId("my-space", "my-ts"),
                ...     source=ViewId("my-space", "MyTimeSeriesExtension", "v1"),
                ... )
        """
        return run_sync(self.__async_client.data_modeling.time_series.retrieve(node_ids=node_ids, source=source))

    def list(
        self,
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
        space: str | SequenceNotStr[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """
        `List time series nodes <https://api-docs.cognite.com/20230101/tag/Instances/operation/listInstances>`_.

        Only time series nodes will be returned, regardless of the source passed.

        Args:
            source (View | ViewId | tuple[str, str, str]): The view to fetch properties from. Defaults to CogniteTimeSeries.
            space (str | SequenceNotStr[str] | None): Restrict results to this space (or list of spaces).
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): Sort order for the results.
            filter (Filter | dict[str, Any] | None): Advanced filter to apply. See :class:`~cognite.client.data_classes.filters`.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            NodeList[Node]: The matching time series.

        Examples:

            List a few time series:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.list(limit=5)

            List all time series in a specific space:

                >>> res = client.data_modeling.time_series.list(space="my-space", limit=None)

            Fetch properties from a custom view (note, only time series will be returned), and
            apply a custom filter on the name:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes import filters
                >>> view_id = ViewId("my-space", "MyTimeSeriesExtension", "v1")
                >>> res = client.data_modeling.time_series.list(
                ...     source=view_id,
                ...     filter=filters.Prefix(view_id.as_property_ref("name"), "sensor"),
                ...     limit=None,
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.time_series.list(
                source=source, space=space, sort=sort, filter=filter, limit=limit
            )
        )
