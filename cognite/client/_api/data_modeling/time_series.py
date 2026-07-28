from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeries
from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeList
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._data_modeling import resolve_source, strip_canonical_source
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client._api.data_modeling.instances import InstancesAPI
    from cognite.client.config import ClientConfig

COGNITE_TIME_SERIES_VIEW_ID = CogniteTimeSeries.get_source()


class DataModelingTimeSeriesAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        # TODO: Add DataModelingDatapointsAPI
        super().__init__(config, api_version, cognite_client)

    @property
    def _instances_api(self) -> InstancesAPI:
        return self._cognite_client.data_modeling.instances

    async def __call__(self) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    @overload
    async def retrieve(
        self,
        node_ids: NodeId | tuple[str, str],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> Node | None: ...

    @overload
    async def retrieve(
        self,
        node_ids: Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> NodeList[Node]: ...

    async def retrieve(
        self,
        node_ids: NodeId | tuple[str, str] | Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
    ) -> Node | NodeList[Node] | None:
        """`Retrieve one or more time series by instance ID <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_.

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
        sources, strip = resolve_source(source, COGNITE_TIME_SERIES_VIEW_ID)
        result = await self._instances_api.retrieve_nodes(nodes=node_ids, sources=sources)
        if strip:
            strip_canonical_source(result, COGNITE_TIME_SERIES_VIEW_ID)
        return result

    async def list(
        self,
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_TIME_SERIES_VIEW_ID,
        space: str | SequenceNotStr[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """`List time series nodes <https://api-docs.cognite.com/20230101/tag/Instances/operation/advancedListInstance>`_.

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
        sources, strip = resolve_source(source, COGNITE_TIME_SERIES_VIEW_ID)
        results = await self._instances_api.list(
            instance_type="node",
            sources=sources,
            space=space,
            sort=sort,
            filter=filter,
            limit=limit,
        )
        if strip:
            strip_canonical_source(results, COGNITE_TIME_SERIES_VIEW_ID)
        return results
