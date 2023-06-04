from __future__ import annotations

from typing import Iterator, Literal, Sequence, Type, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import (
    EdgeDataModelingId,
    EdgeId,
    NodeDataModelingId,
    NodeId,
    TypedDataModelingId,
    load_identifier,
)
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeList,
    Instance,
    InstanceFilter,
    InstanceList,
    InstanceSort,
    Node,
    NodeApply,
    NodeList,
)
from cognite.client.data_classes.data_modeling.shared import ViewReference


class InstancesAPI(APIClient):
    _RESOURCE_PATH = "/models/instances"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node"] = "node",
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Node]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["edge"] = "edge",
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node"] = "node",
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[NodeList]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["edge"] = "edge",
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[EdgeList]:
        ...

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node", "edge"] = "node",
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge] | Iterator[EdgeList] | Iterator[Node] | Iterator[NodeList]:
        """Iterate over instances
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.
        Args:
            chunk_size (int, optional): Number of data_models to return in each chunk. Defaults to yielding
                                        one instance at a time.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewReference]): Views to retrieve properties from.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to return all items.
            sort (list[InstanceSort]): How you want the listed instances information ordered.
            filter (dict | Filter): Advanced filtering of instances.
        Yields:
            Instance | InstanceList: yields Instance one by one if chunk_size is not specified, else InstanceList objects.
        """
        other_params = InstanceFilter(include_typing, sources, instance_type).dump(camel_case=True)
        if sort:
            other_params["sort"] = [s.dump(camel_case=True) if isinstance(s, InstanceSort) else s for s in sort]

        list_cls, resource_cls = self._get_classes(instance_type)

        return self._list_generator(
            list_cls=list_cls,
            resource_cls=resource_cls,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            other_params=other_params,
        )

    def __iter__(self) -> Iterator[Node]:
        """Iterate over instances
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.
        Yields:
            Instance: yields Instances one by one.
        """
        return cast(Iterator[Node], self())

    @overload
    def retrieve(
        self, ids: NodeId, sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> Node | None:
        ...

    @overload
    def retrieve(
        self, ids: Sequence[NodeId], sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> NodeList:
        ...

    @overload
    def retrieve(
        self, ids: EdgeId, sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> Edge | None:
        ...

    @overload
    def retrieve(
        self, ids: Sequence[EdgeId], sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> EdgeList:
        ...

    def retrieve(
        self,
        ids: NodeId | EdgeId | Sequence[NodeId] | Sequence[EdgeId],
        sources: list[ViewReference] | None = None,
        include_typing: bool = False,
    ) -> Instance | InstanceList | None:
        """`Retrieve one or more instance by id(s). <https://docs.cognite.com/api/v1/#tag/Instances/operation/byExternalIdsInstances>`_
        Args:
            ids (InstanceId | Sequence[InstanceId]): Identifier for instance(s).
            sources (list[ViewReference] | None): Retrieve properties from the listed - by reference - views.
            include_typing (bool): Whether to return property type information as part of the result.
        Returns:
            Optional[Instance]: Requested instance or None if it does not exist.
        Examples:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(('node', 'mySpace', 'myNode'))
        """
        identifier = load_identifier(ids)
        return self._retrieve_multiple(list_cls=InstanceList, resource_cls=Instance, identifiers=identifier)

    @overload
    def delete(self, id: NodeId | Sequence[NodeId]) -> list[NodeDataModelingId]:
        ...

    @overload
    def delete(self, id: EdgeId | Sequence[EdgeId]) -> list[EdgeDataModelingId]:
        ...

    def delete(
        self, id: NodeId | EdgeId | Sequence[NodeId] | Sequence[EdgeId]
    ) -> list[NodeDataModelingId] | list[EdgeDataModelingId]:
        """`Delete one or more instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/deleteBulk>`_
        Args:
            id (InstanceId | Sequence[InstanceId): The instance identifier(s).
        Returns:
            The instance(s) which has been deleted. Empty list if nothing was deleted.
        Examples:
            Delete instances by id::
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(("node", "myNode", "mySpace"))
        """
        deleted_instances = cast(
            list,
            self._delete_multiple(identifiers=load_identifier(id), wrap_ids=True, returns_items=True),
        )
        return [
            TypedDataModelingId(space=item["space"], external_id=item["externalId"], instance_type=item["instanceType"])
            for item in deleted_instances
        ]

    @overload
    def list(
        self,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node"] = "node",
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList:
        ...

    @overload
    def list(
        self,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["edge"] = "edge",
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> EdgeList:
        ...

    def list(
        self,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node", "edge"] = "node",
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList | EdgeList:
        """`List instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/advancedListInstance>`_
        Args:
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewReference]): Views to retrieve properties from.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.
            sort (list[InstanceSost]): How you want the listed instances information ordered.
            filter (dict | Filter): Advnanced filtering of instances.
        Returns:
            InstanceList: List of requested instances
        Examples:
            List instances and limit to 5:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> instance_list = c.data_modeling.instances.list(limit=5)
            Iterate over instances:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for instance in c.data_modeling.instances:
                ...     instance # do something with the instance
            Iterate over chunks of instances to reduce memory load:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for instance_list in c.data_modeling.instances(chunk_size=100):
                ...     instance_list # do something with the instances
        """
        other_params = InstanceFilter(include_typing, sources, instance_type).dump(camel_case=True)
        if sort:
            other_params["sort"] = [s.dump(camel_case=True) if isinstance(s, InstanceSort) else s for s in sort]

        list_cls, resource_cls = self._get_classes(instance_type)

        return self._list(
            list_cls=list_cls,
            resource_cls=resource_cls,
            method="POST",
            limit=limit,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            other_params=other_params,
        )

    @staticmethod
    def _get_classes(
        instance_type: Literal["node", "edge"]
    ) -> tuple[Type[Node], Type[NodeList]] | tuple[Type[Edge], Type[EdgeList]]:
        if instance_type == "node":
            return Node, NodeList
        elif instance_type == "edge":
            return Edge, EdgeList
        raise ValueError(f"Unsupported {instance_type=}")

    @overload
    def apply(
        self,
        instance: Sequence[NodeApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeList:
        ...

    @overload
    def apply(
        self,
        instance: NodeApply,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> Node:
        ...

    @overload
    def apply(
        self,
        instance: Sequence[EdgeApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> EdgeList:
        ...

    @overload
    def apply(
        self,
        instance: EdgeApply,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> Edge:
        ...

    def apply(
        self,
        instance: NodeApply | EdgeApply | Sequence[NodeApply] | Sequence[EdgeApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> Node | NodeList | Edge | EdgeList:
        """`Add or update (upsert) instances. <https://docs.cognite.com/api/v1/#tag/Instances/operation/applyNodeAndEdges>`_
        Args:
            instance (instance: Instance | Sequence[Instance]): Instance or instances of instances to create or update.
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default,
                                            the start node of an edge must exist before it can be ingestested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default,
                                          the end node of an edge must exist before it can be ingestested.
            skip_on_version_conflict (bool): If existingVersion is specified on any of the nodes/edges in the input,
                                             the default behaviour is that the entire ingestion will fail when version
                                             conflicts occur. If skipOnVersionConflict is set to true, items with
                                             version conflicts will be skipped instead. If no version is specified for
                                             nodes/edges, it will do the writing directly.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing
                            values with the supplied values (true)? Or should we merge in new values for properties
                            together with the existing values (false)? Note: This setting applies for all nodes or
                            edges specified in the ingestion call.
        Returns:
            Instance | InstanceList: Created instance(s)
        Examples:
            Create new instances:
                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_classes.data_modeling as models
                >>> c = CogniteClient()
                >>> instances = []
                >>> res = c.data_modeling.instances.create(instances)
        """
        other_parameters = {
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
            "skipOnVersionConflict": skip_on_version_conflict,
            "replace": replace,
        }
        list_cls, resource_cls = self._get_classes(
            instance_type=instance[0].instance_type if isinstance(instance, list) else instance.instance_type
        )
        return self._create_multiple(
            list_cls=list_cls, resource_cls=resource_cls, items=instance, extra_body_fields=other_parameters
        )
