from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterator, List, Literal, Sequence, Union, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import (
    EdgeId,
    NodeId,
    ViewId,
    ViewIdentifier,
    _load_identifier,
)
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeApplyResultList,
    EdgeList,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeApplyResultList,
    NodeList,
)
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.utils._identifier import DataModelingIdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class _NodeOrEdgeList(CogniteResourceList):
    _RESOURCE = (Node, Edge)  # type: ignore[assignment]

    @classmethod
    def _load(cls, resource_list: list[dict[str, Any]] | str, cognite_client: CogniteClient = None) -> _NodeOrEdgeList:
        resource_list = json.loads(resource_list) if isinstance(resource_list, str) else resource_list
        resources: list[Node | Edge] = [
            Node.load(data) if data["instanceType"] == "node" else Edge.load(data) for data in resource_list
        ]
        return cls(resources, None)

    def as_ids(self) -> list[NodeId | EdgeId]:
        return [instance.as_id() for instance in self]


class _NodeOrEdgeResourceAdapter:
    @classmethod
    def _load(cls, data: str | dict, cognite_client: CogniteClient = None) -> Node | Edge:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return Node.load(data)
        return Edge.load(data)


class _NodeOrEdgeApplyResultList(CogniteResourceList):
    _RESOURCE = (NodeApplyResult, EdgeApplyResult)  # type: ignore[assignment]

    @classmethod
    def _load(
        cls, resource_list: list[dict[str, Any]] | str, cognite_client: CogniteClient = None
    ) -> _NodeOrEdgeApplyResultList:
        resource_list = json.loads(resource_list) if isinstance(resource_list, str) else resource_list
        resources: list[NodeApplyResult | EdgeApplyResult] = [
            NodeApplyResult.load(data) if data["instanceType"] == "node" else EdgeApplyResult.load(data)
            for data in resource_list
        ]
        return cls(resources, None)

    def as_ids(self) -> list[NodeId | EdgeId]:
        return [result.as_id() for result in self]


class _NodeOrEdgeApplyResultAdapter:
    @classmethod
    def _load(cls, data: str | dict, cognite_client: CogniteClient = None) -> NodeApplyResult | EdgeApplyResult:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return NodeApplyResult.load(data)
        return EdgeApplyResult.load(data)


class _NodeOrEdgeApplyAdapter:
    @classmethod
    def _load(cls, data: str | dict, cognite_client: CogniteClient = None) -> NodeApply | EdgeApply:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return NodeApply.load(data)
        return EdgeApply.load(data)


class InstancesAPI(APIClient):
    _RESOURCE_PATH = "/models/instances"

    @overload
    def __call__(
        self,
        chunk_size: None,
        instance_type: Literal["node"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Node]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: None,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["node"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[NodeList]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[EdgeList]:
        ...

    def __call__(
        self,
        chunk_size: int | None = None,
        instance_type: Literal["node", "edge"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge] | Iterator[EdgeList] | Iterator[Node] | Iterator[NodeList]:
        """Iterate over nodes or edges.
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.

        Args:
            chunk_size (int, optional): Number of data_models to return in each chunk. Defaults to yielding
                                        one instance at a time.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to return all items.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewId] | ViewId): Views to retrieve properties from.
            sort (list[InstanceSort | dict] | InstanceSort | dict): How you want the listed instances information ordered.
            filter (dict | Filter): Advanced filtering of instances.

        Yields:
            Edge | Node | EdgeList | NodeList: yields Instance one by one if chunk_size is not specified, else NodeList/EdgeList objects.
        """
        other_params = self._create_other_params(
            include_typing=include_typing, instance_type=instance_type, sort=sort, sources=sources
        )

        if instance_type == "node":
            resource_cls: type = _NodeOrEdgeResourceAdapter
            list_cls: type = NodeList
        elif instance_type == "edge":
            resource_cls, list_cls = _NodeOrEdgeResourceAdapter, EdgeList
        else:
            raise ValueError(f"Invalid instance type: {instance_type}")

        return cast(
            Union[Iterator[Edge], Iterator[EdgeList], Iterator[Node], Iterator[NodeList]],
            self._list_generator(
                list_cls=list_cls,
                resource_cls=resource_cls,
                method="POST",
                chunk_size=chunk_size,
                limit=limit,
                filter=filter.dump() if isinstance(filter, Filter) else filter,
                other_params=other_params,
            ),
        )

    def __iter__(self) -> Iterator[Node]:
        """Iterate over instances
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.
        Yields:
            Instance: yields Instances one by one.
        """
        return cast(Iterator[Node], self(None, "node"))

    def retrieve(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        include_typing: bool = False,
    ) -> InstancesResult:
        """`Retrieve one or more instance by id(s). <https://docs.cognite.com/api/v1/#tag/Instances/operation/byExternalIdsInstances>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids
            sources (ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence(View) None): Retrieve properties from the listed - by reference - views.
            include_typing (bool): Whether to return property type information as part of the result.

        Returns:
            InstancesResult: Requested instances.

        Examples:

            Retrieve instances by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(nodes=("mySpace", "myNodeExternalId"),
                ...                                          edges=("mySpace", "myEdgeExternalId"),
                ...                                          sources=("mySpace", "myViewExternalId", "myViewVersion")
                ...                                         )

            Retrieve nodes an edges using the built in data class

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(dm.NodeId("mySpace", "myNode"),
                ...                                          dm.EdgeId("mySpace", "myEdge"),
                ...                                          dm.ViewId("mySpace", "myViewExternalId", "myViewVersion")
                ...                                         )

            Retrieve nodes an edges using the the view object as source

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(dm.NodeId("mySpace", "myNode"),
                ...                                          dm.EdgeId("mySpace", "myEdge"),
                ...                                          sources='myView')
                ...                                         )
        """
        identifiers = self._load_node_and_edge_ids(nodes, edges)
        other_params = self._create_other_params(
            include_typing=include_typing,
            sources=sources,
            sort=None,
            instance_type=None,
        )

        res = self._retrieve_multiple(
            list_cls=_NodeOrEdgeList,
            resource_cls=_NodeOrEdgeResourceAdapter,  # type: ignore[type-var]
            identifiers=identifiers,
            other_params=other_params,
        )

        return InstancesResult(
            nodes=NodeList([node for node in res if isinstance(node, Node)]),
            edges=EdgeList([edge for edge in res if isinstance(edge, Edge)]),
        )

    def _load_node_and_edge_ids(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None,
    ) -> DataModelingIdentifierSequence:
        if isinstance(nodes, NodeId) or (isinstance(nodes, tuple) and isinstance(nodes[0], str)):
            nodes_seq: Sequence[NodeId | tuple[str, str]] = [nodes]  # type: ignore[list-item]
        else:
            nodes_seq = nodes  # type: ignore[assignment]

        if isinstance(edges, EdgeId) or (isinstance(edges, tuple) and isinstance(edges[0], str)):
            edges_seq: Sequence[EdgeId | tuple[str, str]] = [edges]  # type: ignore[list-item]
        else:
            edges_seq = edges  # type: ignore[assignment]

        identifiers = []
        if nodes_seq:
            node_ids = _load_identifier(nodes_seq, "node")
            identifiers.extend(node_ids._identifiers)
        if edges_seq:
            edge_ids = _load_identifier(edges_seq, "edge")
            identifiers.extend(edge_ids._identifiers)

        return DataModelingIdentifierSequence(identifiers, is_singleton=False)

    def delete(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> InstancesDeleteResult:
        """`Delete one or more instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/deleteBulk>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids

        Returns:
            The instance(s) which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete instances by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(nodes=("myNode", "mySpace"))

            Delete nodes and edges using the built in data class

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(dm.NodeId('mySpace', 'myNode'), dm.EdgeId('mySpace', 'myEdge'))
        """
        identifiers = self._load_node_and_edge_ids(nodes, edges)
        deleted_instances = cast(List, self._delete_multiple(identifiers, wrap_ids=True, returns_items=True))
        node_ids = [NodeId.load(item) for item in deleted_instances if item["instanceType"] == "node"]
        edge_ids = [EdgeId.load(item) for item in deleted_instances if item["instanceType"] == "edge"]
        return InstancesDeleteResult(node_ids, edge_ids)

    @classmethod
    def _create_other_params(
        cls,
        *,
        include_typing: bool,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None,
        instance_type: Literal["node", "edge"] | None,
    ) -> dict[str, Any]:
        other_params: dict[str, Any] = {"includeTyping": include_typing}
        if sources:
            other_params["sources"] = (
                [cls._dump_instance_source(source) for source in sources]
                if isinstance(sources, Sequence)
                else [cls._dump_instance_source(sources)]
            )
        if sort:
            if isinstance(sort, (InstanceSort, dict)):
                other_params["sort"] = [cls._dump_instance_sort(sort)]
            else:
                other_params["sort"] = [cls._dump_instance_sort(s) for s in sort]
        if instance_type:
            other_params["instanceType"] = instance_type
        return other_params

    @classmethod
    def _dump_instance_source(cls, source: ViewIdentifier | View) -> dict:
        instance_source: ViewIdentifier
        if isinstance(source, View):
            instance_source = source.as_id()
        else:
            instance_source = source
        return {"source": ViewId.load(instance_source).dump(camel_case=True)}

    @classmethod
    def _dump_instance_sort(cls, sort: InstanceSort | dict) -> dict:
        return sort.dump(camel_case=True) if isinstance(sort, InstanceSort) else sort

    def apply(
        self,
        nodes: NodeApply | Sequence[NodeApply] | None = None,
        edges: EdgeApply | Sequence[EdgeApply] | None = None,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        auto_create_direct_relations: bool = True,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> InstancesApplyResult:
        """`Add or update (upsert) instances. <https://docs.cognite.com/api/v1/#tag/Instances/operation/applyNodeAndEdges>`_

        Args:
            nodes (NodeApply | Sequence[NodeApply] | None = None): Nodes to apply
            edges (EdgeApply | Sequence[EdgeApply] | None = None): Edges to apply
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default,
                                            the start node of an edge must exist before it can be ingestested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default,
                                          the end node of an edge must exist before it can be ingestested.
            auto_create_direct_relations (bool): Whether to create missing direct relation targets when ingesting.
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
            InstancesApplyResult: Created instance(s)

        Examples:

            Create new node without data:

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> nodes = [dm.ApplyNode("mySpace", "myNodeId")]
                >>> res = c.data_modeling.instances.apply(nodes)

            Create two nodes with data with an one to many edge, and a one to one edge

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> person = dm.NodeApply("mySpace", "person:arnold_schwarzenegger", sources=[
                ...                        dm.NodeOrEdgeData(
                ...                               dm.ViewId("mySpace", "PersonView", "v1"),
                ...                               {"name": "Arnold Schwarzenegger", "birthYear": 1947})
                ... ])
                >>> actor = dm.NodeApply("mySpace", "actor:arnold_schwarzenegger", sources=[
                ...                        dm.NodeOrEdgeData(
                ...                               dm.ViewId("mySpace", "ActorView", "v1"),
                ...                               {"wonOscar": False,
                ...                               # This is a one-to-one edge from actor to person
                ...                                "person": {"space": "mySpace", "externalId": "person:arnold_schwarzenegger"}})
                ... ])
                >>> # This is one to many edge, in this case from Person to role
                >>> # (a person can have multiple roles, in this model for example Actor and Director)
                >>> person_to_actor = dm.EdgeApply(space="mySpace",
                ...                                       external_id="relation:arnold_schwarzenegger:actor",
                ...                                       type="Person.roles",
                ...                                       start_node="person:arnold_schwarzenegger",
                ...                                       end_node="actor:arnold_schwarzenegger",
                ... )
                >>> res = c.data_modeling.instances.apply([person, actor], [person_to_actor])

            Create new edge an automatically create end nodes.

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> edge = dm.EdgeApply(space="mySpace",
                ...                            external_id="relation:sylvester_stallone:actor",
                ...                            type="Person.roles",
                ...                            start_node="person:sylvester_stallone",
                ...                            end_node="actor:sylvester_stallone",
                ... )
                >>> res = c.data_modeling.instances.apply(edges=edge, auto_create_start_nodes=True, auto_create_end_nodes=True)

        """
        other_parameters = {
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
            "autoCreateDirectRelations": auto_create_direct_relations,
            "skipOnVersionConflict": skip_on_version_conflict,
            "replace": replace,
        }
        nodes = nodes or []
        nodes = nodes if isinstance(nodes, Sequence) else [nodes]

        edges = edges or []
        edges = edges if isinstance(edges, Sequence) else [edges]

        res = self._create_multiple(
            items=(*nodes, *edges),
            list_cls=_NodeOrEdgeApplyResultList,
            resource_cls=_NodeOrEdgeApplyResultAdapter,  # type: ignore[type-var]
            extra_body_fields=other_parameters,
            input_resource_cls=_NodeOrEdgeApplyAdapter,  # type: ignore[arg-type]
        )
        return InstancesApplyResult(
            nodes=NodeApplyResultList([item for item in res if isinstance(item, NodeApplyResult)]),
            edges=EdgeApplyResultList([item for item in res if isinstance(item, EdgeApplyResult)]),
        )

    @overload
    def list(
        self,
        instance_type: Literal["node"] = "node",
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList:
        ...

    @overload
    def list(
        self,
        instance_type: Literal["edge"],
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> EdgeList:
        ...

    def list(
        self,
        instance_type: Literal["node", "edge"] = "node",
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList | EdgeList:
        """`List instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/advancedListInstance>`_

        Args:
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence(View) | None): Views to retrieve properties from.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.
            sort (list[InstanceSost] | InstanceSort | dict): How you want the listed instances information ordered.
            filter (dict | Filter): Advanced filtering of instances.

        Returns:
            Union[EdgeList, NodeList]: List of requested instances

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
        other_params = self._create_other_params(
            include_typing=include_typing, instance_type=instance_type, sort=sort, sources=sources
        )

        if instance_type == "node":
            resource_cls: type = _NodeOrEdgeResourceAdapter
            list_cls: type = NodeList
        elif instance_type == "edge":
            resource_cls, list_cls = _NodeOrEdgeResourceAdapter, EdgeList
        else:
            raise ValueError(f"Invalid instance type: {instance_type}")

        return cast(
            Union[NodeList, EdgeList],
            self._list(
                list_cls=list_cls,
                resource_cls=resource_cls,
                method="POST",
                limit=limit,
                filter=filter.dump() if isinstance(filter, Filter) else filter,
                other_params=other_params,
            ),
        )
