from __future__ import annotations

from typing import Iterator, Literal, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling.dsl_filter import DSLFilter, dump_dsl_filter
from cognite.client.data_classes.data_modeling.ids import InstanceId, TypedDataModelingId
from cognite.client.data_classes.data_modeling.instances import Instance, InstanceFilter, InstanceList, InstanceSort
from cognite.client.data_classes.data_modeling.shared import ViewReference
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class InstancesAPI(APIClient):
    _RESOURCE_PATH = "/models/instances"

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node", "edge"] = "node",
        sort: list[InstanceSort | dict] | None = None,
        filter: DSLFilter | dict | None = None,
    ) -> Iterator[Instance] | Iterator[InstanceList]:
        """Iterate over instances

        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.

        Args:
            chunk_size (int, optional): Number of data_models to return in each chunk. Defaults to yielding one data_model a time.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewReference]): Views to retrieve properties from.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to return all items.
            sort (list[InstanceSort]): How you want the listed instances information ordered.
            filter (dict | DSLFilter): Advanced filtering of instances.

        Yields:
            Instance | InstanceList: yields Instance one by one if chunk_size is not specified, else InstanceList objects.
        """
        other_params = InstanceFilter(include_typing, sources, instance_type).dump(camel_case=True)
        if sort:
            other_params["sort"] = [s.dump(camel_case=True) if isinstance(s, InstanceSort) else s for s in sort]
        return self._list_generator(
            list_cls=InstanceList,
            resource_cls=Instance,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
            filter=dump_dsl_filter(filter),  # type: ignore[arg-type]
            other_params=other_params,
        )

    def __iter__(self) -> Iterator[Instance]:
        """Iterate over instances

        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.

        Yields:
            Instance: yields Instances one by one.
        """
        return cast(Iterator[Instance], self())

    @overload
    def retrieve(
        self, ids: InstanceId, sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> Instance | None:
        ...

    @overload
    def retrieve(
        self, ids: Sequence[InstanceId], sources: list[ViewReference] | None = None, include_typing: bool = False
    ) -> InstanceList:
        ...

    def retrieve(
        self,
        ids: InstanceId | Sequence[InstanceId],
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
                >>> res = c.data_modeling.instances.retrieve(('node', 'myNode', 'mySpace'))

        """
        identifier = DataModelingIdentifierSequence.load(ids)
        return self._retrieve_multiple(list_cls=InstanceList, resource_cls=Instance, identifiers=identifier)

    def delete(self, id: InstanceId | Sequence[InstanceId]) -> list[TypedDataModelingId]:
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
            self._delete_multiple(
                identifiers=DataModelingIdentifierSequence.load(id), wrap_ids=True, returns_items=True
            ),
        )
        return [
            TypedDataModelingId(space=item["space"], external_id=item["externalId"], instance_type=item["instanceType"])
            for item in deleted_instances
        ]

    def list(
        self,
        include_typing: bool = False,
        sources: list[ViewReference] | None = None,
        instance_type: Literal["node", "edge"] = "node",
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | None = None,
        filter: DSLFilter | dict | None = None,
    ) -> InstanceList:
        """`List instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/advancedListInstance>`_

        Args:
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewReference]): Views to retrieve properties from.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.
            sort (list[InstanceSost]): How you want the listed instances information ordered.
            filter (dict | DSLFilter): Advnanced filtering of instances.

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

        return self._list(
            list_cls=InstanceList,
            resource_cls=Instance,
            method="POST",
            limit=limit,
            filter=dump_dsl_filter(filter),  # type: ignore[arg-type]
            other_params=other_params,
        )

    @overload
    def apply(
        self,
        instance: Sequence[Instance],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> InstanceList:
        ...

    @overload
    def apply(
        self,
        instance: Instance,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> Instance:
        ...

    def apply(
        self,
        instance: Instance | Sequence[Instance],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> Instance | InstanceList:
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
        return self._create_multiple(
            list_cls=InstanceList, resource_cls=Instance, items=instance, extra_body_fields=other_parameters
        )
