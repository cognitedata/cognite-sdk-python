from __future__ import annotations

from typing import Iterator, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import CONTAINER_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling.containers import Container, ContainerFilter, ContainerList
from cognite.client.data_classes.data_modeling.ids import ContainerId, DataModelingId
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class ContainersAPI(APIClient):
    _RESOURCE_PATH = "/models/containers"

    def __call__(
        self,
        space: str | None = None,
        include_global: bool = False,
        chunk_size: int = None,
        limit: int = None,
    ) -> Iterator[Container] | Iterator[ContainerList]:
        """Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Args:
            space (int, optional): The space to query.
            Whether the global containers should be returned.
            chunk_size (int, optional): Number of containers to return in each chunk. Defaults to yielding one container a time.
            limit (int, optional): Maximum number of containers to return. Default to return all items.

        Yields:
            Container | ContainerList: yields Container one by one if chunk_size is not specified, else ContainerList objects.
        """
        filter = ContainerFilter(space, include_global)
        return self._list_generator(
            list_cls=ContainerList,
            resource_cls=Container,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter.dump(camel_case=True),
        )

    def __iter__(self) -> Iterator[Container]:
        """Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Yields:
            Container: yields Containers one by one.
        """
        return cast(Iterator[Container], self())

    @overload
    def retrieve(self, ids: ContainerId) -> Container | None:
        ...

    @overload
    def retrieve(self, ids: Sequence[ContainerId]) -> ContainerList:
        ...

    def retrieve(self, ids: ContainerId | Sequence[ContainerId]) -> Container | ContainerList | None:
        """`Retrieve a single container by id. <https://docs.cognite.com/api/v1/#tag/Containers/operation/byExternalIdsContainers>`_

        Args:
            ids (ContainerId | Sequence[ContainerId]): Identifier for container

        Returns:
            Optional[Container]: Requested container or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.containers.retrieve(('mySpace', 'myContainer'))

        """
        identifier = DataModelingIdentifierSequence.load(ids)
        return self._retrieve_multiple(list_cls=ContainerList, resource_cls=Container, identifiers=identifier)

    def delete(self, id: ContainerId | Sequence[ContainerId]) -> list[DataModelingId]:
        """`Delete one or more containers <https://docs.cognite.com/api/v1/#tag/Containers/operation/deleteContainers>`_

        Args:
            id (ContainerId | Sequence[ContainerId): The container identifier(s).
        Returns:
            The container(s) which has been deleted. Empty list if nothing was deleted.
        Examples:

            Delete containers by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.containers.delete(("mySpace", "myContainer"))
        """
        deleted_containers = cast(
            list,
            self._delete_multiple(
                identifiers=DataModelingIdentifierSequence.load(id), wrap_ids=True, returns_items=True
            ),
        )
        return [DataModelingId(space=item["space"], external_id=item["externalId"]) for item in deleted_containers]

    def list(
        self, space: str | None = None, limit: int = CONTAINER_LIST_LIMIT_DEFAULT, include_global: bool = False
    ) -> ContainerList:
        """`List containers <https://docs.cognite.com/api/v1/#tag/Containers/operation/listContainers>`_

        Args:
            space (int, optional): The space to query
            limit (int, optional): Maximum number of containers to return. Default to 10. Set to -1, float("inf") or None
                to return all items.
            include_global (bool, optional): Whether the global containers should be returned.

        Returns:
            ContainerList: List of requested containers

        Examples:

            List containers and limit to 5:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> container_list = c.data_modeling.containers.list(limit=5)

            Iterate over containers::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for container in c.data_modeling.containers:
                ...     container # do something with the container

            Iterate over chunks of containers to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for container_list in c.data_modeling.containers(chunk_size=10):
                ...     container_list # do something with the containers
        """
        filter = ContainerFilter(space, include_global)

        return self._list(
            list_cls=ContainerList,
            resource_cls=Container,
            method="GET",
            limit=limit,
            filter=filter.dump(camel_case=True),
        )

    @overload
    def apply(self, container: Sequence[Container]) -> ContainerList:
        ...

    @overload
    def apply(self, container: Container) -> Container:
        ...

    def apply(self, container: Container | Sequence[Container]) -> Container | ContainerList:
        """`Add or update (upsert) containers. <https://docs.cognite.com/api/v1/#tag/Containers/operation/ApplyContainers>`_

        Args:
            container (container: Container | Sequence[Container]): Container or containers of containers to create or update.

        Returns:
            Container | ContainerList: Created container(s)

        Examples:

            Create new containersd:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import Container
                >>> c = CogniteClient()
                >>> containers = [Container(container="myContainer", description="My first container",
                ... name="My Container", used_for="node"),
                ... Container(container="myOtherContainer", description="My second container",
                ... name="My Other Container", used_for="node")]
                >>> res = c.data_modeling.containers.create(containers)
        """
        return self._create_multiple(list_cls=ContainerList, resource_cls=Container, items=container)
