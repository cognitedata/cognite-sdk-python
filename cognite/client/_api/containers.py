from __future__ import annotations

from typing import Iterator, Optional, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes.containers import Container, ContainerList
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class ContainersAPI(APIClient):
    _RESOURCE_PATH = "/models/containers"

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
    ) -> Iterator[Container] | Iterator[ContainerList]:
        """Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Args:
            chunk_size (int, optional): Number of containers to return in each chunk. Defaults to yielding one container a time.
            limit (int, optional): Maximum number of containers to return. Default to return all items.

        Yields:
            Union[Container, ContainerList]: yields Container one by one if chunk_size is not specified, else ContainerList objects.
        """
        return self._list_generator(
            list_cls=ContainerList,
            resource_cls=Container,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        )

    def __iter__(self) -> Iterator[Container]:
        """Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Yields:
            Container: yields Containers one by one.
        """
        return cast(Iterator[Container], self())

    def retrieve(self, space: str, external_id: str) -> Optional[Container]:
        """`Retrieve a single container by id. <https://docs.cognite.com/api/v1/#tag/Containers/operation/byContainerIdsContainers>`_

        Args:
            space (str): Workspace for container
            external_id (str): Container ID.

        Returns:
            Optional[Container]: Requested container or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.containers.retrieve(container='myContainer')

        """
        identifier = DataModelingIdentifierSequence.load(external_id, space).as_singleton()
        return self._retrieve_multiple(list_cls=ContainerList, resource_cls=Container, identifiers=identifier)

    def retrieve_multiple(
        self,
        space: str,
        external_ids: Sequence[str],
    ) -> ContainerList:
        """`Retrieve multiple containers by id. <https://docs.cognite.com/api/v1/#tag/Containers/operation/byContainerIdsContainers>`_

        Args:
            space (str): Workspace for containers
            external_ids (Sequence[str]): Container IDs.

        Returns:
            ContainerList: The requested containers.

        Examples:

            Get containers by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.containers.retrieve_multiple(containers=["MyContainer", "MyAwesomeContainer", "MyOtherContainer"])

        """
        identifiers = DataModelingIdentifierSequence.load(external_ids, space)
        return self._retrieve_multiple(list_cls=ContainerList, resource_cls=Container, identifiers=identifiers)

    def delete(
        self,
        space: str,
        external_id: str | Sequence[str],
    ) -> str | list[str] | None:
        """`Delete one or more containers <https://docs.cognite.com/api/v1/#tag/Containers/operation/deleteContainersV3>`_

        Args:
            space (str): Workspace for container
            external_id (str): Container ID or IDs.
        Returns:
            The container(s) which has been deleted. None if nothing was deleted.
        Examples:

            Delete containers by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.containers.delete(container=["myContainer", "myOtherContainer"])
        """
        deleted_containers = self._delete_multiple(
            identifiers=DataModelingIdentifierSequence.load(external_id, space),
            wrap_ids=True,
        )
        if not deleted_containers:
            return None
        elif len(deleted_containers) == 1:
            return deleted_containers[0].container
        else:
            return [s.container for s in deleted_containers]

    def list(
        self,
        limit: int = LIST_LIMIT_DEFAULT,
    ) -> ContainerList:
        """`List containers <https://docs.cognite.com/api/v1/#tag/Containers/operation/listContainersV3>`_

        Args:
            limit (int, optional): Maximum number of containers to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ContainerList: List of requested containers

        Examples:

            List containers and filter on max start time::

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
                >>> for container_list in c.data_modeling.containers(chunk_size=2500):
                ...     container_list # do something with the containers
        """
        return self._list(
            list_cls=ContainerList,
            resource_cls=Container,
            method="GET",
            limit=limit,
        )

    @overload
    def apply(self, container: Sequence[Container]) -> ContainerList:
        ...

    @overload
    def apply(self, container: Container) -> Container:
        ...

    def apply(self, container: Container | Sequence[Container]) -> Container | ContainerList:
        """`Create or patch one or more containers. <https://docs.cognite.com/api/v1/#tag/Containers/operation/ApplyContainers>`_

        Args:
            container (container: Container | Sequence[Container]): Container or containers of containersda to create or update.

        Returns:
            Container | ContainerList: Created container(s)

        Examples:

            Create new containersda::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.containers import Container
                >>> c = CogniteClient()
                >>> containers = [Container(container="myContainer", description="My first container", name="My Container"),
                ... Container(container="myOtherContainer", description="My second container", name="My Other Container")]
                >>> res = c.data_modeling.containers.create(containers)
        """
        return self._create_multiple(list_cls=ContainerList, resource_cls=Container, items=container)
