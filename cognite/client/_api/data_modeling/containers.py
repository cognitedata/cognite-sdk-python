from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.containers import (
    Container,
    ContainerApply,
    ContainerFilter,
    ContainerList,
)
from cognite.client.data_classes.data_modeling.ids import (
    ConstraintIdentifier,
    ContainerId,
    ContainerIdentifier,
    IndexIdentifier,
    _load_identifier,
)
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class ContainersAPI(APIClient):
    _RESOURCE_PATH = "/models/containers"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._CREATE_LIMIT = 100

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[Container]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[ContainerList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[Container] | Iterator[ContainerList]:
        """Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Args:
            chunk_size (int | None): Number of containers to return in each chunk. Defaults to yielding one container a time.
            space (str | None): The space to query.
            include_global (bool): Whether the global containers should be returned.
            limit (int | None): Maximum number of containers to return. Defaults to returning all items.

        Returns:
            Iterator[Container] | Iterator[ContainerList]: yields Container one by one if chunk_size is not specified, else ContainerList objects.
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

        Returns:
            Iterator[Container]: yields Containers one by one.
        """
        return self()

    @overload
    def retrieve(self, ids: ContainerIdentifier) -> Container | None: ...

    @overload
    def retrieve(self, ids: Sequence[ContainerIdentifier]) -> ContainerList: ...

    def retrieve(self, ids: ContainerIdentifier | Sequence[ContainerIdentifier]) -> Container | ContainerList | None:
        """`Retrieve one or more container by id(s). <https://developer.cognite.com/api#tag/Containers/operation/byExternalIdsContainers>`_

        Args:
            ids (ContainerIdentifier | Sequence[ContainerIdentifier]): Identifier for container(s).

        Returns:
            Container | ContainerList | None: Requested container or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.containers.retrieve(('mySpace', 'myContainer'))

            Fetch using the ContainerId::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ContainerId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.containers.retrieve(ContainerId(space='mySpace', external_id='myContainer'))
        """
        identifier = _load_identifier(ids, "container")
        return self._retrieve_multiple(
            list_cls=ContainerList,
            resource_cls=Container,
            identifiers=identifier,
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )

    def delete(self, ids: ContainerIdentifier | Sequence[ContainerIdentifier]) -> list[ContainerId]:
        """`Delete one or more containers <https://developer.cognite.com/api#tag/Containers/operation/deleteContainers>`_

        Args:
            ids (ContainerIdentifier | Sequence[ContainerIdentifier]): The container identifier(s).
        Returns:
            list[ContainerId]: The container(s) which has been deleted. Empty list if nothing was deleted.
        Examples:

            Delete containers by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.containers.delete(("mySpace", "myContainer"))
        """
        deleted_containers = cast(
            list,
            self._delete_multiple(
                identifiers=_load_identifier(ids, "container"),
                wrap_ids=True,
                returns_items=True,
                executor=ConcurrencySettings.get_data_modeling_executor(),
            ),
        )
        return [ContainerId(space=item["space"], external_id=item["externalId"]) for item in deleted_containers]

    def delete_constraints(self, ids: Sequence[ConstraintIdentifier]) -> list[ConstraintIdentifier]:
        """`Delete one or more constraints <https://developer.cognite.com/api#tag/Containers/operation/deleteContainerConstraints>`_

        Args:
            ids (Sequence[ConstraintIdentifier]): The constraint identifier(s).
        Returns:
            list[ConstraintIdentifier]: The constraints(s) which have been deleted.
        Examples:

            Delete constraints by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.containers.delete_constraints(
                ...     [(ContainerId("mySpace", "myContainer"), "myConstraint")]
                ... )
        """
        return self._delete_constraints_or_indexes(ids, "constraints")

    def delete_indexes(self, ids: Sequence[IndexIdentifier]) -> list[IndexIdentifier]:
        """`Delete one or more indexes <https://developer.cognite.com/api#tag/Containers/operation/deleteContainerIndexes>`_

        Args:
            ids (Sequence[IndexIdentifier]): The index identifier(s).
        Returns:
            list[IndexIdentifier]: The indexes(s) which has been deleted.
        Examples:

            Delete indexes by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.containers.delete_indexes(
                ...     [(ContainerId("mySpace", "myContainer"), "myIndex")]
                ... )
        """
        return self._delete_constraints_or_indexes(ids, "indexes")

    def _delete_constraints_or_indexes(
        self,
        ids: Sequence[ConstraintIdentifier] | Sequence[IndexIdentifier],
        constraint_or_index: Literal["constraints", "indexes"],
    ) -> list[tuple[ContainerId, str]]:
        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/{constraint_or_index}/delete",
            json={
                "items": [
                    {
                        "space": constraint_id[0].space,
                        "containerExternalId": constraint_id[0].external_id,
                        "identifier": constraint_id[1],
                    }
                    for constraint_id in ids
                ]
            },
        )
        return [
            (
                ContainerId(space=item["space"], external_id=item["containerExternalId"]),
                item["identifier"],
            )
            for item in res.json()["items"]
        ]

    def list(
        self,
        space: str | None = None,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        include_global: bool = False,
    ) -> ContainerList:
        """`List containers <https://developer.cognite.com/api#tag/Containers/operation/listContainers>`_

        Args:
            space (str | None): The space to query
            limit (int | None): Maximum number of containers to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            include_global (bool): Whether the global containers should be returned.

        Returns:
            ContainerList: List of requested containers

        Examples:

            List containers and limit to 5:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> container_list = client.data_modeling.containers.list(limit=5)

            Iterate over containers::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for container in client.data_modeling.containers:
                ...     container # do something with the container

            Iterate over chunks of containers to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for container_list in client.data_modeling.containers(chunk_size=10):
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
    def apply(self, container: Sequence[ContainerApply]) -> ContainerList: ...

    @overload
    def apply(self, container: ContainerApply) -> Container: ...

    def apply(self, container: ContainerApply | Sequence[ContainerApply]) -> Container | ContainerList:
        """`Add or update (upsert) containers. <https://developer.cognite.com/api#tag/Containers/operation/ApplyContainers>`_

        Args:
            container (ContainerApply | Sequence[ContainerApply]): Container(s) to create or update.

        Returns:
            Container | ContainerList: Created container(s)

        Examples:

            Create new containers:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ContainerApply, ContainerProperty, Text
                >>> client = CogniteClient()
                >>> container = [ContainerApply(space="mySpace", external_id="myContainer",
                ...     properties={"name": ContainerProperty(type=Text(), name="name")})]
                >>> res = client.data_modeling.containers.apply(container)

            Create new container with unit-aware properties:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ContainerApply, ContainerProperty, Float64
                >>> from cognite.client.data_classes.data_modeling.data_types import UnitReference
                >>> client = CogniteClient()
                >>> container = ContainerApply(
                ...     space="mySpace",
                ...     external_id="myContainer",
                ...     properties={
                ...         "maxPressure": ContainerProperty(
                ...             nullable=True,
                ...             description="Maximum Pump Pressure",
                ...             name="maxPressure",
                ...             type=Float64(
                ...                 unit=UnitReference(
                ...                     external_id="pressure:bar",
                ...                     source_unit="BAR"
                ...                 )
                ...             )
                ...         ),
                ...         "rotationConfigurations": ContainerProperty(
                ...             nullable=True,
                ...             description="Rotation Configurations",
                ...             name="rotationConfigurations",
                ...             type=Float64(
                ...                 is_list=True,
                ...                 unit=UnitReference(
                ...                     external_id="angular_velocity:rev-per-min"
                ...                 )
                ...             )
                ...         )
                ...     }
                ... )
                >>> res = client.data_modeling.containers.apply(container)
        """
        return self._create_multiple(
            list_cls=ContainerList,
            resource_cls=Container,
            items=container,
            input_resource_cls=ContainerApply,
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )
