"""
===============================================================================
8a40bb13bf895e182628f40657186557
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.containers import (
    Container,
    ContainerApply,
    ContainerList,
)
from cognite.client.data_classes.data_modeling.ids import (
    ConstraintIdentifier,
    ContainerId,
    ContainerIdentifier,
    IndexIdentifier,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncContainersAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None) -> Iterator[Container]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[ContainerList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[Container | ContainerList]:
        """
        Iterate over containers

        Fetches containers as they are iterated over, so you keep a limited number of containers in memory.

        Args:
            chunk_size (int | None): Number of containers to return in each chunk. Defaults to yielding one container a time.
            space (str | None): The space to query.
            include_global (bool): Whether the global containers should be returned.
            limit (int | None): Maximum number of containers to return. Defaults to returning all items.

        Yields:
            Container | ContainerList: yields Container one by one if chunk_size is not specified, else ContainerList objects.
        """
        yield from SyncIterator(
            self.__async_client.data_modeling.containers(
                chunk_size=chunk_size, space=space, include_global=include_global, limit=limit
            )
        )

    @overload
    def retrieve(self, ids: ContainerIdentifier) -> Container | None: ...

    @overload
    def retrieve(self, ids: Sequence[ContainerIdentifier]) -> ContainerList: ...

    def retrieve(self, ids: ContainerIdentifier | Sequence[ContainerIdentifier]) -> Container | ContainerList | None:
        """
        `Retrieve one or more container by id(s). <https://developer.cognite.com/api#tag/Containers/operation/byExternalIdsContainers>`_

        Args:
            ids (ContainerIdentifier | Sequence[ContainerIdentifier]): Identifier for container(s).

        Returns:
            Container | ContainerList | None: Requested container or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.containers.retrieve(('mySpace', 'myContainer'))

            Fetch using the ContainerId:

                >>> from cognite.client.data_classes.data_modeling import ContainerId
                >>> res = client.data_modeling.containers.retrieve(
                ...     ContainerId(space='mySpace', external_id='myContainer'))
        """
        return run_sync(self.__async_client.data_modeling.containers.retrieve(ids=ids))

    def delete(self, ids: ContainerIdentifier | Sequence[ContainerIdentifier]) -> list[ContainerId]:
        """
        `Delete one or more containers <https://developer.cognite.com/api#tag/Containers/operation/deleteContainers>`_

        Args:
            ids (ContainerIdentifier | Sequence[ContainerIdentifier]): The container identifier(s).
        Returns:
            list[ContainerId]: The container(s) which has been deleted. Empty list if nothing was deleted.
        Examples:

            Delete containers by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.containers.delete(("mySpace", "myContainer"))
        """
        return run_sync(self.__async_client.data_modeling.containers.delete(ids=ids))

    def delete_constraints(self, ids: Sequence[ConstraintIdentifier]) -> list[ConstraintIdentifier]:
        """
        `Delete one or more constraints <https://developer.cognite.com/api#tag/Containers/operation/deleteContainerConstraints>`_

        Args:
            ids (Sequence[ConstraintIdentifier]): The constraint identifier(s).
        Returns:
            list[ConstraintIdentifier]: The constraints(s) which have been deleted.
        Examples:

            Delete constraints by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.containers.delete_constraints(
                ...     [(ContainerId("mySpace", "myContainer"), "myConstraint")]
                ... )
        """
        return run_sync(self.__async_client.data_modeling.containers.delete_constraints(ids=ids))

    def delete_indexes(self, ids: Sequence[IndexIdentifier]) -> list[IndexIdentifier]:
        """
        `Delete one or more indexes <https://developer.cognite.com/api#tag/Containers/operation/deleteContainerIndexes>`_

        Args:
            ids (Sequence[IndexIdentifier]): The index identifier(s).
        Returns:
            list[IndexIdentifier]: The indexes(s) which has been deleted.
        Examples:

            Delete indexes by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.containers.delete_indexes(
                ...     [(ContainerId("mySpace", "myContainer"), "myIndex")]
                ... )
        """
        return run_sync(self.__async_client.data_modeling.containers.delete_indexes(ids=ids))

    def list(
        self,
        space: str | None = None,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        include_global: bool = False,
    ) -> ContainerList:
        """
        `List containers <https://developer.cognite.com/api#tag/Containers/operation/listContainers>`_

        Args:
            space (str | None): The space to query
            limit (int | None): Maximum number of containers to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            include_global (bool): Whether the global containers should be returned.

        Returns:
            ContainerList: List of requested containers

        Examples:

            List containers and limit to 5:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> container_list = client.data_modeling.containers.list(limit=5)

            Iterate over containers, one-by-one:

                >>> for container in client.data_modeling.containers():
                ...     container  # do something with the container

            Iterate over chunks of containers to reduce memory load:

                >>> for container_list in client.data_modeling.containers(chunk_size=10):
                ...     container_list # do something with the containers
        """
        return run_sync(
            self.__async_client.data_modeling.containers.list(space=space, limit=limit, include_global=include_global)
        )

    @overload
    def apply(self, container: Sequence[ContainerApply]) -> ContainerList: ...

    @overload
    def apply(self, container: ContainerApply) -> Container: ...

    def apply(self, container: ContainerApply | Sequence[ContainerApply]) -> Container | ContainerList:
        """
        `Add or update (upsert) containers. <https://developer.cognite.com/api#tag/Containers/operation/ApplyContainers>`_

        Args:
            container (ContainerApply | Sequence[ContainerApply]): Container(s) to create or update.

        Returns:
            Container | ContainerList: Created container(s)

        Examples:

            Create a new container:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import (
                ...     ContainerApply, ContainerProperty, Text, Float64)
                >>> client = CogniteClient()
                >>> container = ContainerApply(
                ...     space="mySpace",
                ...     external_id="myContainer",
                ...     properties={
                ...         "name": ContainerProperty(type=Text, name="name"),
                ...         "numbers": ContainerProperty(
                ...             type=Float64(is_list=True, max_list_size=200),
                ...             description="very important numbers",
                ...         ),
                ...     },
                ... ),
                >>> res = client.data_modeling.containers.apply(container)

            Create new container with unit-aware properties:

                >>> from cognite.client.data_classes.data_modeling import Float64
                >>> from cognite.client.data_classes.data_modeling.data_types import UnitReference
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

            Example container with all available properties (for illustration purposes). Note that
            ``ContainerProperty`` has several options not shown here, like ``name``, ``description``,
            ``nullable``, ``auto_increment``, ``default_value`` and ``immutable`` that may be specified,
            depending on the choice of property type (e.g. ``auto_increment`` only works with integer types).

                >>> from cognite.client.data_classes.data_modeling.data_types import UnitReference, EnumValue
                >>> from cognite.client.data_classes.data_modeling.data_types import (
                ...     Boolean, Date, DirectRelation, Enum, FileReference, Float32, Float64,
                ...     Int32, Int64, Json, SequenceReference, Text, TimeSeriesReference, Timestamp
                ... )
                >>> container_properties = {
                ...     "prop01": ContainerProperty(Boolean),
                ...     "prop02": ContainerProperty(Boolean(is_list=True)),
                ...     "prop03": ContainerProperty(Date),
                ...     "prop04": ContainerProperty(Date(is_list=True)),
                ...     "prop05": ContainerProperty(Timestamp),
                ...     "prop06": ContainerProperty(Timestamp(is_list=True)),
                ...     "prop07": ContainerProperty(Text),
                ...     "prop08": ContainerProperty(Text(is_list=True)),
                ...     # Note: DirectRelation(list) support `container`: The (optional) required type for the node
                ...     #       the direct relation points to.
                ...     "prop09": ContainerProperty(DirectRelation),
                ...     "prop10": ContainerProperty(DirectRelation(is_list=True)),
                ...     # Note: Enum also support `unknown_value`: The value to use when the enum value is unknown.
                ...     "prop11": ContainerProperty(
                ...         Enum({"Closed": EnumValue("Valve is closed"),
                ...               "Opened": EnumValue("Valve is opened")})),
                ...     # Note: Floats support unit references, e.g. `unit=UnitReference("pressure:bar")`:
                ...     "prop12": ContainerProperty(Float32),
                ...     "prop13": ContainerProperty(Float32(is_list=True)),
                ...     "prop14": ContainerProperty(Float64),
                ...     "prop15": ContainerProperty(Float64(is_list=True)),
                ...     "prop16": ContainerProperty(Int32),
                ...     "prop17": ContainerProperty(Int32(is_list=True)),
                ...     "prop18": ContainerProperty(Int64),
                ...     "prop19": ContainerProperty(Int64(is_list=True)),
                ...     "prop20": ContainerProperty(Json),
                ...     "prop21": ContainerProperty(Json(is_list=True)),
                ...     "prop22": ContainerProperty(SequenceReference),
                ...     "prop23": ContainerProperty(SequenceReference(is_list=True)),
                ...     # Note: It is adviced to represent files and time series directly as nodes
                ...     #       instead of referencing existing:
                ...     "prop24": ContainerProperty(FileReference),
                ...     "prop25": ContainerProperty(FileReference(is_list=True)),
                ...     "prop26": ContainerProperty(TimeSeriesReference),
                ...     "prop27": ContainerProperty(TimeSeriesReference(is_list=True)),
                ... }
                >>> container = ContainerApply(
                ...     space="my-space",
                ...     external_id="my-everything-container",
                ...     properties=container_properties,
                ... )
        """
        return run_sync(self.__async_client.data_modeling.containers.apply(container=container))
