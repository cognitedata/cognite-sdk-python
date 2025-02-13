from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteFilter
from cognite.client.data_classes.simulators.filters import SimulatorRoutinesFilter
from cognite.client.data_classes.simulators.routines import (
    CreatedTimeSort,
    SimulatorRoutine,
    SimulatorRoutineCore,
    SimulatorRoutineList,
    SimulatorRoutineWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorRoutinesAPI(APIClient):
    _RESOURCE_PATH = "/simulators/routines"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def __iter__(self) -> Iterator[SimulatorRoutine]:
        """Iterate over simulator routines

        Fetches simulator routines as they are iterated over, so you keep a limited number of simulator routines in memory.

        Returns:
            Iterator[SimulatorRoutine]: yields Simulator routines one by one.
        """
        return self()

    @overload
    def __call__(
        self, chunk_size: int, filter: SimulatorRoutinesFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorRoutineList]: ...

    @overload
    def __call__(
        self, chunk_size: None = None, filter: SimulatorRoutinesFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorRoutine]: ...

    def __call__(
        self, chunk_size: int | None = None, filter: SimulatorRoutinesFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorRoutine] | Iterator[SimulatorRoutineList]:
        """Iterate over simulator routines

        Fetches simulator routines as they are iterated over, so you keep a limited number of simulator routines in memory.

        Args:
            chunk_size (int | None): Number of simulator routines to return in each chunk. Defaults to yielding one simulator routine a time.
            filter (SimulatorRoutinesFilter | None): Filter to apply on the routines list.
            limit (int | None): Maximum number of simulator routines to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorRoutine] | Iterator[SimulatorRoutineList]: yields SimulatorRoutine one by one if chunk is not specified, else SimulatorRoutineList objects.
        """
        return self._list_generator(
            list_cls=SimulatorRoutineList,
            resource_cls=SimulatorRoutine,
            method="POST",
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
            chunk_size=chunk_size,
            limit=limit,
        )

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter: SimulatorRoutinesFilter | dict[str, Any] | None = None,
        sort: CreatedTimeSort | None = None,
    ) -> SimulatorRoutineList:
        """`Filter simulator routines <https://developer.cognite.com/api#tag/Simulator-Routines/operation/filter_simulator_routines_simulators_routines_list_post>`_

        Retrieves a list of simulator routines that match the given criteria

        Args:
            limit (int): Maximum number of results to return. Defaults to 1000. Set to -1, float(“inf”) or None to return all items.
            filter (SimulatorRoutinesFilter | dict[str, Any] | None): The filter to narrow down simulator routines.
            sort (CreatedTimeSort | None): The criteria to sort by.

        Returns:
            SimulatorRoutineList: List of simulator routines

        Examples:
            List simulator routines:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.routines.list()

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import SimulatorRoutinesFilter
                >>> from cognite.client.data_classes.simulators.routines import CreatedTimeSort
                >>> res = client.simulators.routines.list(
                ...     filter=SimulatorRoutinesFilter(simulator_integration_external_ids=["integration_ext_id"]),
                ...     sort=CreatedTimeSort(order="asc")
                ... )

        """
        self._warning.warn()
        return self._list(
            limit=limit,
            method="POST",
            url_path="/simulators/routines/list",
            resource_cls=SimulatorRoutine,
            list_cls=SimulatorRoutineList,
            sort=[CreatedTimeSort.load(sort).dump()] if sort else None,
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
        )

    @overload
    def create(self, routines: Sequence[SimulatorRoutineWrite]) -> SimulatorRoutineList: ...

    @overload
    def create(self, routines: SimulatorRoutineWrite) -> SimulatorRoutineList: ...

    def create(
        self,
        routines: SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite],
    ) -> SimulatorRoutine | SimulatorRoutineList:
        """`Create simulator routine <https://developer.cognite.com/api#tag/Simulator-Routines/operation/create_simulator_routine_simulators_routines_post>`_
        You can create an arbitrary number of simulator routines, and the SDK will split the request into multiple requests.
        Args:
            routines (SimulatorRoutineWrite | Sequence[SimulatorRoutineWrite]): Simulator routines to create.
        Returns:
            SimulatorRoutine | SimulatorRoutineList: Created simulator routine(s)
        Examples:
            Create new simulator routines:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SimulatorModelWrite
                >>> client = CogniteClient()
                >>> routines = [SimulatorModelWrite(name="routine1"), SimulatorModelWrite(name="routine2")]
                >>> res = client.simulators.routines.create(routines)
        """
        assert_type(routines, "simulator_routines", [SimulatorRoutineCore, Sequence])

        return self._create_multiple(
            list_cls=SimulatorRoutineList,
            resource_cls=SimulatorRoutine,
            items=routines,
            input_resource_cls=SimulatorRoutineWrite,
            resource_path=self._RESOURCE_PATH,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete one or more routines <https://developer.cognite.com/api#tag/Simulator-Routines/operation/delete_simulator_routine_simulators_routines_delete_post>`_

        Args:
            id (int | Sequence[int] | None): ids (or sequence of ids) for the routine(s) to delete.
            external_ids (str | SequenceNotStr[str] | SequenceNotStr[str] | None): external ids (or sequence of external ids) for the routine(s) to delete.

        Examples:

            Delete routines by id or external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.routines.delete(id=[1,2,3], external_id="foo")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_ids),
            wrap_ids=True,
        )
