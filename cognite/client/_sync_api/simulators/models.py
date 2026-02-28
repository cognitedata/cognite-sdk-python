"""
===============================================================================
daa3274e810f251e36271b95a35470d1
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.simulators.models_revisions import SyncSimulatorModelRevisionsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.models import (
    SimulatorModel,
    SimulatorModelList,
    SimulatorModelUpdate,
    SimulatorModelWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorModelsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.revisions = SyncSimulatorModelRevisionsAPI(async_client)

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorModelList:
        """
        `Filter simulator models <https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        Retrieves a list of simulator models that match the given criteria.

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter by simulator external id(s).
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorModelList: List of simulator models

        Examples:
            List simulator models:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.models.list(limit=10)

            Iterate over simulator models, one-by-one:
                >>> for model in client.simulators.models():
                ...     model  # do something with the simulator model

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> res = client.simulators.models.list(
                ...     simulator_external_ids=["simulator_external_id"],
                ...     sort=PropertySort(
                ...         property="createdTime",
                ...         order="asc"
                ...     )
                ... )
        """
        return run_sync(
            self.__async_client.simulators.models.list(
                limit=limit, simulator_external_ids=simulator_external_ids, sort=sort
            )
        )

    @overload
    def retrieve(self, *, ids: int) -> SimulatorModel | None: ...

    @overload
    def retrieve(self, *, external_ids: str) -> SimulatorModel | None: ...

    @overload
    def retrieve(self, *, ids: Sequence[int]) -> SimulatorModelList: ...

    @overload
    def retrieve(self, *, external_ids: SequenceNotStr[str]) -> SimulatorModelList: ...

    def retrieve(
        self, *, ids: int | Sequence[int] | None = None, external_ids: str | SequenceNotStr[str] | None = None
    ) -> SimulatorModel | SimulatorModelList | None:
        """
        `Retrieve simulator models <https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Retrieve one or more simulator models by ID(s) or external ID(s).

        Args:
            ids (int | Sequence[int] | None): The id of the simulator model(s).
            external_ids (str | SequenceNotStr[str] | None): The external id of the simulator model(s).

        Returns:
            SimulatorModel | SimulatorModelList | None: Requested simulator model(s)

        Examples:
            Get simulator model by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.models.retrieve(ids=1)

            Get simulator model by external id:
                >>> res = client.simulators.models.retrieve(external_ids="model_external_id")

            Get multiple simulator models by ids:
                >>> res = client.simulators.models.retrieve(ids=[1,2])

            Get multiple simulator models by external ids:
                >>> res = client.simulators.models.retrieve(
                ...     external_ids=["model_external_id", "model_external_id2"]
                ... )
        """
        return run_sync(
            self.__async_client.simulators.models.retrieve(  # type: ignore [call-overload]
                ids=ids, external_ids=external_ids
            )
        )

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModel]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModel] | Iterator[SimulatorModelList]:
        """
        Iterate over simulator simulator models

        Fetches simulator models as they are iterated over, so you keep a limited number of simulator models in memory.

        Args:
            chunk_size (int | None): Number of simulator models to return in each chunk. Defaults to yielding one simulator model a time.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter by simulator external id(s).
            sort (PropertySort | None): The criteria to sort by.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Yields:
            SimulatorModel | SimulatorModelList: yields SimulatorModel one by one if chunk is not specified, else SimulatorModelList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.simulators.models(
                chunk_size=chunk_size, simulator_external_ids=simulator_external_ids, sort=sort, limit=limit
            )
        )  # type: ignore [misc]

    @overload
    def create(self, items: SimulatorModelWrite) -> SimulatorModel: ...

    @overload
    def create(self, items: Sequence[SimulatorModelWrite]) -> SimulatorModelList: ...

    def create(self, items: SimulatorModelWrite | Sequence[SimulatorModelWrite]) -> SimulatorModel | SimulatorModelList:
        """
        `Create simulator models <https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        Args:
            items (SimulatorModelWrite | Sequence[SimulatorModelWrite]): The model(s) to create.

        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)

        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> models = [
                ...     SimulatorModelWrite(
                ...         name="model1", simulator_external_id="sim1", type="SteadyState",
                ...         data_set_id=1, external_id="model_external_id"
                ...     ),
                ...     SimulatorModelWrite(
                ...         name="model2", simulator_external_id="sim2", type="SteadyState",
                ...         data_set_id=2, external_id="model_external_id2"
                ...     )
                ... ]
                >>> res = client.simulators.models.create(models)
        """
        return run_sync(self.__async_client.simulators.models.create(items=items))

    def delete(
        self, ids: int | Sequence[int] | None = None, external_ids: str | SequenceNotStr[str] | None = None
    ) -> None:
        """
        `Delete simulator models <https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/delete_simulator_model_simulators_models_delete_post>`_

        Args:
            ids (int | Sequence[int] | None): id (or sequence of ids) for the model(s) to delete.
            external_ids (str | SequenceNotStr[str] | None): external id (or sequence of external ids) for the model(s) to delete.

        Examples:
            Delete simulator models by id or external id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.simulators.models.delete(ids=[1,2,3], external_ids="model_external_id")
        """
        return run_sync(self.__async_client.simulators.models.delete(ids=ids, external_ids=external_ids))

    @overload
    def update(
        self, items: Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate]
    ) -> SimulatorModelList: ...

    @overload
    def update(self, items: SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate) -> SimulatorModel: ...

    def update(
        self,
        items: SimulatorModel
        | SimulatorModelWrite
        | SimulatorModelUpdate
        | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel | SimulatorModelList:
        """
        `Update simulator models <https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/update_simulator_model_simulators_models_update_post>`_

        Args:
            items (SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate]): The model to update.

        Returns:
            SimulatorModel | SimulatorModelList: Updated simulator model(s)

        Examples:
            Update a simulator model that you have fetched. This will perform a full update of the model:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> model = client.simulators.models.retrieve(external_ids="model_external_id")
                >>> model.name = "new_name"
                >>> res = client.simulators.models.update(model)
        """
        return run_sync(self.__async_client.simulators.models.update(items=items))
