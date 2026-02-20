from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api.simulators.models_revisions import SimulatorModelRevisionsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import PropertySort, SimulatorModelsFilter
from cognite.client.data_classes.simulators.models import (
    SimulatorModel,
    SimulatorModelList,
    SimulatorModelUpdate,
    SimulatorModelWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient, ClientConfig


class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorModelRevisionsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._RETRIEVE_LIMIT = 1
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    async def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorModelList:
        """`Filter simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        Retrieves a list of simulator models that match the given criteria.

        Args:
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            simulator_external_ids: Filter by simulator external id(s).
            sort: The criteria to sort by.

        Returns:
            List of simulator models

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
        model_filter = SimulatorModelsFilter(simulator_external_ids=simulator_external_ids)
        self._warning.warn()
        return await self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            filter=model_filter.dump(),
        )

    @overload
    async def retrieve(self, *, ids: int) -> SimulatorModel | None: ...

    @overload
    async def retrieve(self, *, external_ids: str) -> SimulatorModel | None: ...

    @overload
    async def retrieve(self, *, ids: Sequence[int]) -> SimulatorModelList: ...

    @overload
    async def retrieve(self, *, external_ids: SequenceNotStr[str]) -> SimulatorModelList: ...

    async def retrieve(
        self,
        *,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModel | SimulatorModelList | None:
        """`Retrieve simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Retrieve one or more simulator models by ID(s) or external ID(s).

        Args:
            ids: The id of the simulator model(s).
            external_ids: The external id of the simulator model(s).

        Returns:
            Requested simulator model(s)

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
        self._warning.warn()

        return await self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
        )

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[SimulatorModel]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[SimulatorModelList]: ...

    async def __call__(
        self,
        chunk_size: int | None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[SimulatorModel] | AsyncIterator[SimulatorModelList]:
        """Iterate over simulator simulator models

        Fetches simulator models as they are iterated over, so you keep a limited number of simulator models in memory.

        Args:
            chunk_size: Number of simulator models to return in each chunk. Defaults to yielding one simulator model a time.
            simulator_external_ids: Filter by simulator external id(s).
            sort: The criteria to sort by.
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Yields:
            yields SimulatorModel one by one if chunk is not specified, else SimulatorModelList objects.
        """  # noqa: DOC404
        model_filter = SimulatorModelsFilter(simulator_external_ids=simulator_external_ids)
        async for item in self._list_generator(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            method="POST",
            filter=model_filter.dump(),
            sort=[PropertySort.load(sort).dump()] if sort else None,
            chunk_size=chunk_size,
            limit=limit,
        ):
            yield item

    @overload
    async def create(self, items: SimulatorModelWrite) -> SimulatorModel: ...

    @overload
    async def create(self, items: Sequence[SimulatorModelWrite]) -> SimulatorModelList: ...

    async def create(
        self, items: SimulatorModelWrite | Sequence[SimulatorModelWrite]
    ) -> SimulatorModel | SimulatorModelList:
        """`Create simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        Args:
            items: The model(s) to create.

        Returns:
            Created simulator model(s)

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
        assert_type(items, "simulator_model", [SimulatorModelWrite, Sequence])

        return await self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=items,
            input_resource_cls=SimulatorModelWrite,
            resource_path=self._RESOURCE_PATH,
        )

    async def delete(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/delete_simulator_model_simulators_models_delete_post>`_

        Args:
            ids: id (or sequence of ids) for the model(s) to delete.
            external_ids: external id (or sequence of external ids) for the model(s) to delete.

        Examples:
            Delete simulator models by id or external id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.simulators.models.delete(ids=[1,2,3], external_ids="model_external_id")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            wrap_ids=True,
            resource_path=self._RESOURCE_PATH,
        )

    @overload
    async def update(
        self,
        items: Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModelList: ...

    @overload
    async def update(
        self,
        items: SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate,
    ) -> SimulatorModel: ...

    async def update(
        self,
        items: SimulatorModel
        | SimulatorModelWrite
        | SimulatorModelUpdate
        | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel | SimulatorModelList:
        """`Update simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/update_simulator_model_simulators_models_update_post>`_

        Args:
            items: The model to update.

        Returns:
            Updated simulator model(s)

        Examples:
            Update a simulator model that you have fetched. This will perform a full update of the model:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> model = client.simulators.models.retrieve(external_ids="model_external_id")
                >>> model.name = "new_name"
                >>> res = client.simulators.models.update(model)
        """
        return await self._update_multiple(
            list_cls=SimulatorModelList, resource_cls=SimulatorModel, update_cls=SimulatorModelUpdate, items=items
        )
