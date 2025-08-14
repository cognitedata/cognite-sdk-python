from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, NoReturn, overload

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
from cognite.client.utils._experimental import FeaturePreviewWarning, warn_on_all_method_invocations
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


@warn_on_all_method_invocations(
    FeaturePreviewWarning(api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators")
)
class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorModelRevisionsAPI(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 1
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorModelList:
        """`Filter simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        Retrieves a list of simulator models that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter by simulator external id(s).
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorModelList: List of simulator models

        Examples:
            List simulator models:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.list(limit=10)

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
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            filter=model_filter.dump(),
        )

    @overload
    def retrieve(self, ids: None = None, external_ids: None = None) -> NoReturn: ...

    @overload
    def retrieve(self, ids: int, external_ids: None = None) -> SimulatorModel | None: ...

    @overload
    def retrieve(
        self,
        ids: None,
        external_ids: str,
    ) -> SimulatorModel | None: ...

    @overload
    def retrieve(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
    ) -> SimulatorModelList | None: ...

    def retrieve(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModel | SimulatorModelList | None:
        """`Retrieve simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Retrieve one or more simulator models by ID(s) or external ID(s).

        Args:
            ids (int | Sequence[int] | None): The id of the simulator model(s).
            external_ids (str | SequenceNotStr[str] | None): The external id of the simulator model(s).

        Returns:
            SimulatorModel | SimulatorModelList | None: Requested simulator model(s)

        Examples:
            Get simulator model by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
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
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
        )

    def __iter__(self) -> Iterator[SimulatorModel]:
        """Iterate over simulator models

        Fetches simulator models as they are iterated over, so you keep a limited number of simulator models in memory.

        Returns:
            Iterator[SimulatorModel]: yields Simulator model one by one.
        """
        return self()

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
        """Iterate over simulator simulator models

        Fetches simulator models as they are iterated over, so you keep a limited number of simulator models in memory.

        Args:
            chunk_size (int | None): Number of simulator models to return in each chunk. Defaults to yielding one simulator model a time.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter by simulator external id(s).
            sort (PropertySort | None): The criteria to sort by.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Returns:
            Iterator[SimulatorModel] | Iterator[SimulatorModelList]: yields SimulatorModel one by one if chunk is not specified, else SimulatorModelList objects.
        """
        model_filter = SimulatorModelsFilter(simulator_external_ids=simulator_external_ids)
        return self._list_generator(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            method="POST",
            filter=model_filter.dump(),
            chunk_size=chunk_size,
            limit=limit,
        )

    @overload
    def create(self, items: SimulatorModelWrite) -> SimulatorModel: ...

    @overload
    def create(self, items: Sequence[SimulatorModelWrite]) -> SimulatorModelList: ...

    def create(self, items: SimulatorModelWrite | Sequence[SimulatorModelWrite]) -> SimulatorModel | SimulatorModelList:
        """`Create simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        Args:
            items (SimulatorModelWrite | Sequence[SimulatorModelWrite]): The model(s) to create.

        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)

        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelWrite
                >>> client = CogniteClient()
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

        return self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=items,
            input_resource_cls=SimulatorModelWrite,
            resource_path=self._RESOURCE_PATH,
        )

    def delete(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/delete_simulator_model_simulators_models_delete_post>`_

        Args:
            ids (int | Sequence[int] | None): id (or sequence of ids) for the model(s) to delete.
            external_ids (str | SequenceNotStr[str] | None): external id (or sequence of external ids) for the model(s) to delete.

        Examples:
            Delete simulator models by id or external id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.models.delete(ids=[1,2,3], external_ids="model_external_id")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            wrap_ids=True,
            resource_path=self._RESOURCE_PATH,
        )

    @overload
    def update(
        self,
        items: Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModelList: ...

    @overload
    def update(
        self,
        items: SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate,
    ) -> SimulatorModel: ...

    def update(
        self,
        items: SimulatorModel
        | SimulatorModelWrite
        | SimulatorModelUpdate
        | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel | SimulatorModelList:
        """`Update simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/update_simulator_model_simulators_models_update_post>`_

        Args:
            items (SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate]): The model to update.

        Returns:
            SimulatorModel | SimulatorModelList: Updated simulator model(s)

        Examples:
            Update a simulator model that you have fetched. This will perform a full update of the model:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> model = client.simulators.models.retrieve(external_ids="model_external_id")
                >>> model.name = "new_name"
                >>> res = client.simulators.models.update(model)
        """
        return self._update_multiple(
            list_cls=SimulatorModelList, resource_cls=SimulatorModel, update_cls=SimulatorModelUpdate, items=items
        )
