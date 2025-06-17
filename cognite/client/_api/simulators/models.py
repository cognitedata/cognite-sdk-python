from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, NoReturn, overload

from cognite.client._api.simulators.models_revisions import SimulatorModelRevisionsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteFilter
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
    from cognite.client import ClientConfig, CogniteClient


class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorModelRevisionsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._RETRIEVE_LIMIT = 1
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter: SimulatorModelsFilter | dict[str, Any] | None = None,
        sort: PropertySort | None = None,
    ) -> SimulatorModelList:
        """`Filter simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        Retrieves a list of simulator models that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            filter (SimulatorModelsFilter | dict[str, Any] | None): Filter to apply.
            sort (PropertySort | None): The criteria to sort by.

        Returns:
            SimulatorModelList: List of simulator models

        Examples:
            List simulator models:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.list(limit=10)

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators import SimulatorModelsFilter
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> res = client.simulators.models.list(
                ...     filter=SimulatorModelsFilter(simulator_external_ids=["simulator_external_id"]),
                ...     sort=PropertySort(order="asc")
                ... )

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            filter=filter.dump(camel_case=True) if isinstance(filter, CogniteFilter) else filter,
        )

    @overload
    def retrieve(self, id: None = None, external_id: None = None) -> NoReturn: ...

    @overload
    def retrieve(self, id: int, external_id: None = None) -> SimulatorModel | None: ...

    @overload
    def retrieve(
        self,
        id: None,
        external_id: str,
    ) -> SimulatorModel | None: ...

    @overload
    def retrieve(
        self,
        id: Sequence[int] | None = None,
        external_id: SequenceNotStr[str] | None = None,
    ) -> SimulatorModelList | None: ...

    def retrieve(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModel | SimulatorModelList | None:
        """`Retrieve simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Retrieve one or more simulator models by ID(s) or external ID(s).

        Args:
            id (int | Sequence[int] | None): The id of the simulator model.
            external_id (str | SequenceNotStr[str] | None): The external id of the simulator model.

        Returns:
            SimulatorModel | SimulatorModelList | None: Requested simulator model(s)

        Examples:
            Get simulator model by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve(id=1)

            Get simulator model by external id:
                >>> res = client.simulators.models.retrieve(external_id="model_external_id")

            Get multiple simulator models by ids:
                >>> res = client.simulators.models.retrieve(id=[1,2])

            Get multiple simulator models by external ids:
                >>> res = client.simulators.models.retrieve(
                ...     external_id=["model_external_id", "model_external_id2"]
                ... )
        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
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
        self, chunk_size: None = None, filter: SimulatorModelsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModel]: ...

    @overload
    def __call__(
        self, chunk_size: int, filter: SimulatorModelsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModelList]: ...

    def __call__(
        self, chunk_size: int | None = None, filter: SimulatorModelsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModel] | Iterator[SimulatorModelList]:
        """Iterate over simulator simulator models

        Fetches simulator models as they are iterated over, so you keep a limited number of simulator models in memory.

        Args:
            chunk_size (int | None): Number of simulator models to return in each chunk. Defaults to yielding one simulator model a time.
            filter (SimulatorModelsFilter | None): Filter to apply on the model revisions list.
            limit (int | None): Maximum number of simulator models to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorModel] | Iterator[SimulatorModelList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            method="POST",
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
            chunk_size=chunk_size,
            limit=limit,
        )

    @overload
    def create(self, model: SimulatorModelWrite) -> SimulatorModel: ...

    @overload
    def create(self, model: Sequence[SimulatorModelWrite]) -> SimulatorModelList: ...

    def create(self, model: SimulatorModelWrite | Sequence[SimulatorModelWrite]) -> SimulatorModel | SimulatorModelList:
        """`Create simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        Args:
            model (SimulatorModelWrite | Sequence[SimulatorModelWrite]): The model to create.

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
        assert_type(model, "simulator_model", [SimulatorModelWrite, Sequence])

        return self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=model,
            input_resource_cls=SimulatorModelWrite,
            resource_path=self._RESOURCE_PATH,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/delete_simulator_model_simulators_models_delete_post>`_

        Args:
            id (int | Sequence[int] | None): id (or sequence of ids) for the model(s) to delete.
            external_id (str | SequenceNotStr[str] | None): external id (or sequence of external ids) for the model(s) to delete.

        Examples:
            Delete simulator models by id or external id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.models.delete(id=[1,2,3], external_id="model_external_id")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            resource_path=self._RESOURCE_PATH,
        )

    @overload
    def update(
        self,
        model: Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModelList: ...

    @overload
    def update(
        self,
        model: SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate,
    ) -> SimulatorModel: ...

    def update(
        self,
        model: SimulatorModel
        | SimulatorModelWrite
        | SimulatorModelUpdate
        | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel | SimulatorModelList:
        """`Update simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/update_simulator_model_simulators_models_update_post>`_

        Args:
            model (SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate]): The model to update.

        Returns:
            SimulatorModel | SimulatorModelList: Updated simulator model(s)

        Examples:
            Update a simulator model that you have fetched. This will perform a full update of the model:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> model = client.simulators.models.retrieve(external_id="model_external_id")
                >>> model.name = "new_name"
                >>> res = client.simulators.models.update(model)

            Perform a partial update on a simulator model, updating the description and name:
                >>> from cognite.client.data_classes.simulators import SimulatorModelUpdate
                >>> # TODO: Uncomment when SimulatorModelUpdate is fixed
                >>> # my_update = SimulatorModelUpdate(id=1).name.set("new_name").description.set("new_description")
                >>> # res = client.simulators.models.update(my_update)
        """
        return self._update_multiple(
            list_cls=SimulatorModelList, resource_cls=SimulatorModel, update_cls=SimulatorModelUpdate, items=model
        )
