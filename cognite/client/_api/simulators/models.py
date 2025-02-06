from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteFilter
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorModelsFilter
from cognite.client.data_classes.simulators.models import (
    CreatedTimeSort,
    SimulatorModel,
    SimulatorModelList,
    SimulatorModelRevision,
    SimulatorModelRevisionList,
    SimulatorModelRevisionWrite,
    SimulatorModelUpdate,
    SimulatorModelWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorModelRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models/revisions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 1

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        sort: CreatedTimeSort | None = None,
        filter: SimulatorModelRevisionsFilter | dict[str, Any] | None = None,
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_
        Retrieves a list of simulator model revisions that match the given criteria
        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            sort (CreatedTimeSort | None): The criteria to sort by.
            filter (SimulatorModelRevisionsFilter | dict[str, Any] | None): Filter to apply.
        Returns:
            SimulatorModelRevisionList: List of simulator model revisions
        Examples:
            List simulator model revisions:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.list()

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter
                >>> from cognite.client.data_classes.simulators.models import CreatedTimeSort
                >>> res = client.simulators.models.revisions.list(
                ...     filter=SimulatorModelRevisionsFilter(model_external_ids=["model_external_id"]),
                ...     sort=CreatedTimeSort(order="asc")
                ... )
        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModelRevision,
            list_cls=SimulatorModelRevisionList,
            sort=[CreatedTimeSort.load(sort).dump()] if sort else None,
            filter=filter.dump(camel_case=True) if isinstance(filter, CogniteFilter) else filter,
        )

    @overload
    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None: ...

    def retrieve(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None:
        """`Retrieve simulator model revision(s) <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_
        Retrieve one or more simulator model revisions by ID(s) or external ID(s)
        Args:
            id (int | Sequence[int] | None): The ids of the simulator model revisions.
            external_id (str | SequenceNotStr[str] | None): The external ids of the simulator model revisions.
        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList | None: Requested simulator model revision(s)
        Examples:
            Get simulator model revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve(id=1)

            Get simulator model revision by external id:
                >>> res = client.simulators.models.revisions.retrieve(external_id="revision_external_id")

            Get multiple simulator model revisions by ids:
                >>> res = client.simulators.models.revisions.retrieve(id=[1,2])

            Get multiple simulator model revisions by external ids:
                >>> res = client.simulators.models.revisions.retrieve(external_id=["revision1", "revision2"])
        """
        self._warning.warn()

        if isinstance(id, int) or isinstance(external_id, str):
            identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
            return self._retrieve_multiple(
                list_cls=SimulatorModelRevisionList,
                resource_cls=SimulatorModelRevision,
                identifiers=identifiers,
            )
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
        )

    def __iter__(self) -> Iterator[SimulatorModelRevision]:
        """Iterate over simulator model revisions

        Fetches simulator model revisions as they are iterated over, so you keep a limited number of simulator model revisions in memory.

        Returns:
            Iterator[SimulatorModelRevision]: yields Simulator model revisions one by one.
        """
        return self()

    @overload
    def __call__(
        self, chunk_size: int, filter: SimulatorModelRevisionsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModelRevision]: ...

    @overload
    def __call__(
        self, chunk_size: None = None, filter: SimulatorModelRevisionsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModelRevision]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        filter: SimulatorModelRevisionsFilter | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelRevision] | Iterator[SimulatorModelRevisionList]:
        """Iterate over simulator simulator model revisions

        Fetches simulator model revisions as they are iterated over, so you keep a limited number of simulator model revisions in memory.

        Args:
            chunk_size (int | None): Number of simulator model revisions to return in each chunk. Defaults to yielding one simulator model revision a time.
            filter (SimulatorModelRevisionsFilter | None): Filter to apply on the model revisions list.
            limit (int | None): Maximum number of simulator model revisions to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorModelRevision] | Iterator[SimulatorModelRevisionList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            method="POST",
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
            chunk_size=chunk_size,
            limit=limit,
        )

    @overload
    def create(self, revision: SimulatorModelRevisionWrite) -> SimulatorModelRevision: ...

    @overload
    def create(self, revision: Sequence[SimulatorModelRevisionWrite]) -> SimulatorModelRevisionList: ...

    def create(
        self, revision: SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]
    ) -> SimulatorModelRevision | SimulatorModelRevisionList:
        """`Create one or more simulator model revisions. <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post>`_
        You can create an arbitrary number of simulator model revisions.
        Args:
            revision (SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]): No description.
        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList: Created simulator model(s)
        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.models import SimulatorModelRevisionWrite
                >>> client = CogniteClient()
                >>> models = [
                ...     SimulatorModelRevisionWrite(external_id="model1", file_id=1, model_external_id="a_1"),
                ...     SimulatorModelRevisionWrite(external_id="model2", file_id=2, model_external_id="a_2")
                ... ]
                >>> res = client.simulators.models.revisions.create(models)
        """
        assert_type(revision, "simulator_model_revision", [SimulatorModelRevisionWrite, Sequence])

        return self._create_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            items=revision,
            input_resource_cls=SimulatorModelRevisionWrite,
        )


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
        sort: CreatedTimeSort | None = None,
    ) -> SimulatorModelList:
        """`Filter simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_
        Retrieves a list of simulator models that match the given criteria
        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            filter (SimulatorModelsFilter | dict[str, Any] | None): Filter to apply.
            sort (CreatedTimeSort | None): The criteria to sort by.
        Returns:
            SimulatorModelList: List of simulator models

        Examples:
            List simulator models:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.list()

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import SimulatorModelsFilter
                >>> from cognite.client.data_classes.simulators.models import CreatedTimeSort
                >>> res = client.simulators.models.list(
                ...     filter=SimulatorModelsFilter(simulator_external_ids=["simulator_external_id"]),
                ...     sort=CreatedTimeSort(order="asc")
                ... )

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            sort=[CreatedTimeSort.load(sort).dump()] if sort else None,
            filter=filter.dump(camel_case=True) if isinstance(filter, CogniteFilter) else filter,
        )

    @overload
    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModel | None: ...

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
        """`Retrieve simulator model(s) <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_
        Retrieve one or more simulator models by ID(s) or external ID(s)
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
                >>> res = client.simulators.models.retrieve(external_id=["model_external_id", "model_external_id2"])
        """
        self._warning.warn()

        if isinstance(id, int) or isinstance(external_id, str):
            identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
            return self._retrieve_multiple(
                list_cls=SimulatorModelList,
                resource_cls=SimulatorModel,
                identifiers=identifiers,
            )

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
    ) -> Iterator[SimulatorModel]: ...

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
            model (SimulatorModelWrite | Sequence[SimulatorModelWrite]): No description.
        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)
        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators.models import SimulatorModelWrite
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
            Delete models by id or external id:
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
        item: Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel: ...

    @overload
    def update(
        self,
        item: SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate,
    ) -> SimulatorModel: ...

    def update(
        self,
        item: SimulatorModel
        | SimulatorModelWrite
        | SimulatorModelUpdate
        | Sequence[SimulatorModel | SimulatorModelWrite | SimulatorModelUpdate],
    ) -> SimulatorModel | SimulatorModelList:
        return self._update_multiple(
            list_cls=SimulatorModelList, resource_cls=SimulatorModel, update_cls=SimulatorModelUpdate, items=item
        )
