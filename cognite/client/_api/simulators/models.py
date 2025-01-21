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
    SimulatorModelCore,
    SimulatorModelList,
    SimulatorModelRevision,
    SimulatorModelRevisionCore,
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

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        sort: CreatedTimeSort | None = None,
        filter: SimulatorModelRevisionsFilter | dict[str, Any] | None = None,
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_
        Retrieves a list of simulator model revisions that match the given criteria
        Args:
            limit (int): Maximum number of results to return. Defaults to 10. Set to -1, float(“inf”) or None to return all items.
            sort (CreatedTimeSort | None): The criteria to sort by.
            filter (SimulatorModelRevisionsFilter | dict[str, Any] | None): Filter to apply.
        Returns:
            SimulatorModelRevisionList: List of simulator model revisions
        Examples:
            List simulator model revisions:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.list()
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

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModelRevision | None:
        """`Retrieve a simulator model revision <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_
        Retrieve a simulator model revision by ID or external ID
        Args:
            id (int | None): id of the simulator model revision.
            external_id (str | None): external id for a simulator model revision.
        Returns:
            SimulatorModelRevision | None: Requested simulator model revision.
        Examples:
            Get simulator model revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve(id=123)

            Get simulator model revision by external id:
                >>> res = client.simulators.models.revisions.retrieve(external_id="model_external_id")
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=identifiers,
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
    ) -> SimulatorModelRevisionList:
        """`Retrieve simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Retrieve simulator model revisions by IDs or external IDs
        Args:
            ids (Sequence[int] | None): IDs.
            external_ids (SequenceNotStr[str] | None): External Ids.
        Returns:
            SimulatorModelRevisionList: Requested simulator model revision(s).
        Examples:
            Get simulator model revision by ids:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve(ids=[123,124])

            Get simulator model revisions by external ids:
                >>> res = client.simulators.models.revisions.retrieve(external_id=["a", "b"])

        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=identifiers,
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
            limit (int): Maximum number of results to return. Defaults to 10. Set to -1, float(“inf”) or None to return all items.
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
                >>> res = client.simulators.models.list(
                    ...     filter=SimulatorModelsFilter(simulator_external_ids=["simulator_external_id"])
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

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModel | None:
        """`Retrieve simulator model <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_
        Retrieve a simulator model by ID or external ID
        Args:
            id (int | None): The id of the simulator model.
            external_id (str | None): The external id of the simulator model.
        Returns:
            SimulatorModel | None: Requested simulator model
        Examples:
            Retrieve simulator model by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve(id=1)

            Retrieve simulator model by external id:
                >>> res = client.simulators.models.retrieve(external_id="model_external_id")
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
        )

    def retrieve_multiple(
        self, ids: Sequence[int] | None = None, external_ids: SequenceNotStr[str] | None = None
    ) -> SimulatorModelList:
        """`Retrieve simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_
        Retrieve multiple simulator models by IDs or external IDs
        Args:
            ids (Sequence[int] | None): IDs.
            external_ids (SequenceNotStr[str] | None): External Ids.
        Returns:
            SimulatorModelList: Requested simulator model
        Examples:
            Retrieve simulator model by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve(ids=[1,2])

            Retrieve simulator model by external id:
                >>> res = client.simulators.models.retrieve(external_ids=["model_external_id", "model_external_id2"])
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
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
        self, chunk_size: int, filter: SimulatorModelsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModel]: ...

    @overload
    def __call__(
        self, chunk_size: None = None, filter: SimulatorModelsFilter | None = None, limit: int | None = None
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
    def create(self, models: Sequence[SimulatorModel]) -> SimulatorModelList: ...

    @overload
    def create(self, models: SimulatorModel | SimulatorModelWrite) -> SimulatorModelList: ...

    def create(
        self, models: SimulatorModel | SimulatorModelWrite | Sequence[SimulatorModel] | Sequence[SimulatorModelWrite]
    ) -> SimulatorModel | SimulatorModelList:
        """`Create simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_
        You can create an arbitrary number of simulator models, and the SDK will split the request into multiple requests.
        Args:
            models (SimulatorModel | SimulatorModelWrite | Sequence[SimulatorModel] | Sequence[SimulatorModelWrite]): Model(s) to create.
        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)
        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SimulatorModelWrite
                >>> client = CogniteClient()
                >>> models = [SimulatorModelWrite(name="model1"), SimulatorModelWrite(name="model2")]
                >>> res = client.simulators.models.create(models)
        """
        assert_type(models, "simulator_model", [SimulatorModelCore, Sequence])

        return self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=models,
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
            ids (int | Sequence[int] | None): ids (or sequence of ids) for the model(s) to delete.
            external_ids (str | SequenceNotStr[str] | None): external ids (or sequence of external ids) for the model(s) to delete.
        Examples:
            Delete models by id or external id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.delete(ids=[1,2,3], external_ids="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            wrap_ids=True,
            resource_path=self._RESOURCE_PATH,
        )

    @overload
    def create_revisions(
        self, revisions: Sequence[SimulatorModelRevision]
    ) -> SimulatorModelRevision | SimulatorModelRevisionList: ...

    @overload
    def create_revisions(
        self, revisions: SimulatorModelRevision | SimulatorModelRevisionWrite
    ) -> SimulatorModelRevision | SimulatorModelRevisionList: ...

    def create_revisions(
        self,
        revisions: SimulatorModelRevision
        | SimulatorModelRevisionWrite
        | Sequence[SimulatorModelRevision]
        | Sequence[SimulatorModelRevisionWrite],
    ) -> SimulatorModelRevision | SimulatorModelRevisionList:
        """`Create one or more simulator model revisions. <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post>`_
        You can create an arbitrary number of simulator model revisions, and the SDK will split the request into multiple requests.
        Args:
            revisions (SimulatorModelRevision | SimulatorModelRevisionWrite | Sequence[SimulatorModelRevision] | Sequence[SimulatorModelRevisionWrite]): Simulator model or list of Simulator models to create.
        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList: Created simulator model(s)
        Examples:
            Create new simulator models:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SimulatorModelRevision
                >>> client = CogniteClient()
                >>> models = [SimulatorModelRevision(external_id="model1"), SimulatorModelRevision(external_id="model2")]
                >>> res = client.simulators.models.create_revision(models)
        """
        assert_type(revisions, "simulator_model_revision", [SimulatorModelRevisionCore, SimulatorModelRevisionWrite])

        return self._create_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            items=revisions,
            input_resource_cls=SimulatorModelRevisionWrite,
            resource_path=self._RESOURCE_PATH + "/revisions",
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
