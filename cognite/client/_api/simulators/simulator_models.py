from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorModelsFilter
from cognite.client.data_classes.simulators.simulators import (
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


class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators")

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorModelsFilter | dict[str, Any] | None = None
    ) -> SimulatorModelList:
        """`Filter Simulator Models <https://api-docs.cogheim.net/redoc/#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        List all simulation models

        Args:
            limit (int): The maximum number of simulator models to return. Defaults to 100.
            filter (SimulatorModelsFilter | dict[str, Any] | None): The filter to narrow down simulator models.

        Returns:
            SimulatorModelList: List of simulator models

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list_models()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/models/list",
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            headers={"cdf-version": "beta"},
            filter=filter.dump()
            if isinstance(filter, SimulatorModelsFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModel | None:
        """`Retrieve Simulator Model <https://api-docs.cogheim.net/redoc/#tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Get a simulator model by id/externalId

        Args:
            id (int | None): The id of the simulator model.
            external_id (str | None): The external id of the simulator model.

        Returns:
            SimulatorModel | None: Requested simulator model

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.retrieve_model()

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
            resource_path="/simulators/models",
            headers={"cdf-version": "beta"},
        )

    def list_revisions(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorModelRevisionsFilter | dict[str, Any] | None = None
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://api-docs.cognite.com/20230101-alpha/tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List all simulation model revisions

        Args:
            limit (int): The maximum number of model revisions to return. Defaults to 100.
            filter (SimulatorModelRevisionsFilter | dict[str, Any] | None): The filter to narrow down simulator model revisions.

        Returns:
            SimulatorModelRevisionList: List all simulation model revisions

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list_revisions()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/models/revisions/list",
            resource_cls=SimulatorModelRevision,
            list_cls=SimulatorModelRevisionList,
            headers={"cdf-version": "beta"},
            filter=filter.dump()
            if isinstance(filter, SimulatorModelRevisionsFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )

    def retrieve_revision(self, id: int | None = None, external_id: str | None = None) -> SimulatorModelRevision | None:
        """`Retrieve Simulator Model Revisions <https://api-docs.cogheim.net/redoc/#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Retrieve simulator model revisions by IDs or external IDs

        Args:
            id (int | None): The id of the simulator model revision.
            external_id (str | None): The external id of the simulator model revision.

        Returns:
            SimulatorModelRevision | None: Requested simulator model revision

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.retrieve_revision()

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=identifiers,
            resource_path="/simulators/models/revisions",
            headers={"cdf-version": "beta"},
        )

    @overload
    def create(self, model: Sequence[SimulatorModel]) -> SimulatorModelList: ...

    @overload
    def create(self, model: SimulatorModel | SimulatorModelWrite) -> SimulatorModelList: ...

    def create(
        self, models: SimulatorModel | SimulatorModelWrite | Sequence[SimulatorModel] | Sequence[SimulatorModelWrite]
    ) -> SimulatorModel | SimulatorModelList:
        """`Create simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        You can create an arbitrary number of simulator models, and the SDK will split the request into multiple requests.

        Args:
            models (SimulatorModel | SimulatorModelWrite | Sequence[SimulatorModel] | Sequence[SimulatorModelWrite]): No description.

        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)

        Examples:

            Create new simulator models::

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
            resource_path="/simulators/models",
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete one or more models <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_ids (str | SequenceNotStr[str] | None): No description.
        Examples:

            Delete models by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.delete_models(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_ids),
            wrap_ids=True,
            resource_path="/simulators/models",
        )

    @overload
    def create_revisions(self, revision: Sequence[SimulatorModelRevision]) -> SimulatorModelRevisionList: ...

    @overload
    def create_revisions(
        self, revision: SimulatorModelRevision | SimulatorModelRevisionWrite
    ) -> SimulatorModelRevisionList: ...

    def create_revisions(
        self,
        revisions: SimulatorModelRevision
        | SimulatorModelRevisionWrite
        | Sequence[SimulatorModelRevision]
        | Sequence[SimulatorModelRevisionWrite],
    ) -> SimulatorModel | SimulatorModelList:
        """`Create one or more simulator model revisions. <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post>`_

        You can create an arbitrary number of simulator model revisions, and the SDK will split the request into multiple requests.

        Args:
            revisions (SimulatorModelRevision | SimulatorModelRevisionWrite | Sequence[SimulatorModelRevision] | Sequence[SimulatorModelRevisionWrite]): No description.

        Returns:
            SimulatorModel | SimulatorModelList: Created simulator model(s)

        Examples:

            Create new simulator models::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SimulatorModelRevision
                >>> client = CogniteClient()
                >>> models = [SimulatorModelRevision(external_id="model1"), SimulatorModelRevision(external_id="model2")]
                >>> res = client.simulators.models.create_revision(models)

        """
        assert_type(revisions, "simulator_model_revision", [SimulatorModelRevisionCore, Sequence])

        return self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=revisions,
            input_resource_cls=SimulatorModelRevisionWrite,
            resource_path="/simulators/models/revisions",
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
