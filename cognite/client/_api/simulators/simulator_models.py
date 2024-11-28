from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulatorModelRevisionsFilter, SimulatorModelsFilter
from cognite.client.data_classes.simulators.simulators import (
    SimulatorModel,
    SimulatorModelList,
    SimulatorModelRevision,
    SimulatorModelRevisionList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorModelsFilter | dict[str, Any] | None = None
    ) -> SimulatorModelList:
        """`Filter Simulator Models <https://api-docs.cogheim.net/redoc/#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        List simulator models

        Args:
            limit (int): The maximum number of simulator models to return. Defaults to 100.
            filter (SimulatorModelsFilter | dict[str, Any] | None): The filter to narrow down simulator models.

        Returns:
            SimulatorModelList: List of simulator models

        Examples:

            List simulator models:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.models.list()

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/models/list",
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
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

            List simulator models:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.models.list()

            Get simulator model by id:
                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.models.retrieve(id=1)

            Get simulator model by external id:
                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.models.retrieve(external_id="1")

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
            resource_path="/simulators/models",
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
        )
