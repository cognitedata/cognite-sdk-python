from __future__ import annotations

from collections.abc import Sequence
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
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorModelRevisionsFilter | dict[str, Any] | None = None
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_

        List simulator model revisions

        Args:
            limit (int): The maximum number of simulator model revisions to return.
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
            url_path="/simulators/models/revisions/list",
            resource_cls=SimulatorModelRevision,
            list_cls=SimulatorModelRevisionList,
            filter=filter.dump()
            if isinstance(filter, SimulatorModelRevisionsFilter)
            else filter
            if isinstance(filter, dict)
            else None,
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> SimulatorModelRevision | None:
        """`Retrieve simulator model revision <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Retrieve multiple simulator model revisions by IDs or external IDs

        Args:
            id (int | None): The id of the simulator model revision.
            external_id (str | None): The external id of the simulator model revision.

        Returns:
            SimulatorModelRevision | None: Requested simulator model revision

        Examples:

            Get simulator model revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve(id=123)

            Get simulator model revision by external id:
                >>> res = client.simulators.models.revisions.retrieve(external_id="abcdef")

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=identifiers,
            resource_path="/simulators/models/revisions",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> SimulatorModelRevisionList:
        """`Retrieve simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            SimulatorModelRevisionList: Requested simulator model revisions

        Examples:

            Get simulator model revisions by ids:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve_multiple(ids=[1, 2, 3])

            Get simulator model revisions by external ids:
                >>> res = client.simulators.models.revisions.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )


class SimulatorModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.revisions = SimulatorModelRevisionsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self, limit: int = DEFAULT_LIMIT_READ, filter: SimulatorModelsFilter | dict[str, Any] | None = None
    ) -> SimulatorModelList:
        """`Filter simulator models <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        Retrieves a list of simulator models that match the given criteria

        Args:
            limit (int): The maximum number of simulator models to return.
            filter (SimulatorModelsFilter | dict[str, Any] | None): Filter to apply.

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
            else None,
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

            List simulator models:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.list()

            Get simulator model by id:
                >>> res = client.simulators.models.retrieve(id=1)

            Get simulator model by external id:
                >>> res = client.simulators.models.retrieve(external_id="1")

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
            resource_path="/simulators/models",
        )
