from __future__ import annotations

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
        self,
        limit: int = DEFAULT_LIMIT_READ,
        sort: CreatedTimeSort | None = None,
        filter: SimulatorModelRevisionsFilter | dict[str, Any] | None = None,
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_
        Retrieves a list of simulator model revisions that match the given criteria
        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items. sort (CreatedTimeSort | None): Sort order for the results.
            sort (CreatedTimeSort | None): No description.
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
            filter=filter.dump(camel_case=True) if isinstance(filter, CogniteFilter) else None,
        )

    @overload
    def retrieve(
        self,
        external_id: str,
        id: int | None = None,
    ) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], id: int | None = None) -> SimulatorModelRevisionList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], id: int | None = None
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None:
        """`Retrieve simulator model revision <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_
        Retrieve a simulator model revision by ID or external ID
        Args:
            external_id (str | SequenceNotStr[str]): The external id of the simulator model revision.
            id (int | None): The id of the simulator model revision.
        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList | None: Requested simulator model revision
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
                    >>> res = client.simulators.models.list(
                    ...     filter={"name": "my_simulator_model"},
                    ...     sort=("created_time")
                    ... )

        """
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            sort=[CreatedTimeSort.load(sort).dump()] if sort else None,
            filter=filter.dump(camel_case=True) if isinstance(filter, CogniteFilter) else None,
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
                >>> res = client.simulators.models.retrieve(external_id="foo")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=identifiers,
        )
