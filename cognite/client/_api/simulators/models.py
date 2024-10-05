from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite_gen.client.data_classes.simulators.models import SimulatorModel, SimulatorModelList, SimulatorModelUpdate, SimulatorModelWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class ModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Models"
        )

    def create(self, simulator_model_create_command: SimulatorModelWrite | Sequence[SimulatorModelWrite]) -> SimulatorModel:
        """`Create Simulator Model <MISSING>`_

        Create a single simulation model

        Args: 
            simulator_model_create_command (SimulatorModelWrite | Sequence[SimulatorModelWrite]): None
        
        Returns:
            SimulatorModel: None

        Examples:

            Create simulator model:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelWrite
                >>> client = CogniteClient()
                >>> simulator_model = SimulatorModelWrite(<MISSING>)
                >>> res = client.hosted_extractors.destinations.create(simulator_model)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            items=simulator_model_create_command,
            input_resource_cls=SimulatorModelWrite,
            headers={"cdf-version": "beta"},
        )

    def retrieve(self, unknown: Unknown) -> SimulatorModel:
        """`Retrieve Simulator Model <MISSING>`_

        Get a simulator model by id/externalId

        Args: 
            unknown (Unknown): None
        
        Returns:
            SimulatorModel: None

        Examples:

            Retrieve  simulator model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve('mySimulator')

            Get multiple simulator models by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve(["mySimulator", "mySimulator2"])


        """
        self._warning.warn()
        return self._retrieve_multiple(
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            identifiers=IdentifierSequence.load(unknowns=unknown),
            headers={"cdf-version": "beta"},
        )

    def update(self, simulator_model_update_command: SimulatorModelUpdate | Sequence[SimulatorModelUpdate]) -> SimulatorModel:
        """`Update Simulator Model <MISSING>`_

        Update a simulator model.

        Args: 
            simulator_model_update_command (SimulatorModelUpdate | Sequence[SimulatorModelUpdate]): None
        
        Returns:
            SimulatorModel: None

        Examples:

            Update simulator model:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelUpdate
                >>> client = CogniteClient()
                >>> update = SimulatorModelUpdate('mySimulatorModel').<MISSING>
                >>> res = client.simulators.models.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=simulator_model_update_command,
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            update_cls=SimulatorModelUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, unknown: Unknown) -> EmptyResponse | None:
        """`Delete Simulator Model <MISSING>`_

        Delete simulator model

        Args: 
            unknown (Unknown): None
        
        Returns:
            EmptyResponse | None: None

        Examples:

            Delete simulator model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.models.delete(["mySimulator", "mySimulator2"])


        """
        self._warning.warn()
        return self._delete_multiple(
            identifiers=IdentifierSequence.load(unknowns=unknown),
            wrap_ids=False,
            returns_items=False,
            headers={"cdf-version": "beta"},
        )

    def filter(self, limit: int, filter: ListSimulatorModelsFilters | None = None, cursor: str | None = None, sort: SortByCreatedTime | Sequence[SortByCreatedTime] | None = None) -> SimulatorModel:
        """`Filter Simulator Models <MISSING>`_

        List all simulation models

        Args: 
            limit (int): None
            filter (ListSimulatorModelsFilters | None): None
            cursor (str | None): Cursor for pagination
            sort (SortByCreatedTime | Sequence[SortByCreatedTime] | None): Only supports sorting by one property at a time
        
        Returns:
            SimulatorModel: None

        Examples:

            <MISSING>

        """
        "<MISSING>"
