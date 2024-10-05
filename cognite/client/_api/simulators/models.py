from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.models import (
    SimulatorModel,
    SimulatorModelList,
    SimulatorModelUpdate,
    SimulatorModelWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class ModelsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Models")

    def create(self, item: SimulatorModelWrite) -> SimulatorModel:
        """`Create Simulator Model <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_simulators_models_post>`_

        Create a single simulation model

        Args:
            item (SimulatorModelWrite): Simulation model(s) to create

        Returns:
            SimulatorModel: Created simulation model(s)

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
            items=item,
            input_resource_cls=SimulatorModelWrite,
            headers={"cdf-version": "beta"},
        )

    def retrieve(self, id: int, external_id: str) -> SimulatorModel:
        """`Retrieve Simulator Model <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/retrieve_simulator_model_simulators_models_byids_post>`_

        Get a simulator model by id/externalId

        Args:
            id (int): Id of the simulator model
            external_id (str): External Id of the simulator model

        Returns:
            SimulatorModel: The requested simulator model

        Examples:

            Retrieve  simulator model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.retrieve('mySimulator')

        """
        self._warning.warn()
        return cast(
            SimulatorModel,
            self._retrieve_multiple(
                list_cls=SimulatorModelList,
                resource_cls=SimulatorModel,
                identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
                headers={"cdf-version": "beta"},
            ),
        )

    def update(self, item: SimulatorModelUpdate | SimulatorModelWrite) -> SimulatorModel:
        """`Update Simulator Model <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/update_simulator_model_simulators_models_update_post>`_

        Update a simulator model.

        Args:
            item (SimulatorModelUpdate | SimulatorModelWrite): Simulator model to update

        Returns:
            SimulatorModel: None

        Examples:

            Update simulator model:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelUpdate
                >>> client = CogniteClient()
                >>> update = SimulatorModelUpdate('mySimulatorModel').name.set('newName')
                >>> res = client.simulators.models.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=item,
            list_cls=SimulatorModelList,
            resource_cls=SimulatorModel,
            update_cls=SimulatorModelUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, id: int, external_id: str) -> None:
        """`Delete Simulator Model <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/delete_simulator_model_simulators_models_delete_post>`_

        Delete simulator model

        Args:
            id (int): Id of the simulator model to delete
            external_id (str): External Id of the simulator model to delete

        Examples:

            Delete simulator model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.models.delete(["mySimulator", "mySimulator2"])


        """
        self._warning.warn()

        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            returns_items=False,
            headers={"cdf-version": "beta"},
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> SimulatorModelList:
        """`Filter Simulator Models <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/filter_simulator_models_simulators_models_list_post>`_

        List all simulation models

        Args:
            limit (int): Max number of models to return. Defaults to 25.

        Returns:
            SimulatorModelList: None

        Examples:

            <MISSING>

        """
        self._warning.warn()
        return self._list(
            method="POST",
            resource_cls=SimulatorModel,
            list_cls=SimulatorModelList,
            limit=limit,
            headers={"cdf-version": "beta"},
        )
