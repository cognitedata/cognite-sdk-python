from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SimulatorRoutineCore(WriteableCogniteResource["SimulatorRoutineWrite"], ABC):
    """
    The simulator routine resource defines instructions on interacting with a simulator model.

    Simulator routines can have multiple revisions, enabling users to track changes and evolve the routine over time.
    Each model can have multiple routines, each performing different objectives such as calculating optimal
    operation setpoints, forecasting production, benchmarking asset performance, and more.

    Each simulator routine can have a maximum of 10 revisions

    This is the read/response format of a simulator routine.

    Args:
        external_id (str): External id of the simulator routine
        model_external_id (str): External id of the associated simulator model
        simulator_integration_external_id (str): External id of the associated simulator integration
        name (str): The name of the simulator routine
        description (str | None): The description of the simulator routine
    """

    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.model_external_id = model_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.name = name
        self.description = description

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            model_external_id=resource["modelExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            name=resource["name"],
            description=resource.get("description"),
        )


class SimulatorRoutineWrite(SimulatorRoutineCore):
    def as_write(self) -> SimulatorRoutineWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutine(SimulatorRoutineCore):
    """
    The simulator routine resource defines instructions on interacting with a simulator model.

    Simulator routines can have multiple revisions, enabling users to track changes and evolve the routine over time.
    Each model can have multiple routines, each performing different objectives such as calculating optimal
    operation setpoints, forecasting production, benchmarking asset performance, and more.

    Each simulator routine can have a maximum of 10 revisions

    This is the read/response format of a simulator routine.

    Args:
        id (int): A unique id of a simulator routine
        external_id (str): External id of the simulator routine
        model_external_id (str): External id of the associated simulator model
        simulator_integration_external_id (str): External id of the associated simulator integration
        name (str): The name of the simulator routine
        data_set_id (int): The id of the dataset associated with the simulator routine
        simulator_external_id (str): External id of the associated simulator
        created_time (int): The time when the simulator routine was created
        last_updated_time (int): The time when the simulator routine was last updated
        description (str | None): The description of the simulator routine
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int,
        simulator_external_id: str,
        created_time: int,
        last_updated_time: int,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            model_external_id=model_external_id,
            simulator_integration_external_id=simulator_integration_external_id,
            name=name,
            description=description,
        )

        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.simulator_external_id = simulator_external_id
        self.data_set_id = data_set_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            model_external_id=resource["modelExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            name=resource["name"],
            data_set_id=resource["dataSetId"],
            description=resource.get("description"),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            id=resource["id"],
        )

    def as_write(self) -> SimulatorRoutineWrite:
        """Returns a writeable version of this resource"""
        return SimulatorRoutineWrite(
            external_id=self.external_id,
            model_external_id=self.model_external_id,
            simulator_integration_external_id=self.simulator_integration_external_id,
            name=self.name,
            description=self.description,
        )


class SimulatorRoutineWriteList(CogniteResourceList[SimulatorRoutineWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorRoutineWrite


class SimulatorRoutineList(WriteableCogniteResourceList[SimulatorRoutineWrite, SimulatorRoutine], IdTransformerMixin):
    _RESOURCE = SimulatorRoutine

    def as_write(self) -> SimulatorRoutineWriteList:
        return SimulatorRoutineWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())
