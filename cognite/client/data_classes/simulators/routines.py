from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResourceList,
    CogniteSort,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SimulatorRoutineCore(WriteableCogniteResource["SimulatorRoutineWrite"], ABC):
    """
    The simulator routine resource defines instructions on interacting with a simulator model. A simulator routine includes:

    * Inputs (values set into the simulator model)
    * Commands (actions to be performed by the simulator)
    * Outputs (values read from the simulator model)

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutineWrite(SimulatorRoutineCore):
    def as_write(self) -> SimulatorRoutineWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutine(SimulatorRoutineCore):
    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int | None = None,
        simulator_external_id: str | None = None,
        description: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.model_external_id = model_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.name = name
        self.description = description
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time
        self.simulator_external_id: str | None = simulator_external_id
        self.data_set_id: int | None = data_set_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = cls(
            external_id=resource["externalId"],
            simulator_external_id=resource.get("simulatorExternalId"),
            model_external_id=resource["modelExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            name=resource["name"],
            data_set_id=resource.get("dataSetId"),
            description=resource.get("description"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            id=resource.get("id"),
        )
        return instance

    def as_write(self) -> SimulatorRoutineWrite:
        """Returns a writeable version of this resource"""
        return SimulatorRoutineWrite(
            external_id=self.external_id,
            model_external_id=self.model_external_id,
            simulator_integration_external_id=self.simulator_integration_external_id,
            name=self.name,
            description=self.description,
        )

    def __hash__(self) -> int:
        return hash(self.external_id)


class PropertySort(CogniteSort):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        dumped["property"] = self.property
        return dumped


class CreatedTimeSort(PropertySort):
    def __init__(
        self,
        property: Literal["createdTime"] = "createdTime",
        order: Literal["asc", "desc"] = "asc",
    ):
        super().__init__(property, order)


class SimulatorRoutineWriteList(CogniteResourceList[SimulatorRoutineWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorRoutineWrite


class SimulatorRoutineList(WriteableCogniteResourceList[SimulatorRoutineWrite, SimulatorRoutine], IdTransformerMixin):
    _RESOURCE = SimulatorRoutine

    def as_write(self) -> SimulatorRoutineWriteList:
        return SimulatorRoutineWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())
