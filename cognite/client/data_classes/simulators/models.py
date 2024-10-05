from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class _SimulatorModelCore(WriteableCogniteResource["SimulatorModelWrite"], ABC):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        name: str,
        data_set_id: int,
        type: str,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.name = name
        self.data_set_id = data_set_id
        self.type = type
        self.description = description


class SimulatorModelWrite(_SimulatorModelCore):
    """The simulator model resource represents an asset modeled in a simulator.

    This asset could range from a pump or well to a complete processing facility or refinery. The simulator model is the
    root of its associated revisions, routines, runs, and results. The dataset assigned to a model is inherited by its
    children. Deleting a model also deletes all its children, thereby maintaining the integrity and hierarchy of the
    simulation data.  Simulator model revisions track changes and updates to a simulator model over time. Each revision
    ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    #### Limitations:  - A project can have a maximum of 1000 simulator models  - Each simulator model can have a
    maximum of 200 revisions

    This is the write/request format of the simulator model.

    Args:
        external_id (str): External id of the simulation model
        simulator_external_id (str): External id of the simulator
        name (str): Name of the simulation model
        data_set_id (int): Data set id of the simulation model
        type (str): Model type of the simulation model. List of available types is available in the simulator resource.
        description (str | None): Description of the simulation model

    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        name: str,
        data_set_id: int,
        type: str,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            name=name,
            data_set_id=data_set_id,
            type=type,
            description=description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            name=resource["name"],
            data_set_id=resource["dataSetId"],
            type=resource["type"],
            description=resource.get("description"),
        )

    def as_write(self) -> SimulatorModelWrite:
        return self


class SimulatorModel(_SimulatorModelCore):
    """

    The simulator model resource represents an asset modeled in a simulator.

        This asset could range from a pump or well to a complete processing facility or refinery. The simulator model is the
        root of its associated revisions, routines, runs, and results. The dataset assigned to a model is inherited by its
        children. Deleting a model also deletes all its children, thereby maintaining the integrity and hierarchy of the
        simulation data.  Simulator model revisions track changes and updates to a simulator model over time. Each revision
        ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
        #### Limitations:  - A project can have a maximum of 1000 simulator models  - Each simulator model can have a
        maximum of 200 revisions

        This is the read/response format of the simulator model.

        Args:
            id (int): A unique id of a simulation model
            external_id (str): External id of the simulation model
            simulator_external_id (str): External id of the simulator
            name (str): Name of the simulation model
            data_set_id (int): Data set id of the simulation model
            created_time (int): None
            last_updated_time (int): None
            type (str): Model type of the simulation model. List of available types is available in the simulator resource.
            description (str | None): Description of the simulation model

    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        name: str,
        data_set_id: int,
        created_time: int,
        last_updated_time: int,
        type: str,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            name=name,
            data_set_id=data_set_id,
            type=type,
            description=description,
        )
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            name=resource["name"],
            data_set_id=resource["dataSetId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            description=resource.get("description"),
            type=resource["type"],
        )

    def as_write(self) -> SimulatorModelWrite:
        return SimulatorModelWrite(
            type=self.type,
            name=self.name,
            data_set_id=self.data_set_id,
            simulator_external_id=self.simulator_external_id,
            description=self.description,
            external_id=self.external_id,
        )


class SimulatorModelUpdate(CogniteUpdate):
    def __init__(self, id: int) -> None:
        super().__init__(id=id)

    class _UpdateStr(CognitePrimitiveUpdate):
        def set(self, value: str | None) -> SimulatorModelUpdate:
            return self._set(value)

    @property
    def name(self) -> SimulatorModelUpdate._UpdateStr:
        return self._UpdateStr(self, "name")

    @property
    def description(self) -> SimulatorModelUpdate._UpdateStr:
        return self._UpdateStr(self, "description")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=True),
            PropertySpec("description", is_nullable=True),
        ]


class SimulatorModelWriteList(CogniteResourceList[SimulatorModelWrite]):
    _RESOURCE = SimulatorModelWrite


class SimulatorModelList(WriteableCogniteResourceList[SimulatorModelWrite, SimulatorModel]):
    _RESOURCE = SimulatorModel

    def as_write(self) -> SimulatorModelWriteList:
        return SimulatorModelWriteList([item.as_write() for item in self.data])
