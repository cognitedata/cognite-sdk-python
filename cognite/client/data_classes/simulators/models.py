from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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


class SimulatorModelRevisionCore(WriteableCogniteResource["SimulatorModelRevisionWrite"], ABC):
    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        file_id: int,
        simulator_external_id: str | None = None,
        data_set_id: int | None = None,
        created_by_user_id: str | None = None,
        status: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        version_number: int | None = None,
        log_id: int | None = None,
        description: str | None = None,
        status_message: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.file_id = file_id
        self.created_by_user_id = created_by_user_id
        self.status = status
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.version_number = version_number
        self.log_id = log_id
        self.description = description
        self.status_message = status_message

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            model_external_id=resource["modelExternalId"],
            data_set_id=resource["dataSetId"],
            file_id=resource["fileId"],
            created_by_user_id=resource["createdByUserId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            version_number=resource["versionNumber"],
            log_id=resource["logId"],
            description=resource["description"],
            status_message=resource["statusMessage"],
        )


class SimulatorModelRevisionWrite(SimulatorModelRevisionCore):
    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        file_id: int,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            model_external_id=model_external_id,
            file_id=file_id,
            description=description,
        )

    def as_write(self) -> SimulatorModelRevisionWrite:
        """Returns a writeable version of this resource"""
        return self

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> SimulatorModelRevisionWrite:
        return cls(
            external_id=resource["externalId"],
            model_external_id=resource["modelExternalId"],
            file_id=resource["fileId"],
            description=resource.get("description"),
        )


class SimulatorModelRevision(SimulatorModelRevisionCore):
    """
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    Args:
        id (int): Internal id of the simulator model revision
        external_id (str): External id of the simulator model revision
        model_external_id (str): External id of the associated simulator model
        file_id (int): The id of the file associated with the simulator model revision
        created_time (int): The time when the simulator model revision was created
        last_updated_time (int): The time when the simulator model revision was last updated
        simulator_external_id (str): External id of the simulator associated with the simulator model revision
        data_set_id (int): The id of the dataset associated with the simulator model revision
        created_by_user_id (str): The id of the user who created the simulator model revision
        status (str): The status of the simulator model revision
        version_number (int): The version number of the simulator model revision
        log_id (int): The id of the log associated with the simulator model revision
        description (str | None): The description of the simulator model revision
        status_message (str | None): The current status message of the simulator model revision
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        model_external_id: str,
        file_id: int,
        created_time: int,
        last_updated_time: int,
        simulator_external_id: str,
        data_set_id: int,
        created_by_user_id: str,
        status: str,
        version_number: int,
        log_id: int,
        description: str | None = None,
        status_message: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            model_external_id=model_external_id,
            data_set_id=data_set_id,
            file_id=file_id,
            created_by_user_id=created_by_user_id,
            status=status,
            created_time=created_time,
            last_updated_time=last_updated_time,
            version_number=version_number,
            log_id=log_id,
            description=description,
            status_message=status_message,
        )
        self.id: int = id
        self.created_time: int = created_time
        self.last_updated_time: int = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            model_external_id=resource["modelExternalId"],
            data_set_id=resource["dataSetId"],
            file_id=resource["fileId"],
            created_by_user_id=resource["createdByUserId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            version_number=resource["versionNumber"],
            log_id=resource["logId"],
            description=resource.get("description"),
            status_message=resource.get("statusMessage"),
        )

    def as_write(self) -> SimulatorModelRevisionWrite:
        """Returns this SimulatorModelRevision in its writing version."""
        return SimulatorModelRevisionWrite(
            external_id=self.external_id,
            model_external_id=self.model_external_id,
            file_id=self.file_id,
            description=self.description,
        )


class SimulatorModelCore(WriteableCogniteResource["SimulatorModelWrite"], ABC):
    """
    The simulator model resource represents an asset modeled in a simulator.
    This asset could range from a pump or well to a complete processing facility or refinery.
    The simulator model is the root of its associated revisions, routines, runs, and results.
    The dataset assigned to a model is inherited by its children. Deleting a model also deletes all its children, thereby
    maintaining the integrity and hierarchy of the simulation data.
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    This is the read/response format of a simulator model.
    Args:
        external_id (str): External id of the simulator model
        simulator_external_id (str): External id of the associated simulator
        data_set_id (int): The id of the dataset associated with the simulator model
        name (str): The name of the simulator model
        type (str): The type key of the simulator model
        description (str | None): The description of the simulator model
    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        type: str,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.data_set_id = data_set_id
        self.name = name
        self.type = type
        self.description = description

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            data_set_id=resource["dataSetId"],
            name=resource["name"],
            type=resource["type"],
            description=resource.get("description"),
        )


class SimulatorModelWrite(SimulatorModelCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        type: str,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            data_set_id=data_set_id,
            name=name,
            type=type,
            description=description,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SimulatorModelWrite:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            data_set_id=resource["dataSetId"],
            name=resource["name"],
            type=resource["type"],
            description=resource.get("description"),
        )

    def as_write(self) -> SimulatorModelWrite:
        return self


class SimulatorModel(SimulatorModelCore):
    """
    The simulator model resource represents an asset modeled in a simulator.
    This asset could range from a pump or well to a complete processing facility or refinery.
    The simulator model is the root of its associated revisions, routines, runs, and results.
    The dataset assigned to a model is inherited by its children. Deleting a model also deletes all its children, thereby
    maintaining the integrity and hierarchy of the simulation data.
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    This is the read/response format of a simulator model.
    Args:
        id (int): A unique id of a simulator model
        external_id (str): External id of the simulator model
        simulator_external_id (str): External id of the associated simulator
        data_set_id (int): The id of the dataset associated with the simulator model
        name (str): The name of the simulator model
        type (str): The type key of the simulator model
        created_time (int): The time when the simulator model was created
        last_updated_time (int): The time when the simulator model was last updated
        description (str | None): The description of the simulator model
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        type: str,
        created_time: int,
        last_updated_time: int,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            data_set_id=data_set_id,
            name=name,
            type=type,
            description=description,
        )

        self.id: int = id
        self.created_time: int = created_time
        self.last_updated_time: int = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            data_set_id=resource["dataSetId"],
            name=resource["name"],
            type=resource["type"],
            description=resource.get("description"),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def as_write(self) -> SimulatorModelWrite:
        """Returns this SimulatorModel in its writing version."""
        return SimulatorModelWrite(
            external_id=self.external_id,
            simulator_external_id=self.simulator_external_id,
            data_set_id=self.data_set_id,
            name=self.name,
            type=self.type,
            description=self.description,
        )


class SimulatorModelWriteList(CogniteResourceList[SimulatorModelWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorModelWrite


class SimulatorModelList(WriteableCogniteResourceList[SimulatorModelWrite, SimulatorModel], IdTransformerMixin):
    _RESOURCE = SimulatorModel

    def as_write(self) -> SimulatorModelWriteList:
        return SimulatorModelWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())


class SimulatorModelRevisionWriteList(CogniteResourceList[SimulatorModelRevisionWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorModelRevisionWrite


class SimulatorModelRevisionList(
    WriteableCogniteResourceList[SimulatorModelRevisionWrite, SimulatorModelRevision], IdTransformerMixin
):
    _RESOURCE = SimulatorModelRevision

    def as_write(self) -> SimulatorModelRevisionWriteList:
        return SimulatorModelRevisionWriteList(
            [a.as_write() for a in self.data], cognite_client=self._get_cognite_client()
        )


class SimulatorModelUpdate(CogniteUpdate):
    class _PrimitiveModelUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> None:
            self._set(value)

    @property
    def name(self) -> _PrimitiveModelUpdate:
        return SimulatorModelUpdate._PrimitiveModelUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveModelUpdate:
        return SimulatorModelUpdate._PrimitiveModelUpdate(self, "description")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name"),
            PropertySpec("description"),
        ]
