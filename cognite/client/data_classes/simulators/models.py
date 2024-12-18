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
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        model_external_id: str | None = None,
        data_set_id: int | None = None,
        file_id: int | None = None,
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
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorModelRevisionWrite(SimulatorModelRevisionCore):
    def __init__(
        self,
        external_id: str | None = None,
        model_external_id: str | None = None,
        file_id: int | None = None,
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
            external_id=resource.get("externalId"),
            model_external_id=resource.get("modelExternalId"),
            file_id=resource.get("fileId"),
            description=resource.get("description"),
        )


class SimulatorModelRevision(SimulatorModelRevisionCore):
    """
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    Args:
        id (int | None): No description.
        external_id (str | None): External id of the simulator model revision
        simulator_external_id (str | None): No description.
        model_external_id (str | None): External id of the associated simulator model
        data_set_id (int | None): The id of the dataset associated with the simulator model revision
        file_id (int | None): The id of the file associated with the simulator model revision
        created_by_user_id (str | None): The id of the user who created the simulator model revision
        status (str | None): The status of the simulator model revision
        created_time (int | None): The time when the simulator model revision was created
        last_updated_time (int | None): The time when the simulator model revision was last updated
        version_number (int | None): The version number of the simulator model revision
        log_id (int | None): The id of the log associated with the simulator model revision
        description (str | None): The description of the simulator model revision
        status_message (str | None): The current status of the model revision
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        model_external_id: str | None = None,
        data_set_id: int | None = None,
        file_id: int | None = None,
        created_by_user_id: str | None = None,
        status: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        version_number: int | None = None,
        log_id: int | None = None,
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
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

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
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


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
        external_id (str): External id of the simulator model
        simulator_external_id (str): External id of the associated simulator
        data_set_id (int): The id of the dataset associated with the simulator model
        name (str): The name of the simulator model
        id (int): A unique id of a simulator model
        type (str): The type key of the simulator model
        description (str | None): The description of the simulator model
        created_time (int | None): The time when the simulator model was created
        last_updated_time (int | None): The time when the simulator model was last updated
    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        id: int,
        type: str,
        description: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            data_set_id=data_set_id,
            name=name,
            type=type,
            description=description,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        load = super()._load(resource, cognite_client)
        return cls(
            external_id=load.external_id,
            simulator_external_id=load.simulator_external_id,
            data_set_id=load.data_set_id,
            name=load.name,
            id=resource["id"],
            type=resource["type"],
            description=load.description,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case)


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
