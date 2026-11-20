from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
    WriteableCogniteResourceWithClientRef,
)
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._text import copy_doc_from_async


class SimulatorModelRevisionWrite(WriteableCogniteResource["SimulatorModelRevisionWrite"]):
    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        file_id: int,
        description: str | None = None,
        external_dependencies: list[SimulatorModelRevisionDependency] | None = None,
    ) -> None:
        self.external_id = external_id
        self.model_external_id = model_external_id
        self.file_id = file_id
        self.description = description
        self.external_dependencies = external_dependencies

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.external_dependencies is not None:
            output["externalDependencies"] = [item.dump(camel_case=camel_case) for item in self.external_dependencies]

        return output

    def as_write(self) -> SimulatorModelRevisionWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> SimulatorModelRevisionWrite:
        return cls(
            external_id=resource["externalId"],
            model_external_id=resource["modelExternalId"],
            file_id=resource["fileId"],
            description=resource.get("description"),
            external_dependencies=SimulatorModelRevisionDependency._load_list(resource["externalDependencies"])
            if "externalDependencies" in resource
            else None,
        )


class SimulatorModelRevision(WriteableCogniteResourceWithClientRef["SimulatorModelRevisionWrite"]):
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
        external_dependencies (list[SimulatorModelRevisionDependency] | None): A list of external dependencies for the simulator model revision
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
        external_dependencies: list[SimulatorModelRevisionDependency] | None = None,
    ) -> None:
        self.external_id = external_id
        self.model_external_id = model_external_id
        self.file_id = file_id
        self.description = description
        self.external_dependencies = external_dependencies
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_id = data_set_id
        self.created_by_user_id = created_by_user_id
        self.status = status
        self.version_number = version_number
        self.log_id = log_id
        self.status_message = status_message
        self.simulator_external_id = simulator_external_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
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
            external_dependencies=SimulatorModelRevisionDependency._load_list(resource["externalDependencies"])
            if "externalDependencies" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.external_dependencies is not None:
            output["externalDependencies"] = [item.dump(camel_case=camel_case) for item in self.external_dependencies]

        return output

    def as_write(self) -> SimulatorModelRevisionWrite:
        """Returns this SimulatorModelRevision in its write version."""
        return SimulatorModelRevisionWrite(
            external_id=self.external_id,
            model_external_id=self.model_external_id,
            file_id=self.file_id,
            description=self.description,
        )

    async def get_data_async(self) -> SimulatorModelRevisionData | None:
        """`Retrieve data associated with this simulator model revision. <https://api-docs.cognite.com/20230101-alpha/tag/Simulator-Models/operation/retrieve_simulator_model_revision_data>`_

        Returns:
            SimulatorModelRevisionData | None: Data for the simulator model revision.
        """
        data = await self._cognite_client.simulators.models.revisions.retrieve_data(
            model_revision_external_id=self.external_id
        )
        if data:
            return data[0]
        return None

    @copy_doc_from_async(get_data_async)
    def get_data(self) -> SimulatorModelRevisionData | None:
        return run_sync(self.get_data_async())


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
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            data_set_id=resource["dataSetId"],
            name=resource["name"],
            type=resource["type"],
            description=resource.get("description"),
        )


class SimulatorModelWrite(SimulatorModelCore):
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

        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
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
        """Returns this SimulatorModel in its write version."""
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
        return SimulatorModelWriteList([a.as_write() for a in self.data])


class SimulatorModelRevisionWriteList(CogniteResourceList[SimulatorModelRevisionWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorModelRevisionWrite


class SimulatorModelRevisionList(
    WriteableCogniteResourceList[SimulatorModelRevisionWrite, SimulatorModelRevision], IdTransformerMixin
):
    _RESOURCE = SimulatorModelRevision

    def as_write(self) -> SimulatorModelRevisionWriteList:
        return SimulatorModelRevisionWriteList([a.as_write() for a in self.data])


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


@dataclass
class SimulatorModelDependencyFileReference(CogniteResource, ABC): ...


@dataclass
class SimulatorModelDependencyFileId(SimulatorModelDependencyFileReference):
    id: int

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(id=resource["id"])


@dataclass
class SimulatorModelRevisionDependency(CogniteResource):
    """
    Represents an external dependency for a simulator model revision.
    Args:
        file (SimulatorModelDependencyFileReference): The file ID associated with the external dependency.
        arguments (dict[str, str]): A dictionary that contains the key-value pairs (fields) for the external dependency.
    """

    file: SimulatorModelDependencyFileReference
    arguments: dict[str, str]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            file=SimulatorModelDependencyFileId.load(resource["file"])
            if "id" in resource["file"]
            else resource["file"],
            arguments=resource["arguments"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["file"] = (
            self.file.dump(camel_case=camel_case)
            if isinstance(self.file, SimulatorModelDependencyFileReference)
            else self.file
        )
        return output

    @classmethod
    def _load_list(cls, resource: list[dict[str, Any]]) -> list[SimulatorModelRevisionDependency]:
        return [cls._load(item) for item in resource]


@dataclass
class SimulatorFlowsheetObjectEdge(CogniteResource):
    id: str
    name: str | None
    source_id: str
    target_id: str
    connection_type: Literal["Material", "Energy", "Information"]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            name=resource.get("name"),
            source_id=resource["sourceId"],
            target_id=resource["targetId"],
            connection_type=resource["connectionType"],
        )

    @classmethod
    def _load_list(cls, resource: list[dict[str, Any]]) -> list[SimulatorFlowsheetObjectEdge]:
        return [cls._load(item) for item in resource]


@dataclass
class SimulatorFlowsheetThermodynamic(CogniteResource):
    property_packages: list[str]
    components: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            property_packages=resource["propertyPackages"],
            components=resource["components"],
        )


@dataclass
class SimulationValueUnitReference(CogniteResource):
    name: str
    quantity: str | None = None
    external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource["name"],
            quantity=resource.get("quantity"),
            external_id=resource.get("externalId"),
        )


@dataclass
class SimulatorFlowsheetProperty(CogniteResource):
    name: str
    reference_object: dict[str, str]
    value_type: Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"]
    value: str | float | list[str] | list[float]
    unit: SimulationValueUnitReference | None
    read_only: bool | None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource["name"],
            reference_object=resource["referenceObject"],
            value_type=resource["valueType"],
            value=resource["value"],
            unit=SimulationValueUnitReference._load_if(resource.get("unit")),
            read_only=resource.get("readOnly"),
        )

    @classmethod
    def _load_list(cls, resource: list[dict[str, Any]]) -> list[SimulatorFlowsheetProperty]:
        return [cls._load(item) for item in resource]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorFlowsheetPosition(CogniteResource):
    x: float
    y: float

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(x=resource["x"], y=resource["y"])


@dataclass
class SimulatorFlowsheetGraphicalObject(CogniteResource):
    position: SimulatorFlowsheetPosition | None
    height: float | None
    width: float | None
    scale_x: float | int | None
    scale_y: float | int | None
    angle: float | None
    active: bool | None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            position=SimulatorFlowsheetPosition._load_if(resource.get("position")),
            height=resource.get("height"),
            width=resource.get("width"),
            scale_x=resource.get("scaleX"),
            scale_y=resource.get("scaleY"),
            angle=resource.get("angle"),
            active=resource.get("active"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.position is not None:
            output["position"] = self.position.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorFlowsheetObjectNode(CogniteResource):
    id: str
    name: str | None
    type: str
    graphical_object: SimulatorFlowsheetGraphicalObject | None
    properties: list[SimulatorFlowsheetProperty]

    @classmethod
    def _load_list(cls, resource: list[dict[str, Any]]) -> list[SimulatorFlowsheetObjectNode]:
        return [cls._load(item) for item in resource]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            name=resource.get("name"),
            type=resource["type"],
            graphical_object=SimulatorFlowsheetGraphicalObject._load_if(resource.get("graphicalObject")),
            properties=SimulatorFlowsheetProperty._load_list(resource["properties"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.graphical_object is not None:
            output["graphicalObject"] = self.graphical_object.dump(camel_case=camel_case)
        output["properties"] = [item.dump(camel_case=camel_case) for item in self.properties]

        return output


@dataclass
class SimulatorFlowsheet(CogniteResource):
    simulator_object_nodes: list[SimulatorFlowsheetObjectNode]
    simulator_object_edges: list[SimulatorFlowsheetObjectEdge]
    thermodynamics: SimulatorFlowsheetThermodynamic

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            simulator_object_nodes=SimulatorFlowsheetObjectNode._load_list(resource["simulatorObjectNodes"]),
            simulator_object_edges=SimulatorFlowsheetObjectEdge._load_list(resource["simulatorObjectEdges"]),
            thermodynamics=SimulatorFlowsheetThermodynamic._load(resource["thermodynamics"]),
        )

    @classmethod
    def _load_list(cls, resource: list[dict[str, Any]]) -> list[SimulatorFlowsheet]:
        return [cls._load(item) for item in resource]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["simulatorObjectNodes"] = [item.dump(camel_case=camel_case) for item in self.simulator_object_nodes]
        output["simulatorObjectEdges"] = [item.dump(camel_case=camel_case) for item in self.simulator_object_edges]
        if self.thermodynamics is not None:
            output["thermodynamics"] = self.thermodynamics.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorModelRevisionData(CogniteResource):
    """
    Extracted metadata from a simulator model file associated with a model revision.

    When a model revision is created, connectors can optionally parse the simulator file
    to extract structured information about the model's internal structure and configuration.
    This data resource stores the parsed information, which may include flowsheet details,
    process equipment, operating parameters, connections between blocks, and visualization data.

    Note: The availability and extent of this data depends entirely on the connector
    implementation and simulator type. Some connectors may:
    - Not implement this feature at all (no data extraction)
    - Partially implement it (e.g., only populate 'info' or only 'flowsheets')
    - Fully implement it with comprehensive model details

    Args:
        model_revision_external_id (str): External id of the associated model revision
        created_time (int): The time when the simulator model revision data was created
        last_updated_time (int): The time when the simulator model revision data was last updated
        data_set_id (int): The id of the dataset associated with the simulator model revision data
        flowsheets (list[SimulatorFlowsheet] | None): Extracted flowsheet information,
            if supported by the connector. May include blocks, equipment, properties, and connections
        info (dict[str, str] | None): Additional metadata extracted from the simulator file,
            if supported by the connector
    """

    def __init__(
        self,
        model_revision_external_id: str,
        created_time: int,
        last_updated_time: int,
        data_set_id: int,
        flowsheets: list[SimulatorFlowsheet] | None,
        info: dict[str, str] | None,
    ) -> None:
        self.model_revision_external_id = model_revision_external_id
        self.flowsheets = flowsheets
        self.info = info
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            model_revision_external_id=resource["modelRevisionExternalId"],
            flowsheets=SimulatorFlowsheet._load_list(resource["flowsheets"]) if resource.get("flowsheets") else None,
            info=resource.get("info"),
            data_set_id=resource["dataSetId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.flowsheets is not None:
            output["flowsheets"] = [
                item.dump(camel_case=camel_case) if isinstance(item, SimulatorFlowsheet) else item
                for item in self.flowsheets
            ]

        return output


class SimulatorModelRevisionDataList(CogniteResourceList[SimulatorModelRevisionData], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorModelRevisionData
