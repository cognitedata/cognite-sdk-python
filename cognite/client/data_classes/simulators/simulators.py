from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Sequence

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class SimulationValueUnitInput(CogniteObject):
    name: str
    quantity: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            quantity=resource.get("quantity"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineInputTimeseries(CogniteObject):
    name: str
    reference_id: str
    source_external_id: str
    aggregate: str | None = None
    save_timeseries_external_id: str | None = None
    unit: SimulationValueUnitInput | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            reference_id=resource["referenceId"],
            source_external_id=resource["sourceExternalId"],
            aggregate=resource.get("aggregate"),
            save_timeseries_external_id=resource.get("saveTimeseriesExternalId"),
            unit=SimulationValueUnitInput._load(resource["unit"], cognite_client) if "unit" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorRoutineInputConstant(CogniteObject):
    name: str
    reference_id: str
    value: str
    value_type: str
    unit: SimulationValueUnitInput | None = None
    save_timeseries_external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            reference_id=resource["referenceId"],
            value=resource["value"],
            value_type=resource["valueType"],
            unit=SimulationValueUnitInput._load(resource["unit"], cognite_client) if "unit" in resource else None,
            save_timeseries_external_id=resource.get("saveTimeseriesExternalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorRoutineOutput(CogniteObject):
    name: str
    reference_id: str
    value_type: str
    unit: SimulationValueUnitInput | None = None
    save_timeseries_external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            reference_id=resource["referenceId"],
            value_type=resource["valueType"],
            unit=SimulationValueUnitInput._load(resource["unit"], cognite_client) if "unit" in resource else None,
            save_timeseries_external_id=resource.get("saveTimeseriesExternalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output


@dataclass
class SimulatorRoutineSchedule(CogniteObject):
    enabled: bool
    cron_expression: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            enabled=resource["enabled"],
            cron_expression=resource.get("cronExpression"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineDataSampling(CogniteObject):
    enabled: bool
    validation_window: int | None = None
    sampling_window: int | None = None
    granularity: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            enabled=resource["enabled"],
            validation_window=resource.get("validationWindow"),
            sampling_window=resource.get("samplingWindow"),
            granularity=resource.get("granularity"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineLogicalCheckEnabled(CogniteObject):
    enabled: bool
    timeseries_external_id: str | None = None
    aggregate: str | None = None
    operator: str | None = None
    value: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            enabled=resource["enabled"],
            timeseries_external_id=resource.get("timeseriesExternalId"),
            aggregate=resource.get("aggregate"),
            operator=resource.get("operator"),
            value=resource.get("value"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineSteadyStateDetectionEnabled(CogniteObject):
    enabled: bool
    timeseries_external_id: str | None = None
    aggregate: str | None = None
    min_section_size: int | None = None
    var_threshold: float | None = None
    slope_threshold: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            enabled=resource["enabled"],
            timeseries_external_id=resource.get("timeseriesExternalId"),
            aggregate=resource.get("aggregate"),
            min_section_size=resource.get("minSectionSize"),
            var_threshold=resource.get("varThreshold"),
            slope_threshold=resource.get("slopeThreshold"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineConfiguration(CogniteObject):
    schedule: SimulatorRoutineSchedule
    data_sampling: SimulatorRoutineDataSampling
    logical_check: list[SimulatorRoutineLogicalCheckEnabled]
    steady_state_detection: list[SimulatorRoutineSteadyStateDetectionEnabled]
    inputs: list[SimulatorRoutineInputConstant | SimulatorRoutineInputTimeseries]
    outputs: list[SimulatorRoutineOutput]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            schedule=SimulatorRoutineSchedule._load(resource["schedule"], cognite_client),
            data_sampling=SimulatorRoutineDataSampling._load(resource["dataSampling"], cognite_client),
            logical_check=[
                SimulatorRoutineLogicalCheckEnabled._load(check_, cognite_client) for check_ in resource["logicalCheck"]
            ],
            steady_state_detection=[
                SimulatorRoutineSteadyStateDetectionEnabled._load(detection_, cognite_client)
                for detection_ in resource["steadyStateDetection"]
            ],
            inputs=[
                SimulatorRoutineInputConstant._load(input_, cognite_client)
                if "value" in input_
                else SimulatorRoutineInputTimeseries._load(input_, cognite_client)
                for input_ in resource["inputs"]
            ],
            outputs=[SimulatorRoutineOutput._load(output_, cognite_client) for output_ in resource["outputs"]],
        )


@dataclass
class SimulatorRoutineStepArguments(CogniteObject):
    reference_id: str | None = None
    object_name: str | None = None
    object_property: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            reference_id=resource.get("referenceId"),
            object_name=resource.get("objectName"),
            object_property=resource.get("objectProperty"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineStep(CogniteObject):
    step_type: str
    arguments: dict[str, Any]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            step_type=resource["stepType"],
            arguments=resource["arguments"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineStage(CogniteObject):
    order: int
    steps: list[SimulatorRoutineStep]
    description: str | None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            order=resource["order"],
            steps=[SimulatorRoutineStep._load(step_, cognite_client) for step_ in resource["steps"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorUnitEntry(CogniteObject):
    label: str
    name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            label=resource["label"],
            name=resource["name"],
        )


@dataclass
class SimulatorStepOption(CogniteObject):
    label: str
    value: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            label=resource["label"],
            value=resource["value"],
        )


@dataclass
class SimulatorModelType(CogniteObject):
    name: str
    key: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulatorModelType:
        return cls(
            name=resource["name"],
            key=resource["key"],
        )

    @classmethod
    def _load_list_or_dict(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> SimulatorModelType | list[SimulatorModelType]:
        if isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource]

        return cls._load(resource, cognite_client)


@dataclass
class SimulatorQuantity(CogniteObject):
    name: str
    label: str
    units: Sequence[SimulatorUnitEntry]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            label=resource["label"],
            units=[SimulatorUnitEntry._load(unit_, cognite_client) for unit_ in resource["units"]],
        )

    @classmethod
    def _load_list_or_dict(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> SimulatorQuantity | list[SimulatorQuantity]:
        if isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource]

        return cls._load(resource, cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["units"] = [unit_.dump(camel_case=camel_case) for unit_ in self.units]

        return output


@dataclass
class SimulatorStepField(CogniteObject):
    name: str
    label: str
    info: str
    options: Sequence[SimulatorStepOption] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            label=resource["label"],
            info=resource["info"],
            options=[SimulatorStepOption._load(option_, cognite_client) for option_ in resource["options"]]
            if "options" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.options is not None:
            output["options"] = [option_.dump(camel_case=camel_case) for option_ in self.options]

        return output


@dataclass
class SimulatorStep(CogniteObject):
    step_type: str
    fields: Sequence[SimulatorStepField]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            step_type=resource["stepType"],
            fields=[SimulatorStepField._load(field_, cognite_client) for field_ in resource["fields"]],
        )

    @classmethod
    def _load_list_or_dict(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> SimulatorStep | list[SimulatorStep]:
        if isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource]

        return cls._load(resource, cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["fields"] = [field_.dump(camel_case=camel_case) for field_ in self.fields]

        return output


class Simulator(CogniteResource):
    """The simulator resource contains the definitions necessary for Cognite Data Fusion (CDF) to interact with a given simulator.

    It serves as a central contract that allows APIs, UIs, and integrations (connectors) to utilize the same definitions
    when dealing with a specific simulator.  Each simulator is uniquely identified and can be associated with various
    file extension types, model types, step fields, and unit quantities. Simulators are essential for managing data
    flows between CDF and external simulation tools, ensuring consistency and reliability in data handling.  ####
    Limitations:  - A project can have a maximum of 100 simulators

    This is the read/response format of the simulator.

    Args:
        id (int): A unique id of a simulator
        external_id (str): External id of the simulator
        name (str): Name of the simulator
        file_extension_types (str | SequenceNotStr[str]): File extension types supported by the simulator
        created_time (int): None
        last_updated_time (int): None
        model_types (SimulatorModelType | Sequence[SimulatorModelType] | None): Model types supported by the simulator
        step_fields (SimulatorStep | Sequence[SimulatorStep] | None): Step types supported by the simulator when creating routines
        unit_quantities (SimulatorQuantity | Sequence[SimulatorQuantity] | None): Quantities and their units supported by the simulator

    """

    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        file_extension_types: str | SequenceNotStr[str],
        created_time: int,
        last_updated_time: int,
        model_types: SimulatorModelType | Sequence[SimulatorModelType] | None = None,
        step_fields: SimulatorStep | Sequence[SimulatorStep] | None = None,
        unit_quantities: SimulatorQuantity | Sequence[SimulatorQuantity] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.file_extension_types = file_extension_types
        self.model_types = model_types
        self.step_fields = step_fields
        self.unit_quantities = unit_quantities
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            name=resource["name"],
            file_extension_types=resource["fileExtensionTypes"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            model_types=SimulatorModelType._load_list_or_dict(resource["modelTypes"], cognite_client)
            if "modelTypes" in resource
            else None,
            step_fields=SimulatorStep._load_list_or_dict(resource["stepFields"], cognite_client)
            if "stepFields" in resource
            else None,
            unit_quantities=SimulatorQuantity._load_list_or_dict(resource["unitQuantities"], cognite_client)
            if "unitQuantities" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.model_types, SimulatorModelType):
            output["modelTypes" if camel_case else "model_types"] = self.model_types.dump(camel_case=camel_case)
        if isinstance(self.step_fields, SimulatorStep):
            output["stepFields" if camel_case else "step_fields"] = self.step_fields.dump(camel_case=camel_case)
        if isinstance(self.unit_quantities, SimulatorQuantity):
            output["unitQuantities" if camel_case else "unit_quantities"] = self.unit_quantities.dump(
                camel_case=camel_case
            )

        return output


class SimulatorIntegration(CogniteResource):
    """
    The simulator integration resource represents a simulator connector in Cognite Data Fusion (CDF).
    It provides information about the configured connectors for a given simulator, including their status and additional
    details such as dataset, name, license status, connector version, simulator version, and more. This resource is essential
    for monitoring and managing the interactions between CDF and external simulators, ensuring proper data flow and integration.

    Limitations:  - A project can have a maximum of 100 simulators

    This is the read/response format of the simulator integration.

    Args:

        id (int): A unique id of a simulator integration
        external_id (str): External id of the simulator integration
        simulator_external_id (str): External id of the associated simulator
        heartbeat (int): The interval in seconds between the last heartbeat and the current time
        active (bool): Whether the simulator integration is active
        data_set_id (int): The id of the dataset associated with the simulator integration
        connector_version (str): The version of the connector
        log_id (int): The id of the log associated with the simulator integration
        created_time (int): The time when the simulator integration was created
        last_updated_time (int): The time when the simulator integration was last updated
        license_status (str | None): The status of the license
        simulator_version (str | None): The version of the simulator
        license_last_checked_time (int | None): The time when the license was last checked
        connector_status (str | None): The status of the connector
        connector_status_updated_time (int | None): The time when the connector status was last updated

    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        heartbeat: int,
        active: bool,
        data_set_id: int,
        connector_version: str,
        log_id: int,
        created_time: int,
        last_updated_time: int,
        license_status: str | None = None,
        simulator_version: str | None = None,
        license_last_checked_time: int | None = None,
        connector_status: str | None = None,
        connector_status_updated_time: int | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.heartbeat = heartbeat
        self.active = active
        self.data_set_id = data_set_id
        self.connector_version = connector_version
        self.log_id = log_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.license_status = license_status
        self.simulator_version = simulator_version
        self.license_last_checked_time = license_last_checked_time
        self.connector_status = connector_status
        self.connector_status_updated_time = connector_status_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            heartbeat=resource["heartbeat"],
            active=resource["active"],
            data_set_id=resource["dataSetId"],
            connector_version=resource["connectorVersion"],
            log_id=resource["logId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            license_status=resource.get("licenseStatus"),
            simulator_version=resource.get("simulatorVersion"),
            license_last_checked_time=resource.get("licenseLastCheckedTime"),
            connector_status=resource.get("connectorStatus"),
            connector_status_updated_time=resource.get("connectorStatusUpdatedTime"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorModelRevision(CogniteResource):
    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        model_external_id: str,
        data_set_id: int,
        file_id: int,
        created_by_user_id: str,
        status: str,
        created_time: int,
        last_updated_time: int,
        version_number: int,
        log_id: int,
        description: str | None = None,
        status_message: str | None = None,
    ) -> None:
        self.id = id
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutineRevision(CogniteResource):
    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        routine_external_id: str,
        simulator_integration_external_id: str,
        model_external_id: str,
        data_set_id: int,
        created_by_user_id: str,
        version_number: int,
        created_time: int,
        configuration: SimulatorRoutineConfiguration,
        script: list[SimulatorRoutineStage],
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.routine_external_id = routine_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.created_by_user_id = created_by_user_id
        self.version_number = version_number
        self.created_time = created_time
        self.configuration = configuration
        self.script = script

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            routine_external_id=resource["routineExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            model_external_id=resource["modelExternalId"],
            data_set_id=resource["dataSetId"],
            created_by_user_id=resource["createdByUserId"],
            version_number=resource["versionNumber"],
            created_time=resource["createdTime"],
            configuration=SimulatorRoutineConfiguration._load(resource["configuration"], cognite_client),
            script=[SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource["script"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["configuration"] = self.configuration.dump(camel_case=camel_case)
        output["script"] = [stage_.dump(camel_case=camel_case) for stage_ in self.script]

        return output


class SimulatorModel(CogniteResource):
    """
    The simulator model resource represents an asset modeled in a simulator.
    This asset could range from a pump or well to a complete processing facility or refinery.
    The simulator model is the root of its associated revisions, routines, runs, and results.
    The dataset assigned to a model is inherited by its children. Deleting a model also deletes all its children, thereby
    maintaining the integrity and hierarchy of the simulation data.

    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.

    Limitations:
        - A project can have a maximum of 1000 simulator models
        - Each simulator model can have a maximum of 200 revisions


    This is the read/response format of a simulator model.

    Args:
        id (int): A unique id of a simulator model
        external_id (str): External id of the simulator model
        simulator_external_id (str): External id of the associated simulator
        data_set_id (int): The id of the dataset associated with the simulator model
        created_time (int): The time when the simulator model was created
        last_updated_time (int): The time when the simulator model was last updated
        name (str): The name of the simulator model
        type_key (str | None): The type key of the simulator model
        description (str | None): The description of the simulator model
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        created_time: int,
        last_updated_time: int,
        name: str,
        type_key: str | None = None,
        description: str | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.name = name
        self.type_key = type_key
        self.description = description

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
            type_key=resource.get("typeKey"),
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutine(CogniteResource):
    """
    The simulator routine resource defines instructions on interacting with a simulator model. A simulator routine includes:

    * Inputs (values set into the simulator model)
    * Commands (actions to be performed by the simulator)
    * Outputs (values read from the simulator model)

    Simulator routines can have multiple revisions, enabling users to track changes and evolve the routine over time.
    Each model can have multiple routines, each performing different objectives such as calculating optimal
    operation setpoints, forecasting production, benchmarking asset performance, and more.

    Limitations:
        - Each simulator model can have a maximum of 10 simulator routines

    Each simulator routine can have a maximum of 10 revisions

    This is the read/response format of a simulator routine.

    Args:
        id (int): A unique id of a simulator routine
        external_id (str): External id of the simulator routine
        simulator_external_id (str): External id of the associated simulator
        model_external_id (str): External id of the associated simulator model
        simulator_integration_external_id (str): External id of the associated simulator integration
        name (str): The name of the simulator routine
        data_set_id (int): The id of the dataset associated with the simulator routine
        created_time (int): The time when the simulator routine was created
        last_updated_time (int): The time when the simulator routine was last updated
        description (str | None): The description of the simulator routine
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int,
        created_time: int,
        last_updated_time: int,
        description: str | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.model_external_id = model_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.name = name
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.description = description

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            model_external_id=resource["modelExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            name=resource["name"],
            data_set_id=resource["dataSetId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutineList(CogniteResourceList[SimulatorRoutine]):
    _RESOURCE = SimulatorRoutine


class SimulatorRoutineRevisionsList(CogniteResourceList[SimulatorRoutineRevision]):
    _RESOURCE = SimulatorRoutineRevision


class SimulatorList(CogniteResourceList[Simulator]):
    _RESOURCE = Simulator


class SimulatorIntegrationList(CogniteResourceList[SimulatorIntegration]):
    _RESOURCE = SimulatorIntegration


class SimulatorModelList(CogniteResourceList[SimulatorModel]):
    _RESOURCE = SimulatorModel


class SimulatorModelRevisionList(CogniteResourceList[SimulatorModelRevision]):
    _RESOURCE = SimulatorModelRevision
