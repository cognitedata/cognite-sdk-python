from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
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
    enabled: bool = False
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
    enabled: bool = False
    validation_window: int | None = None
    sampling_window: int | None = None
    granularity: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineLogicalCheckEnabled(CogniteObject):
    enabled: bool = False
    timeseries_external_id: str | None = None
    aggregate: str | None = None
    operator: str | None = None
    value: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineSteadyStateDetectionEnabled(CogniteObject):
    enabled: bool = False
    timeseries_external_id: str | None = None
    aggregate: str | None = None
    min_section_size: int | None = None
    var_threshold: float | None = None
    slope_threshold: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

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
        inputs = []
        outputs = []

        if resource.get("inputs", None) is not None:
            inputs = [
                SimulatorRoutineInputConstant._load(input_, cognite_client)
                if "value" in input_
                else SimulatorRoutineInputTimeseries._load(input_, cognite_client)
                for input_ in resource["inputs"]
            ]

        if resource.get("outputs", None) is not None:
            outputs = [SimulatorRoutineOutput._load(output_, cognite_client) for output_ in resource["outputs"]]

        return cls(
            schedule=SimulatorRoutineSchedule.load(resource["schedule"], cognite_client),
            data_sampling=SimulatorRoutineDataSampling._load(resource["dataSampling"], cognite_client),
            logical_check=[
                SimulatorRoutineLogicalCheckEnabled._load(check_, cognite_client) for check_ in resource["logicalCheck"]
            ],
            steady_state_detection=[
                SimulatorRoutineSteadyStateDetectionEnabled._load(detection_, cognite_client)
                for detection_ in resource["steadyStateDetection"]
            ],
            inputs=inputs,
            outputs=outputs,
        )


@dataclass
class SimulatorRoutineStepArguments(CogniteObject):
    reference_id: str | None = None
    object_name: str | None = None
    object_property: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

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
    def _load_list(
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
    def _load_list(
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
    def _load_list(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> SimulatorStep | list[SimulatorStep]:
        if isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource]

        return cls._load(resource, cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["fields"] = [field_.dump(camel_case=camel_case) for field_ in self.fields]

        return output


class SimulatorCore(WriteableCogniteResource["SimulatorWrite"], ABC):
    """The simulator resource contains the definitions necessary for Cognite Data Fusion (CDF) to interact with a given simulator.

    It serves as a central contract that allows APIs, UIs, and integrations (connectors) to utilize the same definitions
    when dealing with a specific simulator.  Each simulator is uniquely identified and can be associated with various
    file extension types, model types, step fields, and unit quantities. Simulators are essential for managing data
    flows between CDF and external simulation tools, ensuring consistency and reliability in data handling.  ####
    Limitations:  - A project can have a maximum of 100 simulators

    This is the read/response format of the simulator.

    Args:
        external_id (str): External id of the simulator
        name (str): Name of the simulator
        file_extension_types (str | SequenceNotStr[str]): File extension types supported by the simulator
        model_types (SimulatorModelType | Sequence[SimulatorModelType] | None): Model types supported by the simulator
        step_fields (SimulatorStep | Sequence[SimulatorStep] | None): Step types supported by the simulator when creating routines
        unit_quantities (SimulatorQuantity | Sequence[SimulatorQuantity] | None): Quantities and their units supported by the simulator

    """

    def __init__(
        self,
        external_id: str,
        name: str,
        file_extension_types: str | SequenceNotStr[str],
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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            file_extension_types=resource["fileExtensionTypes"],
            model_types=SimulatorModelType._load_list(resource["modelTypes"], cognite_client)
            if "modelTypes" in resource
            else None,
            step_fields=SimulatorStep._load_list(resource["stepFields"], cognite_client)
            if "stepFields" in resource
            else None,
            unit_quantities=SimulatorQuantity._load_list(resource["unitQuantities"], cognite_client)
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


class SimulatorWrite(SimulatorCore):
    def __init__(
        self,
        external_id: str,
        name: str,
        file_extension_types: str | SequenceNotStr[str],
        model_types: SimulatorModelType | Sequence[SimulatorModelType] | None = None,
        step_fields: SimulatorStep | Sequence[SimulatorStep] | None = None,
        unit_quantities: SimulatorQuantity | Sequence[SimulatorQuantity] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            file_extension_types=file_extension_types,
            model_types=model_types,
            step_fields=step_fields,
            unit_quantities=unit_quantities,
        )

    def as_write(self) -> SimulatorWrite:
        """Returns a writeable version of this resource"""
        return self


class Simulator(SimulatorCore):
    def __init__(
        self,
        external_id: str,
        name: str,
        file_extension_types: str | SequenceNotStr[str],
        created_time: int | None = None,
        last_updated_time: int | None = None,
        id: int | None = None,
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
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        load = super()._load(resource, cognite_client)
        return cls(
            external_id=load.external_id,
            name=load.name,
            file_extension_types=load.file_extension_types,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            id=resource.get("id"),
            model_types=load.model_types,
            step_fields=load.step_fields,
            unit_quantities=load.unit_quantities,
        )

    def as_write(self) -> SimulatorWrite:
        """Returns a writeable version of this resource"""
        return SimulatorWrite(
            external_id=self.external_id,
            name=self.name,
            file_extension_types=self.file_extension_types,
            model_types=self.model_types,
            step_fields=self.step_fields,
            unit_quantities=self.unit_quantities,
        )

    def __hash__(self) -> int:
        return hash(self.external_id)


class SimulatorIntegrationCore(WriteableCogniteResource["SimulatorIntegrationWrite"], ABC):
    """
    The simulator integration resource represents a simulator connector in Cognite Data Fusion (CDF).
    It provides information about the configured connectors for a given simulator, including their status and additional
    details such as dataset, name, license status, connector version, simulator version, and more. This resource is essential
    for monitoring and managing the interactions between CDF and external simulators, ensuring proper data flow and integration.

    Limitations:  - A project can have a maximum of 100 simulators

    This is the read/response format of the simulator integration.

    Args:

        external_id (str): External id of the simulator integration
        simulator_external_id (str): External id of the associated simulator
        heartbeat (int): The interval in seconds between the last heartbeat and the current time
        data_set_id (int): The id of the dataset associated with the simulator integration
        connector_version (str): The version of the connector
        license_status (str | None): The status of the license
        simulator_version (str | None): The version of the simulator
        license_last_checked_time (int | None): The time when the license was last checked
        connector_status (str | None): The status of the connector
        connector_status_updated_time (int | None): The time when the connector status was last updated

    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        heartbeat: int,
        data_set_id: int,
        connector_version: str,
        license_status: str | None = None,
        simulator_version: str | None = None,
        license_last_checked_time: int | None = None,
        connector_status: str | None = None,
        connector_status_updated_time: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.heartbeat = heartbeat
        self.data_set_id = data_set_id
        self.connector_version = connector_version
        self.license_status = license_status
        self.simulator_version = simulator_version
        self.license_last_checked_time = license_last_checked_time
        self.connector_status = connector_status
        self.connector_status_updated_time = connector_status_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            heartbeat=resource["heartbeat"],
            data_set_id=resource["dataSetId"],
            connector_version=resource["connectorVersion"],
            license_status=resource.get("licenseStatus"),
            simulator_version=resource.get("simulatorVersion"),
            license_last_checked_time=resource.get("licenseLastCheckedTime"),
            connector_status=resource.get("connectorStatus"),
            connector_status_updated_time=resource.get("connectorStatusUpdatedTime"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorIntegrationWrite(SimulatorIntegrationCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        heartbeat: int,
        data_set_id: int,
        connector_version: str,
        license_status: str | None = None,
        simulator_version: str | None = None,
        license_last_checked_time: int | None = None,
        connector_status: str | None = None,
        connector_status_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            heartbeat=heartbeat,
            data_set_id=data_set_id,
            connector_version=connector_version,
            license_status=license_status,
            simulator_version=simulator_version,
            license_last_checked_time=license_last_checked_time,
            connector_status=connector_status,
            connector_status_updated_time=connector_status_updated_time,
        )

    def as_write(self) -> SimulatorIntegrationWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorIntegration(SimulatorIntegrationCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        heartbeat: int,
        data_set_id: int,
        connector_version: str,
        license_status: str | None = None,
        simulator_version: str | None = None,
        license_last_checked_time: int | None = None,
        connector_status: str | None = None,
        connector_status_updated_time: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        id: int | None = None,
        active: bool | None = None,
        log_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.heartbeat = heartbeat
        self.data_set_id = data_set_id
        self.connector_version = connector_version
        self.license_status = license_status
        self.simulator_version = simulator_version
        self.license_last_checked_time = license_last_checked_time
        self.connector_status = connector_status
        self.connector_status_updated_time = connector_status_updated_time
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time
        self.active = active
        self.log_id = log_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        load = super()._load(resource, cognite_client)
        return cls(
            external_id=load.external_id,
            simulator_external_id=load.simulator_external_id,
            heartbeat=load.heartbeat,
            data_set_id=load.data_set_id,
            connector_version=load.connector_version,
            license_status=load.license_status,
            simulator_version=load.simulator_version,
            license_last_checked_time=load.license_last_checked_time,
            connector_status=load.connector_status,
            connector_status_updated_time=load.connector_status_updated_time,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            id=resource.get("id"),
            active=resource.get("active"),
            log_id=resource.get("logId"),
        )

    def as_write(self) -> SimulatorIntegrationWrite:
        """Returns a writeable version of this resource"""
        return SimulatorIntegrationWrite(
            external_id=self.external_id,
            simulator_external_id=self.simulator_external_id,
            heartbeat=self.heartbeat,
            data_set_id=self.data_set_id,
            connector_version=self.connector_version,
            license_status=self.license_status,
            simulator_version=self.simulator_version,
            license_last_checked_time=self.license_last_checked_time,
            connector_status=self.connector_status,
            connector_status_updated_time=self.connector_status_updated_time,
        )

    def __hash__(self) -> int:
        return hash(self.external_id)


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
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore

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

    def __hash__(self) -> int:
        return hash(self.external_id)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case)


class SimulatorRoutineRevisionCore(WriteableCogniteResource["SimulatorRoutineRevisionWrite"], ABC):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        routine_external_id: str,
        simulator_integration_external_id: str,
        model_external_id: str,
        data_set_id: int,
        configuration: SimulatorRoutineConfiguration,
        script: list[SimulatorRoutineStage],
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.routine_external_id = routine_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.configuration = configuration
        self.script = script

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        script = []

        if resource.get("script", None) is not None:
            script = [SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource["script"]]
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            routine_external_id=resource["routineExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            model_external_id=resource["modelExternalId"],
            data_set_id=resource["dataSetId"],
            configuration=SimulatorRoutineConfiguration._load(resource["configuration"], cognite_client),
            script=script,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["configuration"] = self.configuration.dump(camel_case=camel_case)
        output["script"] = [stage_.dump(camel_case=camel_case) for stage_ in self.script]

        return output


class SimulatorRoutineRevisionWrite(SimulatorRoutineRevisionCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        routine_external_id: str,
        simulator_integration_external_id: str,
        model_external_id: str,
        data_set_id: int,
        configuration: SimulatorRoutineConfiguration,
        script: list[SimulatorRoutineStage],
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            routine_external_id=routine_external_id,
            simulator_integration_external_id=simulator_integration_external_id,
            model_external_id=model_external_id,
            data_set_id=data_set_id,
            configuration=configuration,
            script=script,
        )

    def as_write(self) -> SimulatorRoutineRevisionWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutineRevision(SimulatorRoutineRevisionCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        routine_external_id: str,
        simulator_integration_external_id: str,
        model_external_id: str,
        data_set_id: int,
        configuration: SimulatorRoutineConfiguration,
        script: list[SimulatorRoutineStage],
        id: int | None = None,
        created_by_user_id: str | None = None,
        last_updated_time: int | None = None,
        version_number: int | None = None,
        created_time: int | None = None,
        log_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.routine_external_id = routine_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.created_by_user_id = created_by_user_id
        self.configuration = configuration
        self.script = script

        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time
        self.version_number = version_number
        self.log_id = log_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        load = super()._load(resource, cognite_client)
        return cls(
            external_id=load.external_id,
            simulator_external_id=load.simulator_external_id,
            routine_external_id=load.routine_external_id,
            simulator_integration_external_id=load.simulator_integration_external_id,
            model_external_id=load.model_external_id,
            data_set_id=load.data_set_id,
            created_by_user_id=resource.get("createdByUserId"),
            configuration=load.configuration,
            script=load.script,
            id=resource.get("id"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            version_number=resource.get("versionNumber"),
            log_id=resource.get("logId"),
        )

    def as_write(self) -> SimulatorRoutineRevisionWrite:
        """Returns a writeable version of this resource"""
        return SimulatorRoutineRevisionWrite(
            external_id=self.external_id,
            simulator_external_id=self.simulator_external_id,
            routine_external_id=self.routine_external_id,
            simulator_integration_external_id=self.simulator_integration_external_id,
            model_external_id=self.model_external_id,
            data_set_id=self.data_set_id,
            configuration=self.configuration,
            script=self.script,
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
        external_id (str | None): External id of the simulator model
        simulator_external_id (str | None): External id of the associated simulator
        data_set_id (int | None): The id of the dataset associated with the simulator model
        name (str | None): The name of the simulator model
        type (str | None): The type key of the simulator model
        description (str | None): The description of the simulator model
    """

    def __init__(
        self,
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        data_set_id: int | None = None,
        name: str | None = None,
        type: str | None = None,
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
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        data_set_id: int | None = None,
        name: str | None = None,
        type: str | None = None,
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
            external_id=resource.get("externalId"),
            simulator_external_id=resource.get("simulatorExternalId"),
            data_set_id=resource.get("dataSetId"),
            name=resource.get("name"),
            type=resource.get("type"),
            description=resource.get("description"),
        )

    def as_write(self) -> SimulatorModelWrite:
        """Returns self."""
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
        external_id (str | None): External id of the simulator model
        simulator_external_id (str | None): External id of the associated simulator
        data_set_id (int | None): The id of the dataset associated with the simulator model
        name (str | None): The name of the simulator model
        id (int | None): A unique id of a simulator model
        type (str | None): The type key of the simulator model
        description (str | None): The description of the simulator model
        created_time (int | None): The time when the simulator model was created
        last_updated_time (int | None): The time when the simulator model was last updated
    """

    def __init__(
        self,
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        data_set_id: int | None = None,
        name: str | None = None,
        id: int | None = None,
        type: str | None = None,
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
            id=resource.get("id"),
            type=resource.get("type"),
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

    def __hash__(self) -> int:
        return hash(self.external_id)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case)


class SimulationRunCore(CogniteResource, ABC):
    """"""

    """_summary_

    Returns:
        _type_: _description_
    """

    def __init__(
        self,
        simulator_external_id: str | None = None,
        simulator_integration_external_id: str | None = None,
        model_external_id: str | None = None,
        model_revision_external_id: str | None = None,
        routine_external_id: str | None = None,
        routine_revision_external_id: str | None = None,
        run_time: int | None = None,
        simulation_time: int | None = None,
        status: str | None = None,
        status_message: str | None = None,
        data_set_id: int | None = None,
        run_type: str | None = None,
        user_id: str | None = None,
        log_id: int | None = None,
    ) -> None:
        self.simulator_external_id = simulator_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.model_revision_external_id = model_revision_external_id
        self.routine_external_id = routine_external_id
        self.routine_revision_external_id = routine_revision_external_id
        self.run_time = run_time
        self.simulation_time = simulation_time
        self.status = status
        self.status_message = status_message
        self.data_set_id = data_set_id
        self.run_type = run_type
        self.user_id = user_id
        self.log_id = log_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case)


class SimulationRun(SimulationRunCore):
    """
    Every time a simulation routine executes, a simulation run object is created.
    This object ensures that each execution of a routine is documented and traceable.
    Each run has an associated simulation data resource, which stores the inputs and outputs of a
    simulation run, capturing the values set into and read from the simulator model to ensure
    the traceability and integrity of the simulation data.

    Simulation runs provide a historical record of the simulations performed, allowing users to analyze
    and compare different runs, track changes over time, and make informed decisions based on the simulation results.

    Limitations:
    * A retention policy is in place for simulation runs, allowing up to 100000 entries.
    * Once this limit is reached, the oldest runs will be deleted to accommodate new runs.

    This is the read/response format of a simulation run.

    Args:
        simulator_external_id (str | None): External id of the associated simulator
        simulator_integration_external_id (str | None): External id of the associated simulator integration
        model_external_id (str | None): External id of the associated simulator model
        model_revision_external_id (str | None): External id of the associated simulator model revision
        routine_external_id (str | None): External id of the associated simulator routine
        routine_revision_external_id (str | None): External id of the associated simulator routine revision
        run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
        simulation_time (int | None): Simulation time in milliseconds. Timestamp when the input data was sampled. Used for indexing input and output time series.
        status (str | None): The status of the simulation run
        status_message (str | None): The status message of the simulation run
        data_set_id (int | None): The id of the dataset associated with the simulation run
        run_type (str | None): The type of the simulation run
        user_id (str | None): The id of the user who executed the simulation run
        log_id (int | None): The id of the log associated with the simulation run
        id (int | None): A unique id of a simulation run
        created_time (int | None): The number of milliseconds since epoch
        last_updated_time (int | None): The number of milliseconds since epoch

    """

    def __init__(
        self,
        simulator_external_id: str | None = None,
        simulator_integration_external_id: str | None = None,
        model_external_id: str | None = None,
        model_revision_external_id: str | None = None,
        routine_external_id: str | None = None,
        routine_revision_external_id: str | None = None,
        run_time: int | None = None,
        simulation_time: int | None = None,
        status: str | None = None,
        status_message: str | None = None,
        data_set_id: int | None = None,
        run_type: str | None = None,
        user_id: str | None = None,
        log_id: int | None = None,
        id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            simulator_external_id=simulator_external_id,
            simulator_integration_external_id=simulator_integration_external_id,
            model_external_id=model_external_id,
            model_revision_external_id=model_revision_external_id,
            routine_external_id=routine_external_id,
            routine_revision_external_id=routine_revision_external_id,
            run_time=run_time,
            simulation_time=simulation_time,
            status=status,
            status_message=status_message,
            data_set_id=data_set_id,
            run_type=run_type,
            user_id=user_id,
            log_id=log_id,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SimulationRun:
        return super()._load(resource, cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)

    def __hash__(self) -> int:
        return hash(self.id)


class SimulatorRoutineCore(WriteableCogniteResource["SimulatorRoutineWrite"], ABC):
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
        external_id (str): External id of the simulator routine
        simulator_external_id (str): External id of the associated simulator
        model_external_id (str): External id of the associated simulator model
        simulator_integration_external_id (str): External id of the associated simulator integration
        name (str): The name of the simulator routine
        data_set_id (int): The id of the dataset associated with the simulator routine
        description (str | None): The description of the simulator routine
    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.model_external_id = model_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.name = name
        self.data_set_id = data_set_id
        self.description = description

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
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorRoutineWrite(SimulatorRoutineCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            model_external_id=model_external_id,
            simulator_integration_external_id=simulator_integration_external_id,
            name=name,
            data_set_id=data_set_id,
            description=description,
        )

    def as_write(self) -> SimulatorRoutineWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutine(SimulatorRoutineCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        data_set_id: int,
        description: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.model_external_id = model_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.name = name
        self.data_set_id = data_set_id
        self.description = description
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.last_updated_time: int | None = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            model_external_id=resource["modelExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            name=resource["name"],
            data_set_id=resource["dataSetId"],
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
            simulator_external_id=self.simulator_external_id,
            model_external_id=self.model_external_id,
            simulator_integration_external_id=self.simulator_integration_external_id,
            name=self.name,
            data_set_id=self.data_set_id,
            description=self.description,
        )

    def __hash__(self) -> int:
        return hash(self.external_id)


class SimulatorRoutineRevisionWriteList(CogniteResourceList[SimulatorRoutineRevisionWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorRoutineRevisionWrite


class SimulatorRoutineRevisionList(
    WriteableCogniteResourceList[SimulatorRoutineRevisionWrite, SimulatorRoutineRevision], IdTransformerMixin
):
    _RESOURCE = SimulatorRoutineRevision

    def as_write(self) -> SimulatorRoutineRevisionWriteList:
        return SimulatorRoutineRevisionWriteList(
            [a.as_write() for a in self.data], cognite_client=self._get_cognite_client()
        )


class SimulatorRoutineWriteList(CogniteResourceList[SimulatorRoutineWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorRoutineWrite


class SimulatorRoutineList(WriteableCogniteResourceList[SimulatorRoutineWrite, SimulatorRoutine], IdTransformerMixin):
    _RESOURCE = SimulatorRoutine

    def as_write(self) -> SimulatorRoutineWriteList:
        return SimulatorRoutineWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())


class SimulatorRoutineRevisionsList(
    WriteableCogniteResourceList[SimulatorRoutineRevisionWrite, SimulatorRoutineRevision], IdTransformerMixin
):
    _RESOURCE = SimulatorRoutineRevision

    def as_write(self) -> SimulatorRoutineRevisionWriteList:
        return SimulatorRoutineRevisionWriteList(
            [a.as_write() for a in self.data], cognite_client=self._get_cognite_client()
        )


class SimulatorWriteList(CogniteResourceList[SimulatorWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorWrite


class SimulatorList(WriteableCogniteResourceList[SimulatorWrite, Simulator], IdTransformerMixin):
    _RESOURCE = Simulator

    def as_write(self) -> SimulatorWriteList:
        return SimulatorWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())


class SimulatorIntegrationWriteList(CogniteResourceList[SimulatorIntegrationWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorIntegrationWrite


class SimulatorIntegrationList(
    WriteableCogniteResourceList[SimulatorIntegrationWrite, SimulatorIntegration], IdTransformerMixin
):
    _RESOURCE = SimulatorIntegration

    def as_write(self) -> SimulatorIntegrationWriteList:
        return SimulatorIntegrationWriteList(
            [a.as_write() for a in self.data], cognite_client=self._get_cognite_client()
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


class SimulationRunsList(CogniteResourceList[SimulationRun], InternalIdTransformerMixin):
    _RESOURCE = SimulationRun