from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    CogniteSort,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

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

    def __init__(self, enabled: bool, cron_expression: str | None = None, **_: Any) -> None:
        self.enabled = enabled
        self.cron_expression = cron_expression

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulatorRoutineSchedule:
        return cls(
            enabled=resource["enabled"],
            cron_expression=resource.get("cronExpression"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        return output


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
            for input_ in resource["inputs"]:
                if isinstance(input_, SimulatorRoutineInputConstant) or isinstance(
                    input_, SimulatorRoutineInputTimeseries
                ):
                    inputs.append(input_)

                else:
                    if "value" in input_:
                        inputs.append(SimulatorRoutineInputConstant._load(input_, cognite_client))
                    else:
                        inputs.append(SimulatorRoutineInputTimeseries._load(input_, cognite_client))

        if resource.get("outputs", None) is not None:
            for output_ in resource["outputs"]:
                if isinstance(output_, SimulatorRoutineOutput):
                    outputs.append(output_)

                else:
                    outputs.append(SimulatorRoutineOutput._load(output_, cognite_client))

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["schedule"] = self.schedule.dump(camel_case=camel_case)
        output["dataSampling"] = self.data_sampling.dump(camel_case=camel_case)
        output["logicalCheck"] = [check_.dump(camel_case=camel_case) for check_ in self.logical_check]
        output["steadyStateDetection"] = [
            detection_.dump(camel_case=camel_case) for detection_ in self.steady_state_detection
        ]
        output["inputs"] = [input_.dump(camel_case=camel_case) for input_ in self.inputs]
        output["outputs"] = [output_.dump(camel_case=camel_case) for output_ in self.outputs]

        return output


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
            steps=[
                SimulatorRoutineStep._load(step_, cognite_client) if isinstance(step_, dict) else step_
                for step_ in resource["steps"]
            ],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["steps"] = [step_.dump(camel_case=camel_case) for step_ in self.steps]
        return output


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
    def __init__(
        self,
        external_id: str,
        model_external_id: str,
        simulator_integration_external_id: str,
        name: str,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            model_external_id=model_external_id,
            simulator_integration_external_id=simulator_integration_external_id,
            name=name,
            description=description,
        )

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