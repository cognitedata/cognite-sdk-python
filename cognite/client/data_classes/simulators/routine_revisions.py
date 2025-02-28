from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
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
    inputs: list[SimulatorRoutineInputConstant | SimulatorRoutineInputTimeseries] | None
    outputs: list[SimulatorRoutineOutput] | None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        inputs = None
        outputs = None

        if resource.get("inputs", None) is not None:
            inputs = []
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
            outputs = []
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
        output["inputs"] = [input_.dump(camel_case=camel_case) for input_ in self.inputs] if self.inputs else None
        output["outputs"] = [output_.dump(camel_case=camel_case) for output_ in self.outputs] if self.outputs else None

        return output


@dataclass
class SimulatorRoutineStepArguments(CogniteObject, dict[str, str]):  # TODO: fix type
    reference_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineStep(CogniteObject):
    step_type: str
    arguments: SimulatorRoutineStepArguments
    order: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            step_type=resource["stepType"],
            arguments=resource["arguments"],
            order=resource["order"],
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


class SimulatorRoutineRevisionCore(WriteableCogniteResource["SimulatorRoutineRevisionWrite"], ABC):
    def __init__(
        self,
        external_id: str | None = None,
        routine_external_id: str | None = None,
        configuration: SimulatorRoutineConfiguration | None = None,
        script: list[SimulatorRoutineStage] | None = None,
    ) -> None:
        self.external_id = external_id
        self.routine_external_id = routine_external_id
        self.configuration = configuration
        self.script = script

    @classmethod
    def _load(
        cls: type[T_Routine_Revision], resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> T_Routine_Revision:
        script = []

        if resource.get("script", None) is not None:
            script = [SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource["script"]]
        return cls(
            external_id=resource["externalId"],
            routine_external_id=resource["routineExternalId"],
            configuration=SimulatorRoutineConfiguration._load(resource["configuration"], cognite_client),
            script=script,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["configuration"] = self.configuration.dump(camel_case=camel_case) if self.configuration else None
        output["script"] = [stage_.dump(camel_case=camel_case) for stage_ in self.script] if self.script else None

        return output


T_Routine_Revision = TypeVar("T_Routine_Revision", bound="SimulatorRoutineRevisionCore")


class SimulatorRoutineRevisionWrite(SimulatorRoutineRevisionCore):
    def __init__(
        self,
        external_id: str | None = None,
        routine_external_id: str | None = None,
        configuration: SimulatorRoutineConfiguration | None = None,
        script: list[SimulatorRoutineStage] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            routine_external_id=routine_external_id,
            configuration=configuration,
            script=script,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SimulatorRoutineRevisionWrite:
        return cls(
            external_id=resource.get("externalId"),
            routine_external_id=resource.get("routineExternalId"),
            configuration=SimulatorRoutineConfiguration._load(resource.get("configuration", {}), cognite_client),
            script=[SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource.get("script", [])],
        )

    def as_write(self) -> SimulatorRoutineRevisionWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutineRevision(SimulatorRoutineRevisionCore):
    def __init__(
        self,
        configuration: SimulatorRoutineConfiguration | None = None,
        script: list[SimulatorRoutineStage] | None = None,
        simulator_external_id: str | None = None,
        external_id: str | None = None,
        simulator_integration_external_id: str | None = None,
        model_external_id: str | None = None,
        data_set_id: int | None = None,
        id: int | None = None,
        routine_external_id: str | None = None,
        created_by_user_id: str | None = None,
        version_number: int | None = None,
        created_time: int | None = None,
        log_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.routine_external_id: str | None = routine_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.created_by_user_id = created_by_user_id
        self.configuration = configuration
        self.script = script

        # id/created_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int | None = id
        self.created_time: int | None = created_time
        self.version_number = version_number
        self.log_id = log_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulatorRoutineRevision:
        load = super()._load(resource, cognite_client)
        return cls(
            external_id=load.external_id,
            simulator_external_id=resource.get("simulatorExternalId"),
            routine_external_id=load.routine_external_id,
            simulator_integration_external_id=resource.get("simulatorIntegrationExternalId"),
            model_external_id=resource.get("modelExternalId"),
            data_set_id=resource.get("dataSetId"),
            created_by_user_id=resource.get("createdByUserId"),
            configuration=load.configuration,
            script=load.script,
            id=resource.get("id"),
            created_time=resource.get("createdTime"),
            version_number=resource.get("versionNumber"),
            log_id=resource.get("logId"),
        )

    def as_write(self) -> SimulatorRoutineRevisionWrite:
        """Returns a writeable version of this resource"""
        return SimulatorRoutineRevisionWrite(
            external_id=self.external_id,
            routine_external_id=self.routine_external_id,
            configuration=self.configuration,
            script=self.script,
        )


class SimulatorRoutineRevisionWriteList(CogniteResourceList[SimulatorRoutineRevisionWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorRoutineRevisionWrite


class SimulatorRoutineRevisionsList(
    WriteableCogniteResourceList[SimulatorRoutineRevisionWrite, SimulatorRoutineRevision], IdTransformerMixin
):
    _RESOURCE = SimulatorRoutineRevision

    def as_write(self) -> SimulatorRoutineRevisionWriteList:
        return SimulatorRoutineRevisionWriteList(
            [a.as_write() for a in self.data], cognite_client=self._get_cognite_client()
        )
