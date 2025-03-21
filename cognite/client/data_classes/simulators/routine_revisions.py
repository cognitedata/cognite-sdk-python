from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class SimulationValueUnitInput(CogniteObject):
    """
    The unit of the simulation value.

    Args:
        name (str): The name of the unit.
        quantity (str | None): The quantity of the unit.
    """

    name: str
    quantity: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            quantity=resource.get("quantity"),
        )


@dataclass
class SimulatorRoutineInput(CogniteObject):
    """
    The input of the simulator routine revision.

    Args:
        name (str): The name of the input.
        reference_id (str): The reference ID of the input.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the input. If not provided, the input is not saved to a timeseries.
        unit (SimulationValueUnitInput | None): The unit of the input.
    """

    _type: ClassVar[str]

    name: str
    reference_id: str
    save_timeseries_external_id: str | None = None
    unit: SimulationValueUnitInput | None = None

    @classmethod
    @abstractmethod
    def _load_input(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        is_constant = resource.get("value")
        is_timeseries = resource.get("sourceExternalId")
        type_ = "constant" if is_constant else "timeseries" if is_timeseries else None
        if type_ is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        input_class = _INPUT_CLASS_BY_TYPE.get(type_)
        if type_ is None or input_class is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, input_class._load_input(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)
        return output


@dataclass
class SimulatorRoutineInputTimeseries(SimulatorRoutineInput):
    """
    The timeseries input of the simulator routine revision.

    Args:
        name (str): The name of the input.
        reference_id (str): The reference ID of the input.
        source_external_id (str): The external ID of the source timeseries.
        aggregate (Literal["average", "interpolation", "stepInterpolation"] | None): The aggregation method to use for the timeseries.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the input. If not provided, the input is not saved to a timeseries.
        unit (SimulationValueUnitInput | None): The unit of the input.
    """

    _type = "timeseries"

    def __init__(
        self,
        name: str,
        reference_id: str,
        source_external_id: str,
        aggregate: Literal["average", "interpolation", "stepInterpolation"] | None = None,
        save_timeseries_external_id: str | None = None,
        unit: SimulationValueUnitInput | None = None,
    ) -> None:
        super().__init__(name, reference_id, save_timeseries_external_id, unit)
        self.source_external_id = source_external_id
        self.aggregate = aggregate

    @classmethod
    def _load_input(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            reference_id=resource["referenceId"],
            source_external_id=resource["sourceExternalId"],
            aggregate=resource.get("aggregate"),
            save_timeseries_external_id=resource.get("saveTimeseriesExternalId"),
            unit=SimulationValueUnitInput._load(resource["unit"], cognite_client) if "unit" in resource else None,
        )


@dataclass
class SimulatorRoutineInputConstant(SimulatorRoutineInput):
    """
    The constant input of the simulator routine revision.

    Args:
        name (str): The name of the input.
        reference_id (str): The reference ID of the input.
        value (str | int | float | list[str] | list[int] | list[float]): The value of the input.
        value_type (Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"]): The value type of the input.
        unit (SimulationValueUnitInput | None): The unit of the input.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the input. If not provided, the input is not saved to a timeseries.
    """

    _type = "constant"

    def __init__(
        self,
        name: str,
        reference_id: str,
        value: str | int | float | list[str] | list[int] | list[float],
        value_type: Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"],
        unit: SimulationValueUnitInput | None = None,
        save_timeseries_external_id: str | None = None,
    ) -> None:
        super().__init__(name, reference_id, save_timeseries_external_id, unit)
        self.value = value
        self.value_type = value_type

    @classmethod
    def _load_input(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            reference_id=resource["referenceId"],
            value=resource["value"],
            value_type=resource["valueType"],
            unit=SimulationValueUnitInput._load(resource["unit"], cognite_client) if "unit" in resource else None,
            save_timeseries_external_id=resource.get("saveTimeseriesExternalId"),
        )


@dataclass
class SimulatorRoutineOutput(CogniteObject):
    """
    The output of the simulator routine revision.

    Args:
        name (str): The name of the output.
        reference_id (str): The reference ID of the output.
        value_type (str): The value type of the output.
        unit (SimulationValueUnitInput | None): The unit of the output.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the output. If not provided, the output is not saved to a timeseries.
    """

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
    """
    The schedule configuration of the simulator routine revision.

    Args:
        cron_expression (str): The cron expression of the schedule.
    """

    cron_expression: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulatorRoutineSchedule:
        return cls(
            cron_expression=resource["cronExpression"],
        )


@dataclass
class SimulatorRoutineDataSampling(CogniteObject):
    """
    The data sampling configuration of the simulator routine revision.
    Learn more about data sampling <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling>.

    Args:
        sampling_window (int): Sampling window of the data sampling. Represented in minutes
        granularity (str): The granularity of the data sampling.
        validation_window (int | None): Validation window of the data sampling. Represented in minutes. Used when either logical check or steady state detection is enabled.
    """

    sampling_window: int
    granularity: str
    validation_window: int | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            sampling_window=resource["samplingWindow"],
            granularity=resource["granularity"],
            validation_window=resource.get("validationWindow"),
        )


@dataclass
class SimulatorRoutineLogicalCheck(CogniteObject):
    """
    The logical check configuration of the simulator routine revision.
    Learn more about logical checks <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling/#data-validation-methods>.

    Args:
        aggregate (Literal["average", "interpolation", "stepInterpolation"]): The aggregation method to use for the time series.
        operator (Literal["eq", "ne", "gt", "ge", "lt", "le"]): The operator to use for the logical check.
        value (float): The value to use for the logical check.
        timeseries_external_id (str | None): The external ID of the time series to check.
    """

    aggregate: Literal["average", "interpolation", "stepInterpolation"]
    operator: Literal["eq", "ne", "gt", "ge", "lt", "le"]
    value: float
    timeseries_external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            aggregate=resource["aggregate"],
            operator=resource["operator"],
            value=resource["value"],
            timeseries_external_id=resource.get("timeseriesExternalId"),
        )


@dataclass
class SimulatorRoutineSteadyStateDetection(CogniteObject):
    """
    Steady State Detection checks for steady state regions in a given time series.
    The user specifies the time series and three parameters: min section size, var threshold, and slope threshold.
    It returns a binary time series, with 1 for timestamps where the steady state criteria is met and 0 otherwise.

    Args:
        aggregate (Literal["average", "interpolation", "stepInterpolation"]): The aggregation method to use for the time series.
        min_section_size (int): The minimum number of consecutive data points that must meet the steady state criteria.
        var_threshold (float): The maximum variance allowed for the steady state region.
        slope_threshold (float): The maximum slope allowed for the steady state region.
        timeseries_external_id (str | None): The external ID of the time series to check.
    """

    aggregate: Literal["average", "interpolation", "stepInterpolation"]
    min_section_size: int
    var_threshold: float
    slope_threshold: float
    timeseries_external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            aggregate=resource["aggregate"],
            min_section_size=resource["minSectionSize"],
            var_threshold=resource["varThreshold"],
            slope_threshold=resource["slopeThreshold"],
            timeseries_external_id=resource.get("timeseriesExternalId"),
        )


@dataclass
class SimulatorRoutineConfiguration(CogniteObject):
    """
    The simulator routine configuration defines the configuration of a simulator routine revision.
    Learn more about simulator routine configuration <https://docs.cognite.com/cdf/integration/guides/simulators/simulator_routines>.

    Args:
        inputs (list[SimulatorRoutineInput] | None): The inputs of the simulator routine revision. Each element can be either a constant or a timeseries input.
        outputs (list[SimulatorRoutineOutput] | None): The outputs of the simulator routine revision.
        logical_check (list[SimulatorRoutineLogicalCheck] | None): Logical check configuration.
        steady_state_detection (list[SimulatorRoutineSteadyStateDetection] | None): Steady state detection configuration.
        schedule (SimulatorRoutineSchedule | None): Schedule configuration.
        data_sampling (SimulatorRoutineDataSampling | None): Data sampling configuration. Learn more about data sampling <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling>.
    """

    inputs: list[SimulatorRoutineInput] | None
    outputs: list[SimulatorRoutineOutput] | None
    logical_check: list[SimulatorRoutineLogicalCheck]
    steady_state_detection: list[SimulatorRoutineSteadyStateDetection]
    schedule: SimulatorRoutineSchedule | None = None
    data_sampling: SimulatorRoutineDataSampling | None = None

    def __init__(
        self,
        inputs: list[SimulatorRoutineInput] | None,
        outputs: list[SimulatorRoutineOutput] | None,
        logical_check: list[SimulatorRoutineLogicalCheck] | None = None,
        steady_state_detection: list[SimulatorRoutineSteadyStateDetection] | None = None,
        schedule: SimulatorRoutineSchedule | None = None,
        data_sampling: SimulatorRoutineDataSampling | None = None,
    ) -> None:
        self.inputs = inputs
        self.outputs = outputs
        self.logical_check = logical_check or []
        self.steady_state_detection = steady_state_detection or []
        self.schedule = schedule
        self.data_sampling = data_sampling

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        inputs = None
        outputs = None

        if resource.get("inputs", None) is not None:
            inputs = []
            for input in resource["inputs"]:
                if issubclass(type(input), type(SimulatorRoutineInput)):
                    inputs.append(input)
                else:
                    inputs.append(SimulatorRoutineInput._load(input, cognite_client))

        if resource.get("outputs", None) is not None:
            outputs = []
            for output_ in resource["outputs"]:
                if isinstance(output_, SimulatorRoutineOutput):
                    outputs.append(output_)
                else:
                    outputs.append(SimulatorRoutineOutput._load(output_, cognite_client))

        schedule = resource["schedule"] if resource.get("schedule") and resource["schedule"]["enabled"] else None
        data_sampling = (
            resource["dataSampling"] if resource.get("dataSampling") and resource["dataSampling"]["enabled"] else None
        )

        return cls(
            schedule=SimulatorRoutineSchedule.load(schedule, cognite_client) if schedule else None,
            data_sampling=SimulatorRoutineDataSampling._load(data_sampling, cognite_client) if data_sampling else None,
            logical_check=[
                SimulatorRoutineLogicalCheck._load(check_, cognite_client) for check_ in resource["logicalCheck"]
            ],
            steady_state_detection=[
                SimulatorRoutineSteadyStateDetection._load(detection_, cognite_client)
                for detection_ in resource["steadyStateDetection"]
            ],
            inputs=inputs,
            outputs=outputs,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["inputs"] = [input_.dump(camel_case=camel_case) for input_ in self.inputs] if self.inputs else None
        output["outputs"] = [output_.dump(camel_case=camel_case) for output_ in self.outputs] if self.outputs else None

        if self.schedule is None:
            output["schedule"] = {"enabled": False}
        else:
            output["schedule"] = {"enabled": True, **self.schedule.dump(camel_case=camel_case)}

        if self.data_sampling is None:
            output["dataSampling"] = {"enabled": False}
        else:
            output["dataSampling"] = {"enabled": True, **self.data_sampling.dump(camel_case=camel_case)}

        output["logicalCheck"] = [
            {"enabled": True, **check_.dump(camel_case=camel_case)} for check_ in self.logical_check
        ]
        output["steadyStateDetection"] = [
            {"enabled": True, **detection_.dump(camel_case=camel_case)} for detection_ in self.steady_state_detection
        ]

        return output


@dataclass
class SimulatorRoutineStepArguments(CogniteObject):
    """
    The arguments of the simulator routine step.

    Depending on the step type and simulator, the arguments can be different.
    For "Get" and "Set" step type the reference ID is required.
    """

    data: dict[str, str]

    def __init__(self, data: dict[str, str]) -> None:
        self.data = data

    def __getitem__(self, key: str) -> str:
        return self.data[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.data[key] = value

    def __delitem__(self, key: str) -> None:
        del self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def keys(self) -> list[str]:
        return list(self.data.keys())

    def values(self) -> list[str]:
        return list(self.data.values())

    def items(self) -> list[tuple[str, str]]:
        return list(self.data.items())

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SimulatorRoutineStepArguments):
            return False
        return self.data == other.data

    def __repr__(self) -> str:
        return self.data.__repr__()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        data = {to_snake_case(key): val for key, val in resource.items()}
        return cls(data=data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {**(convert_all_keys_to_camel_case(self.data) if camel_case else self.data)}


@dataclass
class SimulatorRoutineStep(CogniteObject):
    """
    The step of the simulator routine revision.

    Args:
        step_type (str): The type of the step. Can be "Get", "Set", or "Command".
        arguments (SimulatorRoutineStepArguments): The arguments of the step.
        order (int): The order of the step.
    """

    step_type: Literal["Get", "Set", "Command"]
    arguments: SimulatorRoutineStepArguments
    order: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            step_type=resource["stepType"],
            arguments=SimulatorRoutineStepArguments._load(resource["arguments"], cognite_client),
            order=resource["order"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["arguments"] = self.arguments.dump(camel_case=camel_case)
        return output


@dataclass
class SimulatorRoutineStage(CogniteObject):
    """
    The stage of the simulator routine revision. This is a way to organize the steps of the simulator routine revision.

    Args:
        order (int): The order of the stage.
        steps (list[SimulatorRoutineStep]): The steps of the stage.
        description (str | None): The description of the stage.
    """

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
        external_id: str,
        routine_external_id: str,
        configuration: SimulatorRoutineConfiguration | None = None,
        script: list[SimulatorRoutineStage] | None = None,
    ) -> None:
        self.external_id = external_id
        self.routine_external_id = routine_external_id
        self.configuration = configuration
        self.script = script

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["configuration"] = self.configuration.dump(camel_case=camel_case) if self.configuration else None
        output["script"] = [stage_.dump(camel_case=camel_case) for stage_ in self.script] if self.script else None

        return output


class SimulatorRoutineRevisionWrite(SimulatorRoutineRevisionCore):
    """
    The simulator routine resource defines instructions on interacting with a simulator model.
    This is a writeable version of a simulator routine revision, it is used when creating simulator routine revisions.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        routine_external_id (str): The external ID of the simulator routine.
        configuration (SimulatorRoutineConfiguration | None): The configuration of the simulator routine revision.
        script (list[SimulatorRoutineStage] | None): The script of the simulator routine revision.

    """

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        configuration = (
            SimulatorRoutineConfiguration._load(resource.get("configuration", {}), cognite_client)
            if resource.get("configuration")
            else None
        )
        script = (
            [SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource.get("script", [])]
            if resource.get("script")
            else None
        )
        return cls(
            external_id=resource["externalId"],
            routine_external_id=resource["routineExternalId"],
            configuration=configuration,
            script=script,
        )

    def as_write(self) -> SimulatorRoutineRevisionWrite:
        """Returns a writeable version of this resource"""
        return self


class SimulatorRoutineRevision(SimulatorRoutineRevisionCore):
    """
    The simulator routine resource defines instructions on interacting with a simulator model.

    A simulator routine includes:

        Inputs (values set into the simulator model)
        Commands (actions to be performed by the simulator)
        Outputs (values read from the simulator model)

    Simulator routines can have multiple revisions, enabling users to track changes and evolve the routine over time.
    Each model can have multiple routines, each performing different objectives such as calculating optimal operation setpoints, forecasting production, benchmarking asset performance, and more.

    Args:
        id (int): The unique identifier of the simulator routine revision.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        simulator_external_id (str): The external ID of the simulator.
        simulator_integration_external_id (str): The external ID of the simulator integration.
        routine_external_id (str): The external ID of the simulator routine.
        model_external_id (str): The external ID of the simulator model.
        version_number (int): The version number of the simulator routine revision. Unique for each simulator routine.
        created_time (int): The timestamp of when the simulator routine revision was created.
        data_set_id (int): The ID of the data set associated with the simulator routine revision.
        created_by_user_id (str): The ID of the user who created the simulator routine revision.
        configuration (SimulatorRoutineConfiguration | None): The configuration of the simulator routine revision.
        script (list[SimulatorRoutineStage] | None): The script of the simulator routine revision.
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        simulator_external_id: str,
        simulator_integration_external_id: str,
        routine_external_id: str,
        model_external_id: str,
        version_number: int,
        created_time: int,
        data_set_id: int,
        created_by_user_id: str,
        configuration: SimulatorRoutineConfiguration | None = None,
        script: list[SimulatorRoutineStage] | None = None,
    ) -> None:
        super().__init__(external_id, routine_external_id, configuration, script)

        self.id = id
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.data_set_id = data_set_id
        self.created_by_user_id = created_by_user_id
        self.created_time = created_time
        self.version_number = version_number

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulatorRoutineRevision:
        configuration = (
            SimulatorRoutineConfiguration._load(resource.get("configuration", {}))
            if resource.get("configuration")
            else None
        )
        script = (
            [SimulatorRoutineStage._load(stage_, cognite_client) for stage_ in resource.get("script", [])]
            if resource.get("script")
            else None
        )
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            routine_external_id=resource["routineExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            model_external_id=resource["modelExternalId"],
            data_set_id=resource["dataSetId"],
            created_by_user_id=resource["createdByUserId"],
            configuration=configuration,
            script=script,
            created_time=resource["createdTime"],
            version_number=resource["versionNumber"],
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


_INPUT_CLASS_BY_TYPE: dict[str, type[SimulatorRoutineInput]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in itertools.chain(SimulatorRoutineInput.__subclasses__())
}
