from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
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
class SimulatorRoutineStepArguments(CogniteObject, dict, MutableMapping[str, str]):
    """
    The arguments of the simulator routine step.

    Depending on the step type and simulator, the arguments can be different.
    For "Get" and "Set" step type the reference ID is required.
    """

    def __init__(self, data: dict[str, str]) -> None:
        super().__init__(data)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {key: value for key, value in self.items()}


_INPUT_CLASS_BY_TYPE: dict[str, type[SimulatorRoutineInput]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in itertools.chain(SimulatorRoutineInput.__subclasses__())
}
