from __future__ import annotations

from abc import ABC
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulatorRoutineInputTimeseries(CogniteObject):
    """
    The timeseries input of the simulator routine revision.

    Args:
        name (str): The name of the input.
        reference_id (str): The reference ID of the input.
        source_external_id (str): The external ID of the source timeseries.
        aggregate (str | None): The aggregation method to use for the timeseries.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the input. If not provided, the input is not saved to a timeseries.
        unit (SimulationValueUnitInput | None): The unit of the input.
    """

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
    """
    The constant input of the simulator routine revision.

    Args:
        name (str): The name of the input.
        reference_id (str): The reference ID of the input.
        value (str): The value of the input.
        value_type (str): The value type of the input.
        unit (SimulationValueUnitInput | None): The unit of the input.
        save_timeseries_external_id (str | None): The external ID of the timeseries to save the input. If not provided, the input is not saved to a timeseries.
    """

    name: str
    reference_id: str
    value: str
    value_type: str  # TODO: ENUM?
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
        enabled (bool): Whether the schedule is enabled.
        cron_expression (str | None): The cron expression of the schedule.
    """

    enabled: bool = False
    cron_expression: str | None = None

    def __init__(self, enabled: bool, cron_expression: str | None = None) -> None:
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
    """
    The data sampling configuration of the simulator routine revision.
    Learn more about data sampling <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling>.

    Args:
        enabled (bool): Whether the data sampling is enabled.
        validation_window (int): The validation window of the data sampling.
        sampling_window (int): The sampling window of the data sampling.
        granularity (str): The granularity of the data sampling.
    """

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
class SimulatorRoutineLogicalCheck(CogniteObject):
    """
    The logical check configuration of the simulator routine revision.
    Learn more about logical checks <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling/#data-validation-methods>.

    Args:
        enabled (bool): Whether the logical check is enabled.
        timeseries_external_id (str): The external ID of the time series to check.
        aggregate (str): The aggregation method to use for the time series.
        operator (str): The operator to use for the logical check.
        value (float): The value to use for the logical check.
    """

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
    """
    Steady State Detection checks for steady state regions in a given time series.
    The user specifies the time series and three parameters: min section size, var threshold, and slope threshold.
    It returns a binary time series, with 1 for timestamps where the steady state criteria is met and 0 otherwise.

    Args:
        enabled (bool): Whether the steady state detection is enabled.
        timeseries_external_id (str): The external ID of the time series to check for steady state.
        aggregate (str): The aggregation method to use for the time series.
        min_section_size (int): The minimum number of consecutive data points that must meet the steady state criteria.
        var_threshold (float): The maximum variance allowed for the steady state region.
        slope_threshold (float): The maximum slope allowed for the steady state region.
    """

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
    """
    The simulator routine configuration defines the configuration of a simulator routine revision.
    Learn more about simulator routine configuration <https://docs.cognite.com/cdf/integration/guides/simulators/simulator_routines>.

    Args:
        schedule (SimulatorRoutineSchedule): Schedule configuration.
        data_sampling (SimulatorRoutineDataSampling): Data sampling configuration. Learn more about data sampling <https://docs.cognite.com/cdf/integration/guides/simulators/about_data_sampling>.
        logical_check (list[SimulatorRoutineLogicalCheck]): Logical check configuration.
        steady_state_detection (list[SimulatorRoutineSteadyStateDetectionEnabled]): Steady state detection configuration.
        inputs (list[SimulatorRoutineInputConstant | SimulatorRoutineInputTimeseries] | None): The inputs of the simulator routine revision.
        outputs (list[SimulatorRoutineOutput] | None): The outputs of the simulator routine revision.
    """

    schedule: SimulatorRoutineSchedule
    data_sampling: SimulatorRoutineDataSampling
    logical_check: list[SimulatorRoutineLogicalCheck]
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
                SimulatorRoutineLogicalCheck._load(check_, cognite_client) for check_ in resource["logicalCheck"]
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

    step_type: str  # TODO: enum? ["Get", "Set", "Command"]
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

    def __init__(
        self,
        external_id: str,
        routine_external_id: str,
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
