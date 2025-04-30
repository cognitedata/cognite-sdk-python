from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import CogniteClient

_WARNING = FeaturePreviewWarning(api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators")


@dataclass
class SimulationValueUnitName(CogniteObject):
    name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
        )

    def __post_init__(self) -> None:
        _WARNING.warn()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulationInputOverride(CogniteObject):
    reference_id: str
    value: str | int | float | list[str] | list[int] | list[float]
    unit: SimulationValueUnitName | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            reference_id=resource["referenceId"],
            value=resource["value"],
            unit=SimulationValueUnitName._load(resource["unit"], cognite_client) if resource.get("unit") else None,
        )

    def __post_init__(self) -> None:
        _WARNING.warn()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output


class SimulationRunCore(WriteableCogniteResource["SimulationRunWrite"]):
    def __init__(
        self,
        run_type: str | None,
        routine_external_id: str,
        run_time: int | None = None,
    ) -> None:
        self.run_type = run_type
        self.run_time = run_time
        self.routine_external_id = routine_external_id


class SimulationRunWrite(SimulationRunCore):
    """
    Request to run a simulator routine asynchronously.

    Args:
        routine_external_id (str): External id of the associated simulator routine
        run_type (str | None): The type of the simulation run
        run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
        queue (bool | None): Queue the simulation run when connector is down.
        log_severity (str | None): Override the minimum severity level for the simulation run logs. If not provided, the minimum severity is read from the connector logger configuration.
        inputs (list[SimulationInputOverride] | None): List of input overrides
    """

    def __init__(
        self,
        routine_external_id: str,
        run_type: str | None = None,
        run_time: int | None = None,
        queue: bool | None = None,
        log_severity: str | None = None,
        inputs: list[SimulationInputOverride] | None = None,
    ) -> None:
        super().__init__(
            routine_external_id=routine_external_id,
            run_type=run_type,
            run_time=run_time,
        )
        self.queue = queue
        self.log_severity = log_severity
        self.inputs = inputs

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulationRunWrite:
        inputs = resource.get("inputs", None)
        return cls(
            run_type=resource.get("runType"),
            routine_external_id=resource["routineExternalId"],
            run_time=resource.get("runTime"),
            queue=resource.get("queue"),
            log_severity=resource.get("logSeverity"),
            inputs=[SimulationInputOverride._load(_input) for _input in inputs] if inputs else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.inputs is not None:
            output["inputs"] = [input_.dump(camel_case=camel_case) for input_ in self.inputs]
        return output

    def as_write(self) -> SimulationRunWrite:
        return self


class SimulationRun(SimulationRunCore):
    """
    Every time a simulation routine executes, a simulation run object is created.
    This object ensures that each execution of a routine is documented and traceable.
    Each run has an associated simulation data resource, which stores the inputs and outputs of a
    simulation run, capturing the values set into and read from the simulator model to ensure
    the traceability and integrity of the simulation data.

    Simulation runs provide a historical record of the simulations performed, allowing users to analyze
    and compare different runs, track changes over time, and make informed decisions based on the simulation results.

    This is the read/response format of a simulation run.

    Args:
        id (int): The id of the simulation run
        simulator_external_id (str): External id of the associated simulator
        simulator_integration_external_id (str): External id of the associated simulator integration
        model_external_id (str): External id of the associated simulator model
        model_revision_external_id (str): External id of the associated simulator model revision
        routine_revision_external_id (str): External id of the associated simulator routine revision
        routine_external_id (str): External id of the associated simulator routine
        run_type (Literal['external', 'manual', 'scheduled']): The type of the simulation run
        status (Literal['ready', 'running', 'success', 'failure']): The status of the simulation run
        data_set_id (int): The id of the dataset associated with the simulation run
        user_id (str): The id of the user who executed the simulation run
        log_id (int): The id of the log associated with the simulation run
        created_time (int): The number of milliseconds since epoch
        last_updated_time (int): The number of milliseconds since epoch
        status_message (str | None): The status message of the simulation run
        simulation_time (int | None): Simulation time in milliseconds. Timestamp when the input data was sampled. Used for indexing input and output time series.
        run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.

    """

    def __init__(
        self,
        id: int,
        simulator_external_id: str,
        simulator_integration_external_id: str,
        model_external_id: str,
        model_revision_external_id: str,
        routine_revision_external_id: str,
        routine_external_id: str,
        run_type: Literal["external", "manual", "scheduled"],
        status: Literal["ready", "running", "success", "failure"],
        data_set_id: int,
        user_id: str,
        log_id: int,
        created_time: int,
        last_updated_time: int,
        status_message: str | None = None,
        simulation_time: int | None = None,
        run_time: int | None = None,
    ) -> None:
        super().__init__(
            run_type=run_type,
            routine_external_id=routine_external_id,
            run_time=run_time,
        )
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.simulator_external_id = simulator_external_id
        self.simulator_integration_external_id = simulator_integration_external_id
        self.model_external_id = model_external_id
        self.model_revision_external_id = model_revision_external_id
        self.routine_revision_external_id = routine_revision_external_id
        self.simulation_time = simulation_time
        self.status = status
        self.status_message = status_message
        self.data_set_id = data_set_id
        self.user_id = user_id
        self.log_id = log_id

    def as_write(self) -> SimulationRunWrite:
        return SimulationRunWrite(
            routine_external_id=self.routine_external_id,
            run_type=self.run_type,
            run_time=self.run_time,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SimulationRun:
        return cls(
            id=resource["id"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            simulator_external_id=resource["simulatorExternalId"],
            simulator_integration_external_id=resource["simulatorIntegrationExternalId"],
            model_external_id=resource["modelExternalId"],
            model_revision_external_id=resource["modelRevisionExternalId"],
            routine_external_id=resource["routineExternalId"],
            routine_revision_external_id=resource["routineRevisionExternalId"],
            status=resource["status"],
            data_set_id=resource["dataSetId"],
            run_type=resource["runType"],
            user_id=resource["userId"],
            log_id=resource["logId"],
            status_message=resource.get("statusMessage"),
            run_time=resource.get("runTime"),
            simulation_time=resource.get("simulationTime"),
        )


class SimulationValueBase(CogniteObject):
    """
    Base class for simulation values. This class is used to define the value and its type.
    The value can be a string, double, array of strings or array of doubles.
    The maximum length is 1024 for strings and 200 for arrays.
    """

    def __init__(
        self,
        reference_id: str,
        value: str | int | float | list[str] | list[int] | list[float],
        value_type: Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"],
        unit: SimulationValueUnitName | None = None,
        simulator_object_reference: Any | None = None,
        timeseries_external_id: str | None = None,
    ) -> None:
        super().__init__()
        self.reference_id = reference_id
        self.value = value
        self.value_type = value_type
        self.unit = unit
        self.simulator_object_reference = simulator_object_reference
        self.timeseries_external_id = timeseries_external_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            reference_id=resource["referenceId"],
            value=resource["value"],
            value_type=resource["valueType"],
            unit=SimulationValueUnitName._load(resource["unit"], cognite_client) if resource.get("unit") else None,
            simulator_object_reference=resource.get("simulatorObjectReference"),
            timeseries_external_id=resource.get("timeseriesExternalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)
        if self.simulator_object_reference is not None:
            output["simulatorObjectReference"] = self.simulator_object_reference
        if self.timeseries_external_id is not None:
            output["timeseriesExternalId"] = self.timeseries_external_id
        return output


class SimulationValue(SimulationValueBase):
    """
    This class is used to define the value and its type.
    The value can be a string, double, array of strings or array of doubles.
    The maximum length is 1024 for strings and 200 for arrays.
    """

    def __init__(
        self,
        reference_id: str,
        value: str | int | float | list[str] | list[int] | list[float],
        value_type: Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"],
        unit: SimulationValueUnitName | None = None,
        simulator_object_reference: Any | None = None,
        timeseries_external_id: str | None = None,
        overridden: bool | None = None,
    ) -> None:
        super().__init__(
            reference_id=reference_id,
            value=value,
            value_type=value_type,
            unit=unit,
            simulator_object_reference=simulator_object_reference,
            timeseries_external_id=timeseries_external_id,
        )
        self.overridden = overridden

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            reference_id=resource["referenceId"],
            value=resource["value"],
            value_type=resource["valueType"],
            unit=SimulationValueUnitName._load(resource["unit"], cognite_client) if resource.get("unit") else None,
            simulator_object_reference=resource.get("simulatorObjectReference"),
            timeseries_external_id=resource.get("timeseriesExternalId"),
            overridden=resource.get("overridden"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)
        if self.simulator_object_reference is not None:
            output["simulatorObjectReference"] = self.simulator_object_reference
        if self.timeseries_external_id is not None:
            output["timeseriesExternalId"] = self.timeseries_external_id
        if self.overridden is not None:
            output["overridden"] = self.overridden
        return output


class SimulationRunDataItem(CogniteResource):
    run_id: int
    inputs: list[SimulationValue]
    outputs: list[SimulationValueBase]

    def __init__(
        self,
        run_id: int,
        inputs: list[SimulationValue],
        outputs: list[SimulationValueBase],
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.inputs = inputs
        self.outputs = outputs

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        inputs = [SimulationValue._load(input_) for input_ in resource["inputs"]]
        outputs = [SimulationValueBase._load(output_) for output_ in resource["outputs"]]
        return cls(
            run_id=resource["runId"],
            inputs=inputs,
            outputs=outputs,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["inputs"] = [input_.dump(camel_case=camel_case) for input_ in self.inputs]
        output["outputs"] = [output_.dump(camel_case=camel_case) for output_ in self.outputs]
        return output


class SimulatorRunDataList(CogniteResourceList[SimulationRunDataItem], IdTransformerMixin):
    _RESOURCE = SimulationRunDataItem


class SimulationRunWriteList(CogniteResourceList[SimulationRunWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulationRunWrite


class SimulatorRunsList(WriteableCogniteResourceList[SimulationRunWrite, SimulationRun], IdTransformerMixin):
    _RESOURCE = SimulationRun

    def as_write(self) -> SimulationRunWriteList:
        return SimulationRunWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())
