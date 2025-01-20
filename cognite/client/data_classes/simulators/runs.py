from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
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


# class SimulationRun(CogniteResource):
#     """
#     Every time a simulation routine executes, a simulation run object is created.
#     This object ensures that each execution of a routine is documented and traceable.
#     Each run has an associated simulation data resource, which stores the inputs and outputs of a
#     simulation run, capturing the values set into and read from the simulator model to ensure
#     the traceability and integrity of the simulation data.

#     Simulation runs provide a historical record of the simulations performed, allowing users to analyze
#     and compare different runs, track changes over time, and make informed decisions based on the simulation results.

#     This is the read/response format of a simulation run.

#     Args:
#         cognite_client (CogniteClient | None): An instance of the Cognite client.
#         simulator_external_id (str | None): External id of the associated simulator
#         simulator_integration_external_id (str | None): External id of the associated simulator integration
#         model_external_id (str | None): External id of the associated simulator model
#         model_revision_external_id (str | None): External id of the associated simulator model revision
#         routine_external_id (str | None): External id of the associated simulator routine
#         routine_revision_external_id (str | None): External id of the associated simulator routine revision
#         run_time (int | None): Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.
#         simulation_time (int | None): Simulation time in milliseconds. Timestamp when the input data was sampled. Used for indexing input and output time series.
#         status (str | None): The status of the simulation run
#         status_message (str | None): The status message of the simulation run
#         data_set_id (int | None): The id of the dataset associated with the simulation run
#         run_type (str | None): The type of the simulation run
#         user_id (str | None): The id of the user who executed the simulation run
#         log_id (int | None): The id of the log associated with the simulation run
#         id (int | None): A unique id of a simulation run
#         created_time (int | None): The number of milliseconds since epoch
#         last_updated_time (int | None): The number of milliseconds since epoch

#     """

#     def __init__(
#         self,
#         id: int,
#         created_time: int,
#         last_updated_time: int,
#         cognite_client: CogniteClient | None = None,
#         simulator_external_id: str | None = None,
#         simulator_integration_external_id: str | None = None,
#         model_external_id: str | None = None,
#         model_revision_external_id: str | None = None,
#         routine_external_id: str | None = None,
#         routine_revision_external_id: str | None = None,
#         run_time: int | None = None,
#         simulation_time: int | None = None,
#         status: str | None = None,
#         status_message: str | None = None,
#         data_set_id: int | None = None,
#         run_type: str | None = None,
#         user_id: str | None = None,
#         log_id: int | None = None,
#     ) -> None:
#         self.id = id
#         self.created_time = created_time
#         self.last_updated_time = last_updated_time
#         self.simulator_external_id = simulator_external_id
#         self.simulator_integration_external_id = simulator_integration_external_id
#         self.model_external_id = model_external_id
#         self.model_revision_external_id = model_revision_external_id
#         self.routine_external_id = routine_external_id
#         self.routine_revision_external_id = routine_revision_external_id
#         self.run_time = run_time
#         self.simulation_time = simulation_time
#         self.status = status
#         self.status_message = status_message
#         self.data_set_id = data_set_id
#         self.run_type = run_type
#         self.user_id = user_id
#         self.log_id = log_id
#         if cognite_client is not None:
#             self._cognite_client = cognite_client

#     @classmethod
#     def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SimulationRun:
#         return super()._load(resource, cognite_client)

#     def dump(self, camel_case: bool = True) -> dict[str, Any]:
#         return super().dump(camel_case=camel_case)

#     def __hash__(self) -> int:
#         return hash(self.id)

#     def update(self) -> None:
#         latest = self._cognite_client.simulators.runs.retrieve(id=self.id)
#         if latest is None:
#             raise RuntimeError("Unable to update the simulation run object (it was not found)")
#         self.status = latest.status

#     def wait(self) -> None:
#         while self.status is not None and self.status.lower() == "ready":
#             self.update()
#             time.sleep(1.0)
