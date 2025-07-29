from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    IdTransformerMixin,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Simulator(CogniteResource):
    """The simulator resource contains the definitions necessary for Cognite Data Fusion (CDF) to interact with a given simulator.

    It serves as a central contract that allows APIs, UIs, and integrations (connectors) to utilize the same definitions
    when dealing with a specific simulator.  Each simulator is uniquely identified and can be associated with various
    file extension types, model types, step fields, and unit quantities. Simulators are essential for managing data
    flows between CDF and external simulation tools, ensuring consistency and reliability in data handling.

    This is the read/response format of the simulator.

    Args:
        external_id (str): External id of the simulator
        id (int): Id of the simulator.
        name (str): Name of the simulator
        file_extension_types (Sequence[str]): File extension types supported by the simulator
        model_types (Sequence[SimulatorModelType] | None): Model types supported by the simulator
        model_dependencies (Sequence[SimulatorModelDependency] | None): Model dependencies supported by the simulator
        step_fields (Sequence[SimulatorStep] | None): Step types supported by the simulator when creating routines
        unit_quantities (Sequence[SimulatorQuantity] | None): Quantities and their units supported by the simulator

    """

    def __init__(
        self,
        external_id: str,
        id: int,
        name: str,
        file_extension_types: Sequence[str],
        model_types: Sequence[SimulatorModelType] | None = None,
        model_dependencies: Sequence[SimulatorModelDependency] | None = None,
        step_fields: Sequence[SimulatorStep] | None = None,
        unit_quantities: Sequence[SimulatorQuantity] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.file_extension_types = file_extension_types
        self.model_dependencies = model_dependencies
        self.model_types = model_types
        self.step_fields = step_fields
        self.unit_quantities = unit_quantities
        self.id = id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
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
            model_dependencies=SimulatorModelDependency._load_list(resource["modelDependencies"], cognite_client)
            if "modelDependencies" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.model_types, list) and all(
            isinstance(item, SimulatorModelType) for item in self.model_types
        ):
            output["modelTypes" if camel_case else "model_types"] = [
                item.dump(camel_case=camel_case) for item in self.model_types
            ]
        if isinstance(self.step_fields, list) and all(isinstance(item, SimulatorStep) for item in self.step_fields):
            output["stepFields" if camel_case else "step_fields"] = [
                item.dump(camel_case=camel_case) for item in self.step_fields
            ]
        if isinstance(self.unit_quantities, list) and all(
            isinstance(item, SimulatorQuantity) for item in self.unit_quantities
        ):
            output["unitQuantities" if camel_case else "unit_quantities"] = [
                item.dump(camel_case=camel_case) for item in self.unit_quantities
            ]

        if isinstance(self.model_dependencies, list) and all(
            isinstance(item, SimulatorModelDependency) for item in self.model_dependencies
        ):
            output["modelDependencies" if camel_case else "model_dependencies"] = [
                item.dump(camel_case=camel_case) for item in self.model_dependencies
            ]

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
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            key=resource["key"],
        )

    @classmethod
    def _load_list(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> list[SimulatorModelType]:
        if isinstance(resource, dict):
            return [cls._load(resource, cognite_client)]
        elif isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource if isinstance(res, dict)]
        else:
            raise TypeError("Expected a dict or a list of dicts.")


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
    ) -> list[SimulatorQuantity]:
        if isinstance(resource, dict):
            return [cls._load(resource, cognite_client)]
        elif isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource if isinstance(res, dict)]
        else:
            raise TypeError("Expected a dict or a list of dicts.")

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
class SimulatorModelDependencyFields(CogniteObject):
    """
    Represents the fields of a simulator model dependency.
    This is used to define the specific fields that are required for a dependency between two models.
    """

    name: str
    label: str
    info: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            label=resource["label"],
            info=resource["info"],
        )


@dataclass
class SimulatorModelDependency(CogniteObject):
    """
    Represents a dependency between two simulator models.
    This is used to define how one model depends on another in the context of a simulator.
    """

    file_extension_types: Sequence[str]
    fields: Sequence[SimulatorModelDependencyFields]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            file_extension_types=resource["fileExtensionTypes"],
            fields=[SimulatorModelDependencyFields._load(field_, cognite_client) for field_ in resource["fields"]],
        )

    @classmethod
    def _load_list(
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> list[SimulatorModelDependency]:
        if isinstance(resource, dict):
            return [cls._load(resource, cognite_client)]
        elif isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource if isinstance(res, dict)]
        else:
            raise TypeError("Expected a dict or a list of dicts.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["fields"] = [field_.dump(camel_case=camel_case) for field_ in self.fields]

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
    ) -> list[SimulatorStep]:
        if isinstance(resource, dict):
            return [cls._load(resource, cognite_client)]
        elif isinstance(resource, list):
            return [cls._load(res, cognite_client) for res in resource if isinstance(res, dict)]
        else:
            raise TypeError("Expected a dict or a list of dicts.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["fields"] = [field_.dump(camel_case=camel_case) for field_ in self.fields]

        return output


class SimulatorList(CogniteResourceList[Simulator], IdTransformerMixin):
    _RESOURCE = Simulator


class SimulatorIntegration(CogniteResource):
    """
    The simulator integration resource represents a simulator connector in Cognite Data Fusion (CDF).

    It provides information about the configured connectors for a given simulator, including their status and additional
    details such as dataset, name, license status, connector version, simulator version, and more. This resource is essential
    for monitoring and managing the interactions between CDF and external simulators, ensuring proper data flow and integration.

    This is the read/response format of the simulator integration.

    Args:
        id (int): Id of the simulator integration.
        external_id (str): External id of the simulator integration
        simulator_external_id (str): External id of the associated simulator
        heartbeat (int): The interval in seconds between the last heartbeat and the current time
        data_set_id (int): The id of the dataset associated with the simulator integration
        connector_version (str): The version of the connector
        log_id (int): Id of the log associated with this simulator integration.
        active (bool): Indicates if the simulator integration is active (i.e., a connector is linked to CDF for this integration).
        created_time (int): The time when this simulator integration resource was created.
        last_updated_time (int): The last time the simulator integration resource was updated.
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
        data_set_id: int,
        connector_version: str,
        log_id: int,
        active: bool,
        created_time: int,
        last_updated_time: int,
        license_status: str | None = None,
        simulator_version: str | None = None,
        license_last_checked_time: int | None = None,
        connector_status: str | None = None,
        connector_status_updated_time: int | None = None,
    ) -> None:
        self.id = id
        self.log_id = log_id
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
        self.active = active
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            active=resource["active"],
            log_id=resource["logId"],
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
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )


class SimulatorIntegrationList(CogniteResourceList[SimulatorIntegration], IdTransformerMixin):
    _RESOURCE = SimulatorIntegration
