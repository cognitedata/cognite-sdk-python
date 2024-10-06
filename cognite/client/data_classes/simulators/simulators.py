from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn, Sequence

from typing_extensions import Self

from cognite.client.utils.useful_types import SequenceNotStr
from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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
    



@dataclass
class SimulatorQuantity(CogniteObject):
    name: str
    label: str
    units: SimulatorUnitEntry | Sequence[SimulatorUnitEntry]
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            label=resource["label"],
            units=SimulatorUnitEntry._load(resource["units"], cognite_client),
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["units"] = self.units.dump(camel_case=camel_case)
        
        return output



@dataclass
class SimulatorStepField(CogniteObject):
    name: str
    label: str
    info: str
    options: SimulatorStepOption | Sequence[SimulatorStepOption] | None
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            label=resource["label"],
            info=resource["info"],
            options=SimulatorStepOption._load(resource["options"], cognite_client) if "options" in resource else None,
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.options, SimulatorStepOption):
            output["options"] = self.options.dump(camel_case=camel_case)
        
        return output



@dataclass
class SimulatorStep(CogniteObject):
    step_type: str
    fields: SimulatorStepField | Sequence[SimulatorStepField]
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            step_type=resource["stepType"],
            fields=SimulatorStepField._load(resource["fields"], cognite_client),
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["fields"] = self.fields.dump(camel_case=camel_case)
        
        return output


class _SimulatorCore(WriteableCogniteResource["SimulatorWrite"], ABC):
    def __init__(self, external_id: str, name: str, file_extension_types: str | SequenceNotStr[str], model_types: SimulatorModelType | Sequence[SimulatorModelType], step_fields: SimulatorStep | Sequence[SimulatorStep], unit_quantities: SimulatorQuantity | Sequence[SimulatorQuantity] | None = None) -> None:
        self.external_id = external_id
        self.name = name
        self.file_extension_types = file_extension_types
        self.model_types = model_types
        self.step_fields = step_fields
        self.unit_quantities = unit_quantities


class SimulatorWrite(_SimulatorCore):
    """The simulator resource contains the definitions necessary for Cognite Data Fusion (CDF) to interact with a given simulator.

    It serves as a central contract that allows APIs, UIs, and integrations (connectors) to utilize the same definitions
    when dealing with a specific simulator.  Each simulator is uniquely identified and can be associated with various
    file extension types, model types, step fields, and unit quantities. Simulators are essential for managing data
    flows between CDF and external simulation tools, ensuring consistency and reliability in data handling.  ####
    Limitations:  - A project can have a maximum of 100 simulators

    This is the write/request format of the simulator.

    Args:
        external_id (str): External id of the simulator
        name (str): Name of the simulator
        file_extension_types (str | SequenceNotStr[str]): File extension types supported by the simulator
        model_types (SimulatorModelType | Sequence[SimulatorModelType]): Model types supported by the simulator
        step_fields (SimulatorStep | Sequence[SimulatorStep]): Step types supported by the simulator when creating routines
        unit_quantities (SimulatorQuantity | Sequence[SimulatorQuantity] | None): Quantities and their units supported by the simulator

    """

    def __init__(self, external_id: str, name: str, file_extension_types: str | SequenceNotStr[str], model_types: SimulatorModelType | Sequence[SimulatorModelType], step_fields: SimulatorStep | Sequence[SimulatorStep], unit_quantities: SimulatorQuantity | Sequence[SimulatorQuantity] | None = None) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            file_extension_types=file_extension_types,
            model_types=model_types,
            step_fields=step_fields,
            unit_quantities=unit_quantities,
        )
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            file_extension_types=resource["fileExtensionTypes"],
            model_types=SimulatorModelType._load(resource["modelTypes"], cognite_client),
            step_fields=SimulatorStep._load(resource["stepFields"], cognite_client),
            unit_quantities=SimulatorQuantity._load(resource["unitQuantities"], cognite_client) if "unitQuantities" in resource else None,
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["modelTypes" if camel_case else "model_types"] = self.model_types.dump(camel_case=camel_case)
        output["stepFields" if camel_case else "step_fields"] = self.step_fields.dump(camel_case=camel_case)
        if isinstance(self.unit_quantities, SimulatorQuantity):
            output["unitQuantities" if camel_case else "unit_quantities"] = self.unit_quantities.dump(camel_case=camel_case)
        
        return output
    def as_write(self) -> SimulatorWrite:
        return self



class Simulator(_SimulatorCore):
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
        model_types (SimulatorModelType | Sequence[SimulatorModelType] | None): Model types supported by the simulator
        step_fields (SimulatorStep | Sequence[SimulatorStep] | None): Step types supported by the simulator when creating routines
        unit_quantities (SimulatorQuantity | Sequence[SimulatorQuantity] | None): Quantities and their units supported by the simulator
        created_time (int): None
        last_updated_time (int): None

    """

    def __init__(self, id: int, external_id: str, name: str, file_extension_types: str | SequenceNotStr[str], created_time: int, last_updated_time: int, model_types: SimulatorModelType | Sequence[SimulatorModelType] | None = None, step_fields: SimulatorStep | Sequence[SimulatorStep] | None = None, unit_quantities: SimulatorQuantity | Sequence[SimulatorQuantity] | None = None) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            file_extension_types=file_extension_types,
            model_types=model_types,
            step_fields=step_fields,
            unit_quantities=unit_quantities,
        )
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
            model_types=SimulatorModelType._load(resource["modelTypes"], cognite_client) if "modelTypes" in resource else None,
            step_fields=SimulatorStep._load(resource["stepFields"], cognite_client) if "stepFields" in resource else None,
            unit_quantities=SimulatorQuantity._load(resource["unitQuantities"], cognite_client) if "unitQuantities" in resource else None,
        ) 

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.model_types, SimulatorModelType):
            output["modelTypes" if camel_case else "model_types"] = self.model_types.dump(camel_case=camel_case)
        if isinstance(self.step_fields, SimulatorStep):
            output["stepFields" if camel_case else "step_fields"] = self.step_fields.dump(camel_case=camel_case)
        if isinstance(self.unit_quantities, SimulatorQuantity):
            output["unitQuantities" if camel_case else "unit_quantities"] = self.unit_quantities.dump(camel_case=camel_case)
        
        return output


    def as_write(self) -> SimulatorWrite:
        return SimulatorWrite(
            unit_quantities=self.unit_quantities,
            name=self.name,
            model_types=self.model_types,
            external_id=self.external_id,
            step_fields=self.step_fields,
            file_extension_types=self.file_extension_types,
        )


class SimulatorUpdate(CogniteUpdate):
    def __init__(self, id: int) -> None:
        super().__init__(
            id = id,
        )

    class _UpdateSetAnnotatedStringConstraintsUpdate(CognitePrimitiveUpdate):
        def set(self, value: UpdateSetAnnotatedStringConstraints | None) -> SimulatorUpdate:
            return self._set(value.dump() if isinstance(value, UpdateSetAnnotatedStringConstraints) else value)

    class _UpdateSetListUpdate(CognitePrimitiveUpdate):
        def set(self, value: UpdateSetList | None) -> SimulatorUpdate:
            return self._set(value.dump() if isinstance(value, UpdateSetList) else value)

    class _UpdateSetListSimulatorModelTypeUpdate(CognitePrimitiveUpdate):
        def set(self, value: UpdateSetListSimulatorModelType | None) -> SimulatorUpdate:
            return self._set(value.dump() if isinstance(value, UpdateSetListSimulatorModelType) else value)

    class _UpdateSetListSimulatorStepUpdate(CognitePrimitiveUpdate):
        def set(self, value: UpdateSetListSimulatorStep | None) -> SimulatorUpdate:
            return self._set(value.dump() if isinstance(value, UpdateSetListSimulatorStep) else value)

    class _UpdateSetList1kSimulatorQuantityUpdate(CognitePrimitiveUpdate):
        def set(self, value: UpdateSetList1kSimulatorQuantity | None) -> SimulatorUpdate:
            return self._set(value.dump() if isinstance(value, UpdateSetList1kSimulatorQuantity) else value)


    @property
    def name(self) -> SimulatorUpdate._UpdateSetAnnotatedStringConstraintsUpdate:
        return self._UpdateSetAnnotatedStringConstraintsUpdate(self, "name")

    @property
    def file_extension_types(self) -> SimulatorUpdate._UpdateSetListUpdate:
        return self._UpdateSetListUpdate(self, "file_extension_types")

    @property
    def model_types(self) -> SimulatorUpdate._UpdateSetListSimulatorModelTypeUpdate:
        return self._UpdateSetListSimulatorModelTypeUpdate(self, "model_types")

    @property
    def step_fields(self) -> SimulatorUpdate._UpdateSetListSimulatorStepUpdate:
        return self._UpdateSetListSimulatorStepUpdate(self, "step_fields")

    @property
    def unit_quantities(self) -> SimulatorUpdate._UpdateSetList1kSimulatorQuantityUpdate:
        return self._UpdateSetList1kSimulatorQuantityUpdate(self, "unit_quantities")


    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=True),
            PropertySpec("file_extension_types", is_nullable=True),
            PropertySpec("model_types", is_nullable=True),
            PropertySpec("step_fields", is_nullable=True),
            PropertySpec("unit_quantities", is_nullable=True),
    ]


class SimulatorWriteList(CogniteResourceList[SimulatorWrite]):
    _RESOURCE = SimulatorWrite


class SimulatorList(WriteableCogniteResourceList[SimulatorWrite, Simulator]):
    _RESOURCE = Simulator

    def as_write(self) -> SimulatorWriteList:
        return SimulatorWriteList([item.as_write() for item in self.data])

