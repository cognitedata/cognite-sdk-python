from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
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
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SimulatorCore(WriteableCogniteResource["SimulatorWrite"], ABC):
    """The simulator resource contains the definitions necessary for Cognite Data Fusion (CDF) to interact with a given simulator.

    It serves as a central contract that allows APIs, UIs, and integrations (connectors) to utilize the same definitions
    when dealing with a specific simulator.  Each simulator is uniquely identified and can be associated with various
    file extension types, model types, step fields, and unit quantities. Simulators are essential for managing data
    flows between CDF and external simulation tools, ensuring consistency and reliability in data handling.  ####

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


class SimulatorWriteList(CogniteResourceList[SimulatorWrite], ExternalIDTransformerMixin):
    _RESOURCE = SimulatorWrite


class SimulatorList(WriteableCogniteResourceList[SimulatorWrite, Simulator], IdTransformerMixin):
    _RESOURCE = Simulator

    def as_write(self) -> SimulatorWriteList:
        return SimulatorWriteList([a.as_write() for a in self.data], cognite_client=self._get_cognite_client())
