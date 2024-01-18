from __future__ import annotations

from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, Sequence, Tuple, TypeVar, Union, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._identifier import DataModelingIdentifier, DataModelingIdentifierSequence
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass(frozen=True)
class AbstractDataclass(ABC):
    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls is AbstractDataclass or cls.__bases__[0] is AbstractDataclass:
            raise TypeError("Cannot instantiate abstract class.")
        return super().__new__(cls)


@dataclass(frozen=True)
class DataModelingId(AbstractDataclass):
    _type: ClassVar[str] = field(init=False)
    space: str
    external_id: str

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id

    def dump(self, camel_case: bool = True, include_type: bool = True) -> dict[str, str]:
        output = asdict(self)
        if include_type:
            output["type"] = self._type
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls: type[T_DataModelingId], data: dict | T_DataModelingId | tuple[str, str]) -> T_DataModelingId:
        if isinstance(data, cls):
            return data
        elif isinstance(data, tuple):
            return cls(*data)
        elif isinstance(data, dict):
            return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))
        raise TypeError(f"Cannot load {data} into {cls}, invalid type={type(data)}")


T_DataModelingId = TypeVar("T_DataModelingId", bound=DataModelingId)


@dataclass(frozen=True)
class VersionedDataModelingId(AbstractDataclass):
    _type: ClassVar[str] = field(init=False)
    space: str
    external_id: str
    version: str | None = None

    def as_tuple(self) -> tuple[str, str, str | None]:
        return self.space, self.external_id, self.version

    def dump(self, camel_case: bool = True, include_type: bool = True) -> dict[str, str]:
        output = asdict(self)
        if include_type:
            output["type"] = self._type
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(
        cls: type[T_Versioned_DataModeling_Id],
        data: dict | T_Versioned_DataModeling_Id | tuple[str, str] | tuple[str, str, str],
    ) -> T_Versioned_DataModeling_Id:
        if isinstance(data, cls):
            return data
        elif isinstance(data, tuple):
            return cls(*data)
        elif isinstance(data, dict):
            return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))
        raise TypeError(f"Cannot load {data} into {cls}, invalid type={type(data)}")


T_Versioned_DataModeling_Id = TypeVar("T_Versioned_DataModeling_Id", bound=VersionedDataModelingId)


@dataclass(frozen=True)
class InstanceId:
    _instance_type: ClassVar[Literal["node", "edge"]]
    space: str
    external_id: str

    def dump(self, camel_case: bool = True, include_instance_type: bool = True) -> dict[str, str]:
        output = asdict(self)
        if include_instance_type:
            output["instanceType" if camel_case else "instance_type"] = self._instance_type
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls: type[T_InstanceId], data: dict) -> T_InstanceId:
        return cls(
            **convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"instanceType", "instance_type"}))
        )

    @property
    def instance_type(self) -> Literal["node", "edge"]:
        return self._instance_type

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


T_InstanceId = TypeVar("T_InstanceId", bound=InstanceId)


@dataclass(frozen=True)
class NodeId(InstanceId):
    _instance_type: ClassVar[Literal["node", "edge"]] = "node"


@dataclass(frozen=True)
class EdgeId(InstanceId):
    _instance_type: ClassVar[Literal["node", "edge"]] = "edge"


@dataclass(frozen=True)
class ContainerId(DataModelingId):
    _type = "container"

    def as_source_identifier(self) -> str:
        return self.external_id

    def as_property_ref(self, property: str) -> tuple[str, str, str]:
        return (self.space, self.as_source_identifier(), property)


@dataclass(frozen=True)
class ViewId(VersionedDataModelingId):
    _type = "view"

    def as_source_identifier(self) -> str:
        return f"{self.external_id}/{self.version}"

    def as_property_ref(self, property: str) -> tuple[str, str, str]:
        return (self.space, self.as_source_identifier(), property)


@dataclass(frozen=True)
class PropertyId(CogniteObject):
    source: ViewId | ContainerId
    property: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            source=cls.__load_view_or_container_id(resource["source"]),
            property=resource["identifier"],
        )

    @staticmethod
    def __load_view_or_container_id(view_or_container_id: dict[str, Any]) -> ViewId | ContainerId:
        if "type" in view_or_container_id and view_or_container_id["type"] in {"view", "container"}:
            if view_or_container_id["type"] == "view":
                return ViewId.load(view_or_container_id)
            return ContainerId.load(view_or_container_id)
        raise ValueError(f"Invalid type {view_or_container_id}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case, include_type=True),
            "identifier": self.property,
        }


@dataclass(frozen=True)
class DataModelId(VersionedDataModelingId):
    _type = "datamodel"


class IdLike(Protocol):
    @property
    def space(self) -> str:
        ...

    @property
    def external_id(self) -> str:
        ...


class VersionedIdLike(IdLike, Protocol):
    @property
    def version(self) -> str | None:
        ...


ContainerIdentifier = Union[ContainerId, Tuple[str, str]]
ConstraintIdentifier = Tuple[ContainerId, str]
IndexIdentifier = Tuple[ContainerId, str]
ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
DataModelIdentifier = Union[DataModelId, Tuple[str, str], Tuple[str, str, str]]
NodeIdentifier = Union[NodeId, Tuple[str, str, str]]
EdgeIdentifier = Union[EdgeId, Tuple[str, str, str]]

Id = Union[Tuple[str, str], Tuple[str, str, str], IdLike, VersionedIdLike]


def _load_space_identifier(ids: str | SequenceNotStr[str]) -> DataModelingIdentifierSequence:
    is_sequence = isinstance(ids, Sequence) and not isinstance(ids, str)
    spaces = [ids] if isinstance(ids, str) else ids
    return DataModelingIdentifierSequence(
        identifiers=[DataModelingIdentifier(space) for space in spaces], is_singleton=not is_sequence
    )


def _load_identifier(
    ids: Id | Sequence[Id], id_type: Literal["container", "view", "data_model", "space", "node", "edge"]
) -> DataModelingIdentifierSequence:
    is_sequence = isinstance(ids, Sequence) and not (isinstance(ids, tuple) and isinstance(ids[0], str))
    is_view_or_data_model = id_type in {"view", "data_model"}
    is_instance = id_type in {"node", "edge"}
    id_list = cast(Sequence, ids)
    if not is_sequence:
        id_list = [ids]

    def create_args(id_: Id) -> tuple[str, str, str | None, Literal["node", "edge"] | None]:
        if isinstance(id_, tuple) and is_instance:
            if len(id_) == 2:
                return id_[0], id_[1], None, id_type  # type: ignore[return-value]
            raise ValueError("Instance given as a tuple must have two elements (space, externalId)")
        if isinstance(id_, tuple):
            return id_[0], id_[1], (id_[2] if len(id_) == 3 else None), None
        instance_type = None
        if is_instance:
            instance_type = "node" if isinstance(id_, NodeId) else "edge"
        return id_.space, id_.external_id, getattr(id_, "version", None), instance_type  # type: ignore[return-value]

    return DataModelingIdentifierSequence(
        identifiers=[DataModelingIdentifier(*create_args(id_)) for id_ in id_list],
        is_singleton=not is_sequence and not is_view_or_data_model,
    )
