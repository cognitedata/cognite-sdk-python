from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import ClassVar, Optional, Sequence, Tuple, Type, TypeVar, Union, cast

from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._identifier import DataModelingIdentifier, DataModelingIdentifierSequence
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case


@dataclass
class DataModelingId:
    _type: ClassVar[str] = field(init=False)
    space: str
    external_id: str

    def dump(self, camel_case: bool = False, include_type: bool = True) -> dict[str, str]:
        output = asdict(self)
        if include_type:
            output["type"] = self._type
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls: Type[T_DataModelingId], data: dict) -> T_DataModelingId:
        return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))


T_DataModelingId = TypeVar("T_DataModelingId", bound=DataModelingId)


@dataclass
class VersionedDataModelingId:
    _type: ClassVar[str] = field(init=False)
    space: str
    external_id: str
    version: Optional[str] = None

    def dump(self, camel_case: bool = False, include_type: bool = True) -> dict[str, str]:
        output = asdict(self)
        if include_type:
            output["type"] = self._type
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls: Type[T_Versioned_DataModeling_Id], data: dict) -> T_Versioned_DataModeling_Id:
        return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))


T_Versioned_DataModeling_Id = TypeVar("T_Versioned_DataModeling_Id", bound=VersionedDataModelingId)


@dataclass
class ContainerId(DataModelingId):
    _type = "container"


@dataclass
class ViewId(VersionedDataModelingId):
    _type = "view"


@dataclass
class DataModelId(VersionedDataModelingId):
    _type = "datamodel"


ContainerIdentifier = Union[ContainerId, Tuple[str, str]]
ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
DataModelIdentifier = Union[DataModelId, Tuple[str, str], Tuple[str, str, str]]

Id = Union[Tuple[str, str], Tuple[str, str, str], DataModelingId, VersionedDataModelingId]


def load_identifier(ids: Id | Sequence[Id]) -> DataModelingIdentifierSequence:
    is_sequence = isinstance(ids, Sequence) and not (isinstance(ids, tuple) and isinstance(ids[0], str))
    id_list = cast(Sequence, ids)
    if not is_sequence:
        id_list = [ids]

    return DataModelingIdentifierSequence(
        identifiers=[
            DataModelingIdentifier(
                *id_ if isinstance(id_, tuple) else (id_.space, id_.external_id, getattr(id_, "version", None))
            )
            for id_ in id_list
        ],
        is_singleton=len(id_list) == 1,
    )
