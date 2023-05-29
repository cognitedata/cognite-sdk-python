from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Tuple, Union, cast


@dataclass
class DataModelingId:
    space: str
    external_id: str

    @classmethod
    def from_tuple(cls, tup: tuple[str, str] | tuple[str, str, str]) -> DataModelingId:
        return DataModelingId(*tup)


@dataclass
class VersionedDataModelingId(DataModelingId):
    version: Optional[str] = None

    @classmethod
    def from_tuple(cls, tup: tuple[str, str] | tuple[str, str, str]) -> VersionedDataModelingId:
        return VersionedDataModelingId(*tup)


@dataclass
class TypedDataModelingId(DataModelingId):
    instance_type: Literal["node", "edge"] = "node"

    @classmethod
    def from_tuple(cls, tup: tuple[str, str] | tuple[str, str, str]) -> TypedDataModelingId:
        if len(tup) == 2:
            return cls(space=tup[1], external_id=tup[0], instance_type="node")
        elif len(tup) == 3:
            return cls(space=tup[2], external_id=tup[1], instance_type=cast(Literal["node", "edge"], tup[0]))  # type: ignore[misc]
        raise ValueError(f"Invalid number of elements, expected either 2 or 3 got {len(tup)}")


ContainerId = Union[DataModelingId, Tuple[str, str]]
ViewId = Union[VersionedDataModelingId, Tuple[str, str], Tuple[str, str, str]]
DataModelId = Union[VersionedDataModelingId, Tuple[str, str], Tuple[str, str, str]]
InstanceId = Union[TypedDataModelingId, Tuple[str, str], Tuple[str, str, str]]
