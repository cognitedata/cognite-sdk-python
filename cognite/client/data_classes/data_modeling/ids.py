from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Union


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


ContainerId = Union[DataModelingId, Tuple[str, str]]
ViewId = Union[VersionedDataModelingId, Tuple[str, str], Tuple[str, str, str]]
DataModelId = Union[VersionedDataModelingId, Tuple[str, str], Tuple[str, str, str]]
