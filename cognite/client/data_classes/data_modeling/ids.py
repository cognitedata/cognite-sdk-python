from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, Union, cast

from cognite.client.utils._identifier import DataModelingIdentifier, DataModelingIdentifierSequence


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

Id = Union[Tuple[str, str], Tuple[str, str, str], DataModelingId, VersionedDataModelingId]


def load_identifier(ids: Id | Sequence[Id]) -> DataModelingIdentifierSequence:
    is_sequence = isinstance(ids, Sequence) and not (isinstance(ids, tuple) and isinstance(ids[0], str))
    id_list = cast(Sequence, ids)
    if not is_sequence:
        id_list = [ids]

    return DataModelingIdentifierSequence(
        identifiers=[
            DataModelingIdentifier(*id_)
            if isinstance(id_, tuple)
            else DataModelingIdentifier(id_.space, id_.external_id, id_.version if hasattr(id_, "version") else None)
            for id_ in id_list
        ],
        is_singleton=len(id_list) == 1,
    )
