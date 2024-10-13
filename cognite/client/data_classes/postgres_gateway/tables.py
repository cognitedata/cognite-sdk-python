from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn, Sequence, Literal

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
class Unknown(CogniteObject):
    type: Literal["raw_rows", "view"] | None
    tablename: str | None


@dataclass
class RawTableOptions(CogniteObject):
    database: str | None
    table: str | None
    primary_key: str | None


@dataclass
class FdmTableOptions(CogniteObject):
    space: str | None
    external_id: str | None
    version: str | None


@dataclass
class CreateFdm(CogniteObject):
    unknown: Unknown
    unknown: Unknown | None
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            unknown=Unknown._load(resource["unknown"], cognite_client),
            unknown=Unknown._load(resource["unknown"], cognite_client) if "unknown" in resource else None,
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["unknown"] = self.unknown.dump(camel_case=camel_case)
        if isinstance(self.unknown, Unknown):
            output["unknown"] = self.unknown.dump(camel_case=camel_case)
        
        return output



@dataclass
class CreateRawTable(CogniteObject):
    unknown: Unknown
    unknown: Unknown | None
    
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            unknown=Unknown._load(resource["unknown"], cognite_client),
            unknown=Unknown._load(resource["unknown"], cognite_client) if "unknown" in resource else None,
        )
    
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["unknown"] = self.unknown.dump(camel_case=camel_case)
        if isinstance(self.unknown, Unknown):
            output["unknown"] = self.unknown.dump(camel_case=camel_case)
        
        return output


class _TableCore(WriteableCogniteResource["TableWrite"], ABC):
    _type: str

    def __init__(self, tablename: str):
        self.tablename = tablename


class TableWrite(_TableCore, ABC):
    """View and create foreign **tables** for a given **user**.

    This is the write/request format of the table.

    Args:

    """

    def __init__(self, ) -> None:
    
    
    def as_write(self) -> TableWrite:
        return self


class Table(_TableCore):
    """Foreign tables.

    

    This is the read/response format of the custom table.

    Args:
        columns (dict | None): Foreign table columns.
        options (Unknown | None): Table options
        type (Literal["raw_rows", "built_in", "nodes", "edges"] | None): The type of table (usually built_in) but could be any of raw_rows, nodes or edges when custom.
        tablename (str | None): None

    """

    def __init__(self, columns: dict | None = None, options: Unknown | None = None, type: Literal["raw_rows", "built_in", "nodes", "edges"] | None = None, tablename: str | None = None) -> None:
        self.columns = columns
        self.options = options
        self.type = type
        self.tablename = tablename

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            columns=resource.get("<UNKNOWN>"),
            options=Unknown._load(resource["options"], cognite_client) if "options" in resource else None,
            type=resource.get("type"),
            tablename=resource.get("tablename"),
        ) 

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.options, Unknown):
            output["options"] = self.options.dump(camel_case=camel_case)
        
        return output


    def as_write(self) -> TableWrite:
        return TableWrite(

        )


class TableWriteList(CogniteResourceList[TableWrite]):
    _RESOURCE = TableWrite


class TableList(WriteableCogniteResourceList[TableWrite, Table]):
    _RESOURCE = Table

    def as_write(self) -> TableWriteList:
        return TableWriteList([item.as_write() for item in self.data])

