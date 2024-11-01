from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling.ids import ViewId

if TYPE_CHECKING:
    from cognite.client import CogniteClient

ColumnType: TypeAlias = Literal[
    "TEXT",
    "VARCHAR",
    "BOOL",
    "BIGINT",
    "DOUBLE PRECISION",
    "REAL",
    "TIMESTAMPTZ",
    "JSON",
    "TEXT[]",
    "VARCHAR[]",
    "BOOL[]",
    "BIGINT[]",
    "DOUBLE PRECISION[]",
    "REAL[]",
    "JSON[]",
    "BYTEA",
]


@dataclass
class RawTableOptions(CogniteObject):
    database: str
    table: str
    primary_key: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            database=resource["database"],
            table=resource["table"],
            primary_key=resource.get("primaryKey"),
        )


@dataclass
class Column(CogniteResource):
    name: str
    type: ColumnType

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["propertyName"],
            type=resource["type"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "propertyName": self.name,
            "type": self.type,
        }


class ColumnList(CogniteResourceList[Column]):
    _RESOURCE = Column

    @classmethod
    def _load_columns(cls, data: dict[str, Any]) -> ColumnList:
        columns = cls([], None)
        for name, column_data in data.items():
            columns.append(
                Column(
                    name=name,
                    type=column_data["type"],
                )
            )
        return columns

    def _dump_columns(self) -> dict[str, Any]:
        return {column.name: {"type": column.type} for column in self.data}


class _TableCore(WriteableCogniteResource["TableWrite"], ABC):
    _type: str

    def __init__(self, tablename: str):
        self.tablename = tablename

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"tablename": self.tablename, "type": self._type}


class TableWrite(_TableCore, ABC):
    """View and create foreign **tables** for a given **user**.

    This is the write/request format of the table.
    """

    def as_write(self) -> Self:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        try:
            return cast(Self, _TABLE_WRITE_CLASS_BY_TYPE[type_]._load_table(resource))
        except KeyError:
            raise TypeError(f"Unknown source type: {type_}")

    @classmethod
    @abstractmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError()


class RawTableWrite(TableWrite):
    """Foreign tables.

    This is the read/response format of the raw table.

    Args:
        tablename (str): Name of the foreign table.
        options (RawTableOptions): Table options
        columns (Sequence[Column] | ColumnList): Foreign table columns.

    """

    _type = "raw_rows"

    def __init__(self, tablename: str, options: RawTableOptions, columns: Sequence[Column] | ColumnList) -> None:
        super().__init__(tablename=tablename)
        self.options = options
        self.columns: ColumnList = ColumnList(columns)

    @classmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        return cls(
            tablename=data["tablename"],
            options=RawTableOptions._load(data["options"]),
            columns=ColumnList._load_columns(data["columns"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["options"] = self.options.dump(camel_case=camel_case)
        output["columns"] = self.columns._dump_columns()
        return output


class ViewTableWrite(TableWrite):
    """Foreign tables.

    This is the read/response format of the custom table.

    Args:
        tablename (str): Name of the foreign table.
        options (ViewId): Table options
    """

    _type = "view"

    def __init__(self, tablename: str, options: ViewId) -> None:
        super().__init__(tablename=tablename)
        self.options = options

    @classmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        return cls(
            tablename=data["tablename"],
            options=ViewId.load(data["options"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["options"] = self.options.dump(camel_case=camel_case, include_type=False)
        return output


class Table(_TableCore, ABC):
    """Foreign tables.

    This is the read/response format of the custom table.

    Args:
        tablename (str): Name of the foreign table.
        created_time (int | None): Time when the table was created

    """

    def __init__(self, tablename: str, created_time: int | None = None) -> None:
        super().__init__(tablename=tablename)
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        table_cls = _TABLE_CLASS_BY_TYPE.get(type_)
        if table_cls is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, table_cls._load_table(resource))

    @classmethod
    @abstractmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError()


class RawTable(Table):
    """Foreign tables.

    This is the read/response format of the raw table.

    Args:
        tablename (str): Name of the foreign table.
        options (RawTableOptions): Table options
        columns (ColumnList): Foreign table columns.
        created_time (int | None): Time when the table was created.

    """

    _type = "raw_rows"

    def __init__(
        self, tablename: str, options: RawTableOptions, columns: ColumnList, created_time: int | None = None
    ) -> None:
        super().__init__(tablename=tablename, created_time=created_time)
        self.options = options
        self.columns = columns

    @classmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        return cls(
            tablename=data["tablename"],
            options=RawTableOptions._load(data["options"]),
            columns=ColumnList._load_columns(data["columns"]),
            created_time=data.get("createdTime"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["options"] = self.options.dump(camel_case=camel_case)
        output["columns"] = self.columns.dump(camel_case=camel_case)
        return output

    def as_write(self) -> RawTableWrite:
        return RawTableWrite(
            tablename=self.tablename,
            options=self.options,
            columns=self.columns,
        )


class ViewTable(Table):
    """Foreign tables.

    This is the read/response format of the custom table.

    Args:
        tablename (str): Name of the foreign table.
        options (ViewId): Table options
        created_time (int | None): Time when the table was created.
    """

    _type = "view"

    def __init__(self, tablename: str, options: ViewId, created_time: int | None = None) -> None:
        super().__init__(tablename=tablename, created_time=created_time)
        self.options = options

    @classmethod
    def _load_table(cls, data: dict[str, Any]) -> Self:
        return cls(
            tablename=data["tablename"],
            options=ViewId.load(data["options"]),
            created_time=data.get("created_time"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["options"] = self.options.dump(camel_case=camel_case)
        return output

    def as_write(self) -> ViewTableWrite:
        return ViewTableWrite(
            tablename=self.tablename,
            options=self.options,
        )


class TableWriteList(CogniteResourceList[TableWrite]):
    _RESOURCE = TableWrite


class TableList(WriteableCogniteResourceList[TableWrite, Table]):
    _RESOURCE = Table

    def as_write(self) -> TableWriteList:
        return TableWriteList([item.as_write() for item in self.data])


_TABLE_WRITE_CLASS_BY_TYPE: dict[str, type[TableWrite]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in TableWrite.__subclasses__()
}

_TABLE_CLASS_BY_TYPE: dict[str, type[Table]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in Table.__subclasses__()
}
