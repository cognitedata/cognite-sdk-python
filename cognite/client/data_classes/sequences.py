from __future__ import annotations

import typing
import warnings
from abc import ABC
from collections.abc import Iterator
from enum import auto
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeAlias, cast, get_args, overload

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import at_least_one_is_not_none
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient

ValueType: TypeAlias = Literal["String", "Double", "Long"]


class SequenceColumnCore(WriteableCogniteResource["SequenceColumnWrite"], ABC):
    """This represents a column in a sequence.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the column
        description (str | None): Description of the column
        value_type (ValueType): The type of the column. It can be String, Double or Long.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. The maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        value_type: ValueType = "Double",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.description = description
        self.value_type = value_type
        self.metadata = metadata

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        # Snake case is supported for backwards compatibility
        resource = convert_all_keys_to_camel_case(resource)
        return super()._load(resource, cognite_client)


class SequenceColumn(SequenceColumnCore):
    """This represents a column in a sequence. It is used for reading only.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the column
        description (str | None): Description of the column
        value_type (ValueType): The type of the column. It can be String, Double or Long.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        created_time (int | None): Time when this column was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int | None): The last time this column was updated in CDF, in milliseconds since Jan 1, 1970.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        value_type: ValueType = "Double",
        metadata: dict[str, Any] | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            value_type=value_type,
            metadata=metadata,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self) -> SequenceColumnWrite:
        """Returns a writeable version of this column."""
        if self.external_id is None:
            raise ValueError("External ID must be set for the writing version of the column")

        return SequenceColumnWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            value_type=self.value_type,
            metadata=self.metadata,
        )


class SequenceColumnWrite(SequenceColumnCore):
    """This represents a column in a sequence. This is used for writing only.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the column
        description (str | None): Description of the column
        value_type (ValueType): The type of the column. It can be String, Double or Long.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. The maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        value_type: ValueType = "Double",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            value_type=value_type,
            metadata=metadata,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SequenceColumnWrite:
        return cls(
            external_id=resource["externalId"],
            name=resource.get("name"),
            description=resource.get("description"),
            value_type=resource.get("valueType", "Double"),
            metadata=resource.get("metadata"),
        )

    def as_write(self) -> SequenceColumnWrite:
        """Returns this SequenceColumnWrite."""
        return self


class SequenceColumnList(CogniteResourceList[SequenceColumn], ExternalIDTransformerMixin):
    _RESOURCE = SequenceColumn

    @property
    def value_types(self) -> list[ValueType]:
        """Retrieves list of column value types

        Returns:
            list[ValueType]: List of column value types
        """
        return [c.value_type for c in self]

    def as_write(self) -> SequenceColumnWriteList:
        """Returns a writeable version of this column list."""
        return SequenceColumnWriteList([col.as_write() for col in self], cognite_client=self._get_cognite_client())


class SequenceColumnWriteList(CogniteResourceList[SequenceColumnWrite], ExternalIDTransformerMixin):
    _RESOURCE = SequenceColumnWrite

    @property
    def value_types(self) -> list[ValueType]:
        """Retrieves list of column value types

        Returns:
            list[ValueType]: List of column value types
        """
        return [c.value_type for c in self]


class SequenceCore(WriteableCogniteResource["SequenceWrite"], ABC):
    """Information about the sequence stored in the database

    Args:
        name (str | None): Name of the sequence
        description (str | None): Description of the sequence
        asset_id (int | None): Optional asset this sequence is associated with
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. The maximum length of the key is 32 bytes, the value 512 bytes, with up to 16 key-value pairs.
        data_set_id (int | None): Data set that this sequence belongs to
    """

    def __init__(
        self,
        name: str | None = None,
        description: str | None = None,
        asset_id: int | None = None,
        external_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.asset_id = asset_id
        self.external_id = external_id
        self.metadata = metadata
        self.data_set_id = data_set_id


class Sequence(SequenceCore):
    """Information about the sequence stored in the database.
    This is the reading version of the class, it is used for retrieving data from the CDF.

    Args:
        id (int | None): Unique cognite-provided identifier for the sequence
        name (str | None): Name of the sequence
        description (str | None): Description of the sequence
        asset_id (int | None): Optional asset this sequence is associated with
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. The maximum length of the key is 32 bytes, the value 512 bytes, with up to 16 key-value pairs.
        columns (typing.Sequence[SequenceColumn] | None): List of column definitions
        created_time (int | None): Time when this sequence was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int | None): The last time this sequence was updated in CDF, in milliseconds since Jan 1, 1970.
        data_set_id (int | None): Data set that this sequence belongs to
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        name: str | None = None,
        description: str | None = None,
        asset_id: int | None = None,
        external_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        columns: typing.Sequence[SequenceColumn] | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        data_set_id: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            asset_id=asset_id,
            external_id=external_id,
            metadata=metadata,
            data_set_id=data_set_id,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore

        self.columns: SequenceColumnList | None
        if columns is None or isinstance(columns, SequenceColumnList):
            self.columns = columns
        elif isinstance(columns, typing.Sequence) and all(isinstance(col, SequenceColumn) for col in columns):
            self.columns = SequenceColumnList(columns)
        elif isinstance(columns, list):
            warnings.warn(
                "Columns is no longer a dict, you should first load the list of dictionaries using SequenceColumnList.load([{...}, {...}])",
                DeprecationWarning,
            )
            self.columns = SequenceColumnList._load(columns)
        else:
            raise ValueError(f"columns must be a sequence of SequenceColumn objects not {type(columns)}")
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        loaded = super()._load(resource, cognite_client)
        if loaded.columns is not None:
            loaded.columns = SequenceColumnList._load(loaded.columns)
        return loaded

    def as_write(self) -> SequenceWrite:
        """Returns a writeable version of this sequence."""
        if self.columns is None:
            raise ValueError("Columns must be set for the writing version of the sequence")

        return SequenceWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            asset_id=self.asset_id,
            metadata=self.metadata,
            data_set_id=self.data_set_id,
            columns=self.columns.as_write(),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if self.columns is not None:
            dumped["columns"] = self.columns.dump(camel_case)
        return dumped

    def rows(self, start: int, end: int | None) -> SequenceRows:
        """Retrieves rows from this sequence.

        Args:
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to get all rows until end of sequence.

        Returns:
            SequenceRows: List of sequence data.
        """
        if self.external_id is not None:
            return self._cognite_client.sequences.rows.retrieve(external_id=self.external_id, start=start, end=end)
        elif self.id is not None:
            return self._cognite_client.sequences.rows.retrieve(id=self.id, start=start, end=end)
        raise ValueError("Sequence must have either id or external_id set")

    @property
    def column_external_ids(self) -> list[str]:
        """Retrieves list of column external ids for the sequence, for use in e.g. data retrieve or insert methods

        Returns:
            list[str]: List of sequence column external ids
        """
        assert self.columns is not None
        return self.columns.as_external_ids()

    @property
    def column_value_types(self) -> list[ValueType]:
        """Retrieves list of column value types

        Returns:
            list[ValueType]: List of column value types
        """
        assert self.columns is not None
        return self.columns.value_types


class SequenceWrite(SequenceCore):
    """Information about the sequence stored in the database.
    This is the writing version of the class, it is used for inserting data into the CDF.

    Args:
        columns (typing.Sequence[SequenceColumnWrite]): List of column definitions
        name (str | None): Name of the sequence
        description (str | None): Description of the sequence
        asset_id (int | None): Optional asset this sequence is associated with
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        metadata (dict[str, Any] | None): Custom, application-specific metadata. String key -> String value. Th maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        data_set_id (int | None): Data set that this sequence belongs to
    """

    def __init__(
        self,
        columns: typing.Sequence[SequenceColumnWrite],
        name: str | None = None,
        description: str | None = None,
        asset_id: int | None = None,
        external_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            asset_id=asset_id,
            external_id=external_id,
            metadata=metadata,
            data_set_id=data_set_id,
        )
        self.columns: SequenceColumnWriteList
        if isinstance(columns, SequenceColumnWriteList):
            self.columns = columns
        elif isinstance(columns, typing.Sequence) and all(isinstance(col, SequenceColumnWrite) for col in columns):
            self.columns = SequenceColumnWriteList(columns)
        else:
            raise ValueError(f"columns must be a sequence of SequenceColumnWrite objects not {type(columns)}")

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            columns=SequenceColumnWriteList._load(resource["columns"]),
            name=resource.get("name"),
            description=resource.get("description"),
            asset_id=resource.get("assetId"),
            external_id=resource.get("externalId"),
            metadata=resource.get("metadata"),
            data_set_id=resource.get("dataSetId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        dumped["columns"] = self.columns.dump(camel_case)
        return dumped

    def as_write(self) -> SequenceWrite:
        """Returns this SequenceWrite."""
        return self


class SequenceFilter(CogniteFilter):
    """No description.

    Args:
        name (str | None): Return only sequences with this *exact* name.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        metadata (dict[str, Any] | None): Filter the sequences by metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (typing.Sequence[int] | None): Return only sequences linked to one of the specified assets.
        asset_subtree_ids (typing.Sequence[dict[str, Any]] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        data_set_ids (typing.Sequence[dict[str, Any]] | None): Only include sequences that belong to these datasets.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, Any] | None = None,
        asset_ids: typing.Sequence[int] | None = None,
        asset_subtree_ids: typing.Sequence[dict[str, Any]] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: typing.Sequence[dict[str, Any]] | None = None,
    ) -> None:
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_ids = data_set_ids


class SequenceColumnUpdate(CogniteUpdate):
    """This class is used to update a sequence column."""

    class _PrimitiveSequenceColumnUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> SequenceColumnUpdate:
            return self._set(value)

    class _ObjectSequenceColumnUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> SequenceColumnUpdate:
            return self._set(value)

        def add(self, value: dict) -> SequenceColumnUpdate:
            return self._add(value)

        def remove(self, value: list) -> SequenceColumnUpdate:
            return self._remove(value)

    @property
    def description(self) -> _PrimitiveSequenceColumnUpdate:
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, "description")

    @property
    def external_id(self) -> _PrimitiveSequenceColumnUpdate:
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveSequenceColumnUpdate:
        return SequenceColumnUpdate._PrimitiveSequenceColumnUpdate(self, "name")

    @property
    def metadata(self) -> _ObjectSequenceColumnUpdate:
        return SequenceColumnUpdate._ObjectSequenceColumnUpdate(self, "metadata")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("description"),
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("metadata", is_object=True),
        ]


class SequenceUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveSequenceUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> SequenceUpdate:
            return self._set(value)

    class _ObjectSequenceUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> SequenceUpdate:
            return self._set(value)

        def add(self, value: dict) -> SequenceUpdate:
            return self._add(value)

        def remove(self, value: list) -> SequenceUpdate:
            return self._remove(value)

    class _ListSequenceUpdate(CogniteListUpdate):
        def set(self, value: list) -> SequenceUpdate:
            return self._set(value)

        def add(self, value: list) -> SequenceUpdate:
            return self._add(value)

        def remove(self, value: list) -> SequenceUpdate:
            return self._remove(value)

    class _LabelSequenceUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> SequenceUpdate:
            return self._add(value)

        def remove(self, value: list) -> SequenceUpdate:
            return self._remove(value)

    class _ColumnsSequenceUpdate(CogniteListUpdate):
        def add(self, value: dict | list[dict]) -> SequenceUpdate:
            single_item = not isinstance(value, list)
            if single_item:
                value_list = cast(list[str], [value])
            else:
                value_list = cast(list[str], value)

            return self._add(value_list)

        def remove(self, value: str | list[str]) -> SequenceUpdate:
            single_item = not isinstance(value, list)
            if single_item:
                value_list = cast(list[str], [value])
            else:
                value_list = cast(list[str], value)

            return self._remove([{"externalId": id} for id in value_list])

        def modify(self, value: list[SequenceColumnUpdate]) -> SequenceUpdate:
            return self._modify([col.dump() for col in value])

    @property
    def name(self) -> _PrimitiveSequenceUpdate:
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveSequenceUpdate:
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "description")

    @property
    def asset_id(self) -> _PrimitiveSequenceUpdate:
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "assetId")

    @property
    def external_id(self) -> _PrimitiveSequenceUpdate:
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "externalId")

    @property
    def metadata(self) -> _ObjectSequenceUpdate:
        return SequenceUpdate._ObjectSequenceUpdate(self, "metadata")

    @property
    def data_set_id(self) -> _PrimitiveSequenceUpdate:
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "dataSetId")

    @property
    def columns(self) -> _ColumnsSequenceUpdate:
        return SequenceUpdate._ColumnsSequenceUpdate(self, "columns")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("description"),
            PropertySpec("asset_id"),
            # Sequences do not support setting metadata to an empty array.
            PropertySpec("metadata", is_object=True, is_nullable=False),
            PropertySpec("data_set_id"),
            # PropertySpec("columns", is_list=True),
        ]


class SequenceWriteList(CogniteResourceList[SequenceWrite], ExternalIDTransformerMixin):
    _RESOURCE = SequenceWrite


class SequenceList(WriteableCogniteResourceList[SequenceWrite, Sequence], IdTransformerMixin):
    _RESOURCE = Sequence

    def as_write(self) -> SequenceWriteList:
        """Returns a writeable version of this sequence list."""
        return SequenceWriteList([item.as_write() for item in self], cognite_client=self._get_cognite_client())


RowValues: TypeAlias = int | str | float | None


class SequenceRow(CogniteResource):
    """This class represents a row in a sequence. It is used for both read and write.

    Args:
        row_number (int): The row number for this row.
        values (typing.Sequence[RowValues]): List of values in the order defined in the columns field. Number of items must match. Null is accepted for missing values. String values must be no longer than 256 characters.

    """

    def __init__(self, row_number: int, values: typing.Sequence[RowValues]) -> None:
        self.row_number = row_number
        self.values = values

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            row_number=resource["rowNumber"],
            values=resource["values"],
        )


ColumnNames: TypeAlias = Literal[
    "externalId",
    "id",
    "columnExternalId",
    "id|columnExternalId",
    "externalId|columnExternalId",
]

_VALID_COLUMN_NAMES = set(get_args(ColumnNames))


class SequenceRows(CogniteResource):
    """An object representing a list of rows from a sequence.

    Args:
        rows (typing.Sequence[SequenceRow]): The sequence rows.
        columns (SequenceColumnList): The column information.
        id (int | None): Identifier of the sequence the data belong to
        external_id (str | None): External id of the sequence the data belong to
    """

    def __init__(
        self,
        rows: typing.Sequence[SequenceRow],
        columns: SequenceColumnList,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        if not at_least_one_is_not_none(id, external_id):
            raise ValueError("At least one of id and external_id must be specified.")

        col_length = len(columns)
        if wrong_length := [r for r in rows if len(r.values) != col_length]:
            raise ValueError(
                f"Rows { [r.row_number for r in wrong_length] } have wrong number of values, expected {col_length}"
            )
        self.rows = rows
        self.columns: SequenceColumnList = columns
        self.id = id
        self.external_id = external_id

    def __str__(self) -> str:
        return _json.dumps(self, indent=4)

    def __len__(self) -> int:
        return len(self.rows)

    @overload
    def __getitem__(self, item: int) -> list[RowValues]: ...

    @overload
    def __getitem__(self, item: slice) -> NoReturn: ...

    def __getitem__(self, item: int | slice) -> list[RowValues]:
        if isinstance(item, slice):
            raise TypeError("Slicing is not supported for SequenceRows")
            # Todo: Solution below needs more work
            # return [list(self.rows[i].values) for i in range(item.start or 0, item.stop or len(self), item.step or 1)]
        else:
            return list(self.rows[item].values)

    def get_column(self, external_id: str) -> list[RowValues]:
        """Get a column by external_id.

        Args:
            external_id (str): External id of the column.

        Returns:
            list[RowValues]: A list of values for that column in the sequence
        """
        try:
            ix = self.column_external_ids.index(external_id)
        except ValueError:
            raise ValueError(
                f"Column {external_id} not found, Sequence column external ids are {self.column_external_ids}"
            )
        return [row.values[ix] for row in self.rows]

    def items(self) -> Iterator[tuple[int, list[RowValues]]]:
        """Returns an iterator over tuples of (row number, values)."""
        for row in self.rows:
            yield row.row_number, list(row.values)

    @property
    def values(self) -> list[list[RowValues]]:
        """Returns a list of lists of values"""
        return [list(row.values) for row in self.rows]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the sequence data into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        key = "rowNumber" if camel_case else "row_number"
        dumped: dict[str, Any] = {
            "columns": self.columns.dump(camel_case),
            "rows": [{key: r.row_number, "values": r.values} for r in self.rows],
        }
        if self.id is not None:
            dumped["id"] = self.id
        if self.external_id is not None:
            dumped["externalId" if camel_case else "external_id"] = self.external_id
        return dumped

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            rows=[SequenceRow._load(r) for r in resource["rows"]],
            columns=SequenceColumnList._load(resource["columns"]),
            id=resource.get("id"),
            external_id=resource.get("externalId"),
        )

    def to_pandas(self, column_names: ColumnNames = "columnExternalId") -> pandas.DataFrame:  # type: ignore[override]
        """Convert the sequence data into a pandas DataFrame.

        Args:
            column_names (ColumnNames): Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId".

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = local_import("pandas")

        if column_names not in _VALID_COLUMN_NAMES:
            raise ValueError(
                f"Invalid column_names value '{column_names}', should be one of {list(_VALID_COLUMN_NAMES)}"
            )

        column_fmt = (
            column_names.replace("columnExternalId", "{columnExternalId}")
            .replace("externalId", "{externalId}")
            .replace("id", "{id}")
        )
        df_columns = [
            column_fmt.format(id=str(self.id), externalId=str(self.external_id), columnExternalId=eid)
            for eid in self.column_external_ids
        ]
        index: Any
        values: Any
        if self.rows:
            index, values = list(zip(*self.items()))
        else:
            index, values = [], []
        return pd.DataFrame(values, index=index, columns=df_columns).convert_dtypes(
            convert_string=True,
            convert_integer=True,
            # Keep nan as missing in float columns:
            convert_floating=False,
        )

    @property
    def column_external_ids(self) -> list[str]:
        """Retrieves list of column external ids for the sequence, for use in e.g. data retrieve or insert methods.

        Returns:
            list[str]: List of sequence column external ids.
        """
        assert self.columns is not None
        return self.columns.as_external_ids()

    @property
    def column_value_types(self) -> list[ValueType]:
        """Retrieves list of column value types.

        Returns:
            list[ValueType]: List of column value types
        """
        assert self.columns is not None
        return self.columns.value_types


class SequenceData(SequenceRows):
    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        rows: typing.Sequence[dict] | typing.Sequence[SequenceRow] | None = None,
        row_numbers: typing.Sequence[int] | None = None,
        values: typing.Sequence[typing.Sequence[int | str | float]] | None = None,
        columns: typing.Sequence[dict[str, Any]] | SequenceColumnList | None = None,
    ):
        # Conversion for backwards compatibility
        rows_parsed: list[SequenceRow]
        if rows and isinstance(rows, list) and rows and isinstance(rows[0], dict):
            rows_parsed = [SequenceRow._load(r) for r in rows]
        elif rows and isinstance(rows, list) and rows and isinstance(rows[0], SequenceRow):
            rows_parsed = rows
        elif (row_numbers and values) and not rows:
            if len(row_numbers) != len(values):
                raise ValueError(
                    f"row_numbers and values must have same length, got {len(row_numbers)} and {len(values)}"
                )
            warnings.warn("row_numbers and values are deprecated, use rows instead", DeprecationWarning, stacklevel=2)
            rows_parsed = [SequenceRow(row_number, value) for row_number, value in zip(row_numbers, values)]
        else:
            raise ValueError("Either rows or both row_numbers and values must be specified")

        if columns is None:
            raise ValueError("columns must be specified")
        is_column_loaded = isinstance(columns, typing.Sequence) and columns and isinstance(columns[0], SequenceColumn)
        super().__init__(
            id=id,
            external_id=external_id,
            columns=SequenceColumnList._load(columns) if not is_column_loaded else SequenceColumnList(columns),
            rows=rows_parsed,
        )


class SequenceRowsList(CogniteResourceList[SequenceRows]):
    _RESOURCE = SequenceRows

    def __str__(self) -> str:
        return _json.dumps(self, indent=4)

    @overload  # type: ignore[override]
    def to_pandas(
        self,
        key: Literal["id", "external_id"] = "external_id",
        column_names: ColumnNames = "externalId|columnExternalId",
        concat: Literal[True] = True,
    ) -> pandas.DataFrame: ...

    @overload
    def to_pandas(
        self,
        key: Literal["external_id"] = "external_id",
        column_names: ColumnNames = "externalId|columnExternalId",
        concat: Literal[False] = False,
    ) -> dict[str, pandas.DataFrame]: ...

    @overload
    def to_pandas(
        self,
        key: Literal["id"],
        column_names: ColumnNames = "externalId|columnExternalId",
        concat: Literal[False] = False,
    ) -> dict[int, pandas.DataFrame]: ...

    def to_pandas(
        self,
        key: Literal["id", "external_id"] = "external_id",
        column_names: ColumnNames = "externalId|columnExternalId",
        concat: bool = False,
    ) -> pandas.DataFrame | dict[str, pandas.DataFrame] | dict[int, pandas.DataFrame]:
        """Convert the sequence data list into a pandas DataFrame. Each column will be a sequence.

        Args:
            key (Literal['id', 'external_id']): If concat = False, this decides which field to use as key in the dictionary. Defaults to "external_id".
            column_names (ColumnNames): Which field to use as column header. Can use any combination of "externalId", "columnExternalId", "id" and other characters as a template.
            concat (bool): Whether to concatenate the sequences into a single DataFrame or return a dictionary of DataFrames. Defaults to False.

        Returns:
            pandas.DataFrame | dict[str, pandas.DataFrame] | dict[int, pandas.DataFrame]: The sequence data list as a pandas DataFrame.
        """
        pd = local_import("pandas")
        if concat:
            return pd.concat([seq.to_pandas(column_names) for seq in self], axis=1)

        if key == "external_id":
            if not all(seq.external_id is not None for seq in self):
                raise ValueError("All sequences must have external_id set")
            return {seq.external_id: seq.to_pandas(column_names) for seq in self}
        elif key == "id":
            if not all(seq.id is not None for seq in self):
                raise ValueError("All sequences must have id set")
            return {seq.id: seq.to_pandas(column_names) for seq in self}

        raise ValueError(f"Invalid key value '{key}', should be one of ['id', 'external_id']")

    def _repr_html_(self) -> str:
        return self.to_pandas(key="external_id", concat=True)._repr_html_()


SequenceDataList = SequenceRowsList


class SequenceProperty(EnumProperty):
    description = auto()
    external_id = auto()
    name = auto()  # type: ignore [assignment]
    asset_id = auto()
    asset_root_id = auto()
    created_time = auto()
    data_set_id = auto()
    id = auto()
    last_updated_time = auto()
    access_categories = auto()
    metadata = auto()

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


class SortableSequenceProperty(EnumProperty):
    asset_id = auto()
    created_time = auto()
    data_set_id = auto()
    description = auto()
    external_id = auto()
    last_updated_time = auto()
    name = auto()  # type: ignore [assignment]

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


SortableSequencePropertyLike: TypeAlias = SortableSequenceProperty | str | list[str]


class SequenceSort(CogniteSort):
    def __init__(
        self,
        property: SortableSequenceProperty,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
