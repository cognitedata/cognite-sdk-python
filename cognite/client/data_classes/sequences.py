from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterator, List, Literal, Union, cast, overload
from typing import Sequence as SequenceType

from typing_extensions import Self, TypeAlias

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    NoCaseConversionPropertyList,
    PropertySpec,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._auxiliary import at_least_one_is_not_none
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._importing import local_import

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient

ValueType: TypeAlias = Literal["String", "Double", "Long"]


class SequenceColumn(CogniteResource):
    """This represents a column in a sequence. It is used for both read and write.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the column
        description (str | None): Description of the column
        value_type (ValueType): The type of the column. Can be String, Double or Long.
        metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value. Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
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
        self.external_id = external_id
        self.name = name
        self.description = description
        self.value_type = value_type
        self.metadata = metadata
        self.created_time = created_time
        self.last_updated_time = last_updated_time


class SequenceColumnList(CogniteResourceList[SequenceColumn], ExternalIDTransformerMixin):
    _RESOURCE = SequenceColumn

    @property
    def value_types(self) -> list[ValueType]:
        """Retrieves list of column value types

        Returns:
            list[ValueType]: List of column value types
        """
        return [c.value_type for c in self]


class Sequence(CogniteResource):
    """Information about the sequence stored in the database

    Args:
        id (int | None): Unique cognite-provided identifier for the sequence
        name (str | None): Name of the sequence
        description (str | None): Description of the sequence
        asset_id (int | None): Optional asset this sequence is associated with
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value. Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        columns (SequenceColumnList | None): List of column definitions
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
        columns: SequenceColumnList | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        data_set_id: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        if not isinstance(columns, SequenceColumnList) and columns is not None:
            raise ValueError("columns must be a SequenceColumnList")
        self.id = id
        self.name = name
        self.description = description
        self.asset_id = asset_id
        self.external_id = external_id
        self.metadata = metadata
        self.columns = columns
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_id = data_set_id
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        loaded = super()._load(resource, cognite_client)
        if loaded.columns is not None:
            loaded.columns = SequenceColumnList._load(loaded.columns)
        return loaded

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if self.columns is not None:
            dumped["columns"] = self.columns.dump(camel_case)
        return dumped

    def rows(self, start: int, end: int | None) -> SequenceData:
        """Retrieves rows from this sequence.

        Args:
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to get all rows until end of sequence.

        Returns:
            SequenceData: List of sequence data.
        """
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        return self._cognite_client.sequences.data.retrieve(**identifier, start=start, end=end)

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


class SequenceFilter(CogniteFilter):
    """No description.

    Args:
        name (str | None): Return only sequences with this *exact* name.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        metadata (dict[str, Any] | None): Filter the sequences by metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (SequenceType[int] | None): Return only sequences linked to one of the specified assets.
        asset_subtree_ids (SequenceType[dict[str, Any]] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        data_set_ids (SequenceType[dict[str, Any]] | None): Only include sequences that belong to these datasets.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, Any] | None = None,
        asset_ids: SequenceType[int] | None = None,
        asset_subtree_ids: SequenceType[dict[str, Any]] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: SequenceType[dict[str, Any]] | None = None,
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
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("description"),
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("metadata", is_container=True),
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
                value_list = cast(List[str], [value])
            else:
                value_list = cast(List[str], value)

            return self._add(value_list)

        def remove(self, value: str | list[str]) -> SequenceUpdate:
            single_item = not isinstance(value, list)
            if single_item:
                value_list = cast(List[str], [value])
            else:
                value_list = cast(List[str], value)

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
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("description"),
            PropertySpec("asset_id"),
            # Sequences do not support setting metadata to an empty array.
            PropertySpec("metadata", is_container=True, is_nullable=False),
            PropertySpec("data_set_id"),
            # PropertySpec("columns", is_list=True),
        ]


class SequenceAggregate(dict):
    """No description.

    Args:
        count (int | None): No description.
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, **kwargs: Any) -> None:
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class SequenceList(CogniteResourceList[Sequence], IdTransformerMixin):
    _RESOURCE = Sequence


RowValues: TypeAlias = Union[int, str, float, None]


class SequenceRow(CogniteResource):
    """This class represents a row in a sequence. It is used for both read and write.

    Args:
        row_number (int): The row number for this row.
        values (SequenceType[RowValues]): List of values in the order defined in the columns field. Number of items must match. Null is accepted for missing values. String values must be no longer than 256 characters.

    """

    def __init__(self, row_number: int, values: SequenceType[RowValues]) -> None:
        self.row_number = row_number
        self.values = values


class SequenceData(CogniteResource):
    """An object representing a list of rows from a sequence.

    Args:
        rows (SequenceType[SequenceRow]): The sequence rows.
        columns (SequenceColumnList): The column information.
        id (int | None): Identifier of the sequence the data belong to
        external_id (str | None): External id of the sequence the data belong to
    """

    def __init__(
        self,
        rows: SequenceType[SequenceRow],
        columns: SequenceColumnList,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        if not at_least_one_is_not_none(id, external_id):
            raise ValueError("One of id or external_id must be specified")

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
        return json.dumps(self.dump(), indent=4)

    def __len__(self) -> int:
        return len(self.rows)

    @overload
    def __getitem__(self, item: int) -> list[RowValues]:
        ...

    @overload
    def __getitem__(self, item: slice) -> list[list[RowValues]]:
        ...

    def __getitem__(self, item: int | slice) -> list[RowValues] | list[list[RowValues]]:
        # slow, should be replaced by dict cache if it sees more than incidental use
        if isinstance(item, slice):
            return [list(self.rows[i].values) for i in range(item.start or 0, item.stop or len(self), item.step or 1)]
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

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the sequence data into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        dumped: dict[str, Any] = {
            "columns": self.columns.dump(camel_case),
            "rows": [
                {("rowNumber" if camel_case else "row_number"): r.row_number, "values": r.values} for r in self.rows
            ],
        }
        if self.id is not None:
            dumped["id"] = self.id
        if self.external_id is not None:
            dumped[("externalId" if camel_case else "external_id")] = self.external_id
        return dumped

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            rows=[SequenceRow(**r) for r in resource["rows"]],
            columns=SequenceColumnList._load(resource["columns"]),
            id=resource.get("id"),
            external_id=resource.get("externalId"),
        )

    def to_pandas(self, column_names: str = "columnExternalId") -> pandas.DataFrame:  # type: ignore[override]
        """Convert the sequence data into a pandas DataFrame.

        Args:
            column_names (str):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId".

        Returns:
            pandas.DataFrame: The dataframe.
        """
        raise NotImplementedError
        # pd = local_import("pandas")
        #
        # options = ["externalId", "id", "columnExternalId", "id|columnExternalId", "externalId|columnExternalId"]
        # if column_names not in options:
        #     raise ValueError(f"Invalid column_names value '{column_names}', should be one of {options}")
        #
        # column_names = (
        #     column_names.replace("columnExternalId", "{columnExternalId}")
        #     .replace("externalId", "{externalId}")
        #     .replace("id", "{id}")
        # )
        # df_columns = [
        #     column_names.format(id=str(self.id), externalId=str(self.external_id), columnExternalId=eid)
        #     for eid in self.column_external_ids
        # ]
        # # TODO: Optimization required (None/nan):
        # return pd.DataFrame(
        #     [[x if x is not None else math.nan for x in r] for r in self.values],
        #     index=self.row_numbers,
        #     columns=df_columns,
        # )

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


class SequenceDataList(CogniteResourceList[SequenceData]):
    _RESOURCE = SequenceData

    def __str__(self) -> str:
        return json.dumps(self.dump(), indent=4)

    # def to_pandas(
    #     self,
    #     camel_case: bool = False,
    #     expand_metadata: bool = False,
    #     metadata_prefix: str = "metadata.",
    # ) -> pandas.DataFrame:

    def to_pandas(self, column_names: str = "externalId|columnExternalId") -> pandas.DataFrame:  # type: ignore[override]
        """Convert the sequence data list into a pandas DataFrame. Each column will be a sequence.

        Args:
            column_names (str): Which field to use as column header. Can use any combination of "externalId", "columnExternalId", "id" and other characters as a template.

        Returns:
            pandas.DataFrame: The sequence data list as a pandas DataFrame.
        """
        pd = local_import("pandas")
        return pd.concat([seq_data.to_pandas(column_names=column_names) for seq_data in self.data], axis=1)


class SequenceProperty(EnumProperty):
    description = "description"
    external_id = "externalId"
    name = "name"
    asset_id = "assetId"
    asset_root_id = "assetRootId"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    id = "id"
    last_updated_time = "lastUpdatedTime"
    access_categories = "accessCategories"
    metadata = "metadata"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return NoCaseConversionPropertyList(["metadata", key])


class SortableSequenceProperty(EnumProperty):
    asset_id = "assetId"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    description = "description"
    external_id = "externalId"
    last_updated_time = "lastUpdatedTime"
    name = "name"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return NoCaseConversionPropertyList(["metadata", key])


SortableSequencePropertyLike: TypeAlias = Union[SortableSequenceProperty, str, List[str]]


class SequenceSort(CogniteSort):
    def __init__(
        self,
        property: SortableSequenceProperty,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
