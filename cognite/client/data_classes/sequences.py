import math
from typing import *
from typing import List

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


class Sequence(CogniteResource):
    """Information about the sequence stored in the database

    Args:
        id (int): Unique cognite-provided identifier for the sequence
        name (str): Name of the sequence
        description (str): Description of the sequence
        asset_id (int): Optional asset this sequence is associated with
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value. Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        columns (List[Dict[str, Any]]): List of column definitions
        created_time (int): Time when this sequence was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int): The last time this sequence was updated in CDF, in milliseconds since Jan 1, 1970.
        data_set_id (int): Data set that this sequence belongs to
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        asset_id: int = None,
        external_id: str = None,
        metadata: Dict[str, Any] = None,
        columns: List[Dict[str, Any]] = None,
        created_time: int = None,
        last_updated_time: int = None,
        data_set_id: int = None,
        cognite_client=None,
    ):
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
        self._cognite_client = cognite_client

    def rows(self, start: int, end: int) -> List[dict]:
        """Retrieves rows from this sequence.

        Returns:
            List of sequence data.
        """
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        return self._cognite_client.sequences.data.retrieve(**identifier, start=start, end=end)

    @property
    def column_external_ids(self):
        """Retrieves list of column external ids for the sequence, for use in e.g. data retrieve or insert methods

        Returns:
            List of sequence column external ids
        """
        return [c.get("externalId") for c in self.columns]

    @property
    def column_value_types(self):
        """Retrieves list of column value types

        Returns:
            List of column value types
        """
        return [c.get("valueType") for c in self.columns]


class SequenceFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Return only sequences with this *exact* name.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        metadata (Dict[str, Any]): Filter the sequences by metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (List[int]): Return only sequences linked to one of the specified assets.
        root_asset_ids (List[int]): Only include sequences that have a related asset in a tree rooted at any of these root assetIds.
        asset_subtree_ids (List[Dict[str, Any]]): Only include sequences that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        data_set_ids (List[Dict[str, Any]]): Only include sequences that belong to these datasets.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        root_asset_ids: List[int] = None,
        asset_subtree_ids: List[Dict[str, Any]] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        data_set_ids: List[Dict[str, Any]] = None,
        cognite_client=None,
    ):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.root_asset_ids = root_asset_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_ids = data_set_ids
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(SequenceFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance


class SequenceUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveSequenceUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "SequenceUpdate":
            return self._set(value)

    class _ObjectSequenceUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "SequenceUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "SequenceUpdate":
            return self._add(value)

        def remove(self, value: List) -> "SequenceUpdate":
            return self._remove(value)

    class _ListSequenceUpdate(CogniteListUpdate):
        def set(self, value: List) -> "SequenceUpdate":
            return self._set(value)

        def add(self, value: List) -> "SequenceUpdate":
            return self._add(value)

        def remove(self, value: List) -> "SequenceUpdate":
            return self._remove(value)

    class _LabelSequenceUpdate(CogniteLabelUpdate):
        def add(self, value: List) -> "SequenceUpdate":
            return self._add(value)

        def remove(self, value: List) -> "SequenceUpdate":
            return self._remove(value)

    @property
    def name(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "name")

    @property
    def description(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "description")

    @property
    def asset_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "assetId")

    @property
    def external_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "externalId")

    @property
    def metadata(self):
        return SequenceUpdate._ObjectSequenceUpdate(self, "metadata")

    @property
    def data_set_id(self):
        return SequenceUpdate._PrimitiveSequenceUpdate(self, "dataSetId")


class SequenceAggregate(dict):
    """No description.

    Args:
        count (int): No description.
    """

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class SequenceList(CogniteResourceList):
    _RESOURCE = Sequence
    _UPDATE = SequenceUpdate


class SequenceData:
    """An object representing a list of rows from a sequence.

    Args:
        id (int): Id of the sequence the data belong to
        external_id (str): External id of the sequence the data belong to
        rows (List[dict]): Combined row numbers and row data object from the API. If you pass this, row_numbers/values are ignored.
        row_numbers (List[int]): The data row numbers.
        values (List[List[ Union[int, str, float]]]): The data values, one row at a time.
        columns: List[dict]: The column information, in the format returned by the API.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        rows: List[dict] = None,
        row_numbers: List[int] = None,
        values: List[List[Union[int, str, float]]] = None,
        columns: List[dict] = None,
    ):
        if rows:
            row_numbers = [r["rowNumber"] for r in rows]
            values = [r["values"] for r in rows]
        self.id = id
        self.external_id = external_id
        self.row_numbers = row_numbers or []
        self.values = values or []
        self.columns = columns

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def _repr_html_(self):
        return self.to_pandas()._repr_html_()

    def __len__(self) -> int:
        return len(self.row_numbers)

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and self.id == other.id
            and self.external_id == other.external_id
            and self.row_numbers == other.row_numbers
            and self.values == other.values
        )

    def __getitem__(self, item: int) -> List[Union[int, str, float]]:
        # slow, should be replaced by dict cache if it sees more than incidental use
        if isinstance(item, slice):
            raise TypeError("Slicing SequenceData not supported")
        return self.values[self.row_numbers.index(item)]

    def get_column(self, external_id: str) -> List[Union[int, str, float]]:
        """Get a column by external_id.

        Args:
            external_id (str): External id of the column.

        Returns:
            List[Union[int, str, float]]: A list of values for that column in the sequence
        """
        try:
            ix = self.column_external_ids.index(external_id)
        except ValueError:
            raise ValueError(
                "Column {} not found, Sequence column external ids are {}".format(external_id, self.column_external_ids)
            )
        return [r[ix] for r in self.values]

    def items(self) -> Generator[Tuple[int, List[Union[int, str, float]]], None, None]:
        """Returns an iterator over tuples of (row number, values)."""
        for row, values in zip(self.row_numbers, self.values):
            yield row, values

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the sequence data into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        dumped = {
            "id": self.id,
            "external_id": self.external_id,
            "columns": self.columns,
            "rows": [{"rowNumber": r, "values": v} for r, v in zip(self.row_numbers, self.values)],
        }
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for key, value in dumped.items()}
        return {key: value for key, value in dumped.items() if value is not None}

    def to_pandas(self, column_names: str = "columnExternalId") -> "pandas.DataFrame":
        """Convert the sequence data into a pandas DataFrame.

        Args:
            column_names (str):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId".

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils._auxiliary.local_import("pandas")

        options = ["externalId", "id", "columnExternalId", "id|columnExternalId", "externalId|columnExternalId"]
        if column_names not in options:
            raise ValueError('Invalid column_names value, should be one of "%s"' % '", "'.join(options))

        column_names = (
            column_names.replace("columnExternalId", "{columnExternalId}")
            .replace("externalId", "{externalId}")
            .replace("id", "{id}")
        )
        df_columns = [
            column_names.format(id=str(self.id), externalId=str(self.external_id), columnExternalId=eid)
            for eid in self.column_external_ids
        ]

        return pd.DataFrame(
            [[x if x is not None else math.nan for x in r] for r in self.values],
            index=self.row_numbers,
            columns=df_columns,
        )

    @property
    def column_external_ids(self) -> List[Optional[str]]:
        """Retrieves list of column external ids for the sequence, for use in e.g. data retrieve or insert methods.

        Returns:
            List of sequence column external ids.
        """
        return [c.get("externalId") for c in self.columns]

    @property
    def column_value_types(self) -> List[str]:
        """Retrieves list of column value types.

        Returns:
            List of column value types
        """
        return [c.get("valueType") for c in self.columns]


class SequenceDataList(CogniteResourceList):
    _RESOURCE = SequenceData
    _ASSERT_CLASSES = False

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def to_pandas(self, column_names: str = "externalId|columnExternalId") -> "pandas.DataFrame":
        """Convert the sequence data list into a pandas DataFrame. Each column will be a sequence.

        Args:
            column_names (str):  Which field to use as column header. Can use any combination of "externalId", "columnExternalId", "id" and other characters as a template.
            include_aggregate_name (bool): Include aggregate in the column name

        Returns:
            pandas.DataFrame: The sequence data list as a pandas DataFrame.
        """
        pd = utils._auxiliary.local_import("pandas")
        return pd.concat([seq_data.to_pandas(column_names=column_names) for seq_data in self.data], axis=1)

    def _repr_html_(self):
        return self.to_pandas()._repr_html_()
