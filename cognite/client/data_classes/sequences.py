import math
from typing import *
from typing import List

from cognite.client.data_classes._base import *


# GenClass: GetSequenceDTO
class Sequence(CogniteResource):
    """Information about the sequence stored in the database

    Args:
        id (int): Unique cognite-provided identifier for the sequence
        name (str): Name of the sequence
        description (str): Description of the sequence
        asset_id (int): Optional asset this sequence is associated with
        external_id (str): Projectwide unique identifier for the sequence
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        columns (List[Dict[str, Any]]): List of column definitions
        created_time (int): Time when this asset was created in CDP in milliseconds since Jan 1, 1970.
        last_updated_time (int): The last time this asset was updated in CDP, in milliseconds since Jan 1, 1970.
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
        self._cognite_client = cognite_client

    # GenStop

    def rows(self, start: int, end: int) -> List[dict]:
        """Retrieves rows from this sequence.

        Returns:
            List of sequence data.
        """
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        return self._cognite_client.sequences.data.retrieve(**identifier, start=start, end=end)

    @property
    def column_ids(self):
        """Retrieves list of column ids for the sequence, for use in e.g. data retrieve or insert methods

        Returns:
            List of sequence column ids.
        """
        return [c.get("id") for c in self.columns]

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


# GenClass: SequenceFilter
class SequenceFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Filter out sequences that do not have this *exact* name.
        external_id_prefix (str): Filter out sequences that do not have this string as the start of the externalId
        metadata (Dict[str, Any]): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (List[int]): Filter out sequences that are not linked to any of these assets.
        created_time (Dict[str, Any]): Filter out sequences with createdTime outside this range.
        last_updated_time (Dict[str, Any]): Filter out sequences with lastUpdatedTime outside this range.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: SequencesUpdate
class SequenceUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def name(self):
        return _PrimitiveSequenceUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveSequenceUpdate(self, "description")

    @property
    def asset_id(self):
        return _PrimitiveSequenceUpdate(self, "assetId")

    @property
    def external_id(self):
        return _PrimitiveSequenceUpdate(self, "externalId")

    @property
    def metadata(self):
        return _ObjectSequenceUpdate(self, "metadata")


class _PrimitiveSequenceUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> SequenceUpdate:
        return self._set(value)


class _ObjectSequenceUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> SequenceUpdate:
        return self._set(value)

    def add(self, value: Dict) -> SequenceUpdate:
        return self._add(value)

    def remove(self, value: List) -> SequenceUpdate:
        return self._remove(value)


class _ListSequenceUpdate(CogniteListUpdate):
    def set(self, value: List) -> SequenceUpdate:
        return self._set(value)

    def add(self, value: List) -> SequenceUpdate:
        return self._add(value)

    def remove(self, value: List) -> SequenceUpdate:
        return self._remove(value)

    # GenStop


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

    def __repr__(self):
        return self.__str__()

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

    def get_column(self, external_id: str = None, id: int = None) -> List[Union[int, str, float]]:
        """Get a column by id or external_id.

        Args:
            external_id (str): External id of the column.
            id (int): Id of the column.

        Returns:
            List[Union[int, str, float]]: A list of values for that column in the sequence
        """
        try:
            if id:
                ix = self.column_ids.index(id)
            elif external_id:
                ix = self.column_external_ids.index(external_id)
            else:
                raise ValueError("Expecting either id or external_id for the column to get")
        except ValueError as e:
            raise ValueError(
                "Column {} not found, Sequence column external ids are {} and ids are {}".format(
                    id if id else external_id, self.column_external_ids, self.column_ids
                )
            )
        return [r[ix] for r in self.values]

    def iteritems(self) -> Generator[Tuple[int, List[Union[int, str, float]]], None, None]:
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

    def to_pandas(self, column_names="externalIdIfExists") -> "pandas.DataFrame":
        """Convert the sequence data into a pandas DataFrame.

        Args:
            column_names (str): Which field to use as column header. Either "externalId", "id" or "externalIdIfExists" for externalId if it exists for all columns and id otherwise.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils._auxiliary.local_import("pandas")
        if column_names == "externalIdIfExists":
            column_names = "externalId" if None not in self.column_external_ids else "id"

        if column_names == "externalId":
            identifiers = self.column_external_ids
        elif column_names == "id":
            identifiers = self.column_ids
        else:
            raise ValueError("column_names must be 'externalIdIfExists', 'externalId' or 'id'")
        return pd.DataFrame(
            [[x or math.nan for x in r] for r in self.values], index=self.row_numbers, columns=identifiers
        )

    @property
    def column_ids(self) -> List[int]:
        """Retrieves list of column ids for the sequence, for use in e.g. data retrieve or insert methods.

        Returns:
            List of sequence column ids.
        """
        return [c.get("id") for c in self.columns]

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
