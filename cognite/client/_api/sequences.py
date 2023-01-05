import copy
import math
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
from typing import Sequence as SequenceType
from typing import Tuple, Union, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    Sequence,
    SequenceAggregate,
    SequenceData,
    SequenceDataList,
    SequenceFilter,
    SequenceList,
    SequenceUpdate,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier, IdentifierSequence

if TYPE_CHECKING:
    import pandas


class SequencesAPI(APIClient):
    _RESOURCE_PATH = "/sequences"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.data = SequencesDataAPI(self, *args, **kwargs)

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        external_id_prefix: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: SequenceType[int] = None,
        asset_subtree_ids: SequenceType[int] = None,
        asset_subtree_external_ids: SequenceType[str] = None,
        data_set_ids: SequenceType[int] = None,
        data_set_external_ids: SequenceType[str] = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        limit: int = None,
    ) -> Union[Iterator[Sequence], Iterator[SequenceList]]:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int, optional): Number of sequences to return in each chunk. Defaults to yielding one event a time.
            name (str): Filter out sequences that do not have this *exact* name.
            external_id_prefix (str): Filter out sequences that do not have this string as the start of the externalId
            metadata (Dict[str, Any]): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
            asset_ids (SequenceType[int]): Filter out sequences that are not linked to any of these assets.
            asset_subtree_ids (SequenceType[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (SequenceType[str]): List of asset subtrees external ids to filter on.
            data_set_ids (SequenceType[int]): Return only events in the specified data sets with these ids.
            data_set_external_ids (SequenceType[str]): Return only events in the specified data sets with these external ids.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int, optional): Max number of sequences to return. Defaults to return all items.

        Yields:
            Union[Sequence, SequenceList]: yields Sequence one by one if chunk is not specified, else SequenceList objects.
        """
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

        filter = SequenceFilter(
            name=name,
            metadata=metadata,
            external_id_prefix=external_id_prefix,
            asset_ids=asset_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
        )

    def __iter__(self) -> Iterator[Sequence]:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            Sequence: yields Sequence one by one.
        """
        return cast(Iterator[Sequence], self())

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Sequence]:
        """`Retrieve a single sequence by id. <https://docs.cognite.com/api/v1/#operation/getSequenceById>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Sequence]: Requested sequences or None if it does not exist.

        Examples:

            Get sequences by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(id=1)

            Get sequences by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Optional[SequenceType[int]] = None,
        external_ids: Optional[SequenceType[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> SequenceList:
        """`Retrieve multiple sequences by id. <https://docs.cognite.com/api/v1/#operation/getSequenceById>`_

        Args:
            ids (SequenceType[int], optional): IDs
            external_ids (SequenceType[str], optional): External IDs
            ignore_unknown_ids (bool, optional): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            SequenceList: The requested sequences.

        Examples:

            Get sequences by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve_multiple(ids=[1, 2, 3])

            Get sequences by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def list(
        self,
        name: str = None,
        external_id_prefix: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: SequenceType[int] = None,
        asset_subtree_ids: SequenceType[int] = None,
        asset_subtree_external_ids: SequenceType[str] = None,
        data_set_ids: SequenceType[int] = None,
        data_set_external_ids: SequenceType[str] = None,
        created_time: (Union[Dict[str, Any], TimestampRange]) = None,
        last_updated_time: (Union[Dict[str, Any], TimestampRange]) = None,
        limit: Optional[int] = 25,
    ) -> SequenceList:
        """`Iterate over sequences <https://docs.cognite.com/api/v1/#operation/advancedListSequences>`_

        Fetches sequences as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            name (str): Filter out sequences that do not have this *exact* name.
            external_id_prefix (str): Filter out sequences that do not have this string as the start of the externalId
            metadata (Dict[str, Any]): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
            asset_ids (SequenceType[int]): Filter out sequences that are not linked to any of these assets.
            asset_subtree_ids (SequenceType[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (SequenceType[str]): List of asset subtrees external ids to filter on.
            data_set_ids (SequenceType[int]): Return only events in the specified data sets with these ids.
            data_set_external_ids (SequenceType[str]): Return only events in the specified data sets with these external ids.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int, optional): Max number of sequences to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            SequenceList: The requested sequences.

        Examples:

            List sequences::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.list(limit=5)

            Iterate over sequences::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for seq in c.sequences:
                ...     seq # do something with the sequences

            Iterate over chunks of sequences to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for seq_list in c.sequences(chunk_size=2500):
                ...     seq_list # do something with the sequences
        """
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

        filter = SequenceFilter(
            name=name,
            metadata=metadata,
            external_id_prefix=external_id_prefix,
            asset_ids=asset_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)
        return self._list(list_cls=SequenceList, resource_cls=Sequence, method="POST", filter=filter, limit=limit)

    def aggregate(self, filter: Union[SequenceFilter, Dict] = None) -> List[SequenceAggregate]:
        """`Aggregate sequences <https://docs.cognite.com/api/v1/#operation/aggregateSequences>`_

        Args:
            filter (Union[SequenceFilter, Dict]): Filter on sequence filter with exact match

        Returns:
            List[SequenceAggregate]: List of sequence aggregates

        Examples:

            Aggregate sequences::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.aggregate(filter={"external_id_prefix": "prefix"})
        """

        return self._aggregate(filter=filter, cls=SequenceAggregate)

    @overload
    def create(self, sequence: Sequence) -> Sequence:
        ...

    @overload
    def create(self, sequence: SequenceType[Sequence]) -> SequenceList:
        ...

    def create(self, sequence: Union[Sequence, SequenceType[Sequence]]) -> Union[Sequence, SequenceList]:
        """`Create one or more sequences. <https://docs.cognite.com/api/v1/#operation/createSequence>`_

        Args:
            sequence (Union[Sequence, SequenceType[Sequence]]): Sequence or list of Sequence to create.
                The Sequence columns parameter is a list of objects with fields
                `externalId` (external id of the column, when omitted, they will be given ids of 'column0, column1, ...'),
                `valueType` (data type of the column, either STRING, LONG, or DOUBLE, with default DOUBLE),
                `name`, `description`, `metadata` (optional fields to describe and store information about the data in the column).
                Other fields will be removed automatically, so a columns definition from a different sequence object can be passed here.

        Returns:
            Union[Sequence, SequenceList]: The created sequences.

        Examples:

            Create a new sequence::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Sequence
                >>> c = CogniteClient()
                >>> column_def = [{"valueType":"STRING","externalId":"user","description":"some description"}, {"valueType":"DOUBLE","externalId":"amount"}]
                >>> seq = c.sequences.create(Sequence(external_id="my_sequence", columns=column_def))

            Create a new sequence with the same column specifications as an existing sequence::

                >>> seq2 = c.sequences.create(Sequence(external_id="my_copied_sequence", columns=seq.columns))

        """
        utils._auxiliary.assert_type(sequence, "sequences", [SequenceType, Sequence])
        if isinstance(sequence, SequenceType):
            sequence = [self._clean_columns(seq) for seq in sequence]
        else:
            sequence = self._clean_columns(sequence)
        return self._create_multiple(list_cls=SequenceList, resource_cls=Sequence, items=sequence)

    def _clean_columns(self, sequence: Sequence) -> Sequence:
        sequence = copy.copy(sequence)
        sequence.columns = [
            {
                k: v
                for k, v in utils._auxiliary.convert_all_keys_to_camel_case(col).items()
                if k in ["externalId", "valueType", "metadata", "name", "description"]
            }
            for col in cast(List, sequence.columns)
        ]
        for i in range(len(sequence.columns)):
            if not sequence.columns[i].get("externalId"):
                sequence.columns[i]["externalId"] = "column" + str(i)
            if sequence.columns[i].get("valueType"):
                sequence.columns[i]["valueType"] = sequence.columns[i]["valueType"].upper()
        return sequence

    def delete(
        self, id: Union[int, SequenceType[int]] = None, external_id: Union[str, SequenceType[str]] = None
    ) -> None:
        """`Delete one or more sequences. <https://docs.cognite.com/api/v1/#operation/deleteSequences>`_

        Args:
            id (Union[int, SequenceType[int]): Id or list of ids
            external_id (Union[str, SequenceType[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete sequences by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.sequences.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    @overload
    def update(self, item: Union[Sequence, SequenceUpdate]) -> Sequence:
        ...

    @overload
    def update(self, item: SequenceType[Union[Sequence, SequenceUpdate]]) -> SequenceList:
        ...

    def update(
        self, item: Union[Sequence, SequenceUpdate, SequenceType[Union[Sequence, SequenceUpdate]]]
    ) -> Union[Sequence, SequenceList]:
        """`Update one or more sequences. <https://docs.cognite.com/api/v1/#operation/updateSequences>`_

        Args:
            item (Union[Sequence, SequenceUpdate, SequenceType[Union[Sequence, SequenceUpdate]]]): Sequences to update

        Returns:
            Union[Sequence, SequenceList]: Updated sequences.

        Examples:

            Update a sequence that you have fetched. This will perform a full update of the sequences::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = c.sequences.update(res)

            Perform a partial update on a sequence, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>> my_update = SequenceUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.sequences.update(my_update)

            **Updating column definitions**

            Currently, updating the column definitions of a sequence is only supported through partial update, using `add`, `remove` and `modify` methods on the `columns` property.

            Add a single new column::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.add({"valueType":"STRING","externalId":"user","description":"some description"})
                >>> res = c.sequences.update(my_update)

            Add multiple new columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>>
                >>> column_def = [{"valueType":"STRING","externalId":"user","description":"some description"}, {"valueType":"DOUBLE","externalId":"amount"}]
                >>> my_update = SequenceUpdate(id=1).columns.add(column_def)
                >>> res = c.sequences.update(my_update)

            Remove a single column::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove("col_external_id1")
                >>> res = c.sequences.update(my_update)

            Remove multiple columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove(["col_external_id1","col_external_id2"])
                >>> res = c.sequences.update(my_update)

            Update existing columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumnUpdate
                >>> c = CogniteClient()
                >>>
                >>> column_updates = [
                ...     SequenceColumnUpdate(external_id="col_external_id_1").external_id.set("new_col_external_id"),
                ...     SequenceColumnUpdate(external_id="col_external_id_2").description.set("my new description"),
                ... ]
                >>> my_update = SequenceUpdate(id=1).columns.modify(column_updates)
                >>> res = c.sequences.update(my_update)
        """
        return self._update_multiple(
            list_cls=SequenceList, resource_cls=Sequence, update_cls=SequenceUpdate, items=item
        )

    def search(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        filter: Union[SequenceFilter, Dict] = None,
        limit: int = 100,
    ) -> SequenceList:
        """`Search for sequences. <https://docs.cognite.com/api/v1/#operation/searchSequences>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str, optional): Prefix and fuzzy search on name.
            description (str, optional): Prefix and fuzzy search on description.
            query (str, optional): Search on name and description using wildcard search on each of the words (separated
                by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (Union[SequenceFilter, Dict], optional): Filter to apply. Performs exact match on these fields.
            limit (int, optional): Max number of results to return.

        Returns:
            SequenceList: List of requested sequences.

        Examples:

            Search for a sequence::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.search(name="some name")
        """
        return self._search(
            list_cls=SequenceList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )


class SequencesDataAPI(APIClient):
    _DATA_PATH = "/sequences/data"

    def __init__(self, sequences_api: SequencesAPI, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._sequences_api = sequences_api
        self._SEQ_POST_LIMIT_ROWS = 10000
        self._SEQ_POST_LIMIT_VALUES = 100000
        self._SEQ_RETRIEVE_LIMIT = 10000

    def insert(
        self,
        rows: Union[
            Dict[int, SequenceType[Union[int, float, str]]],
            SequenceType[Tuple[int, SequenceType[Union[int, float, str]]]],
            SequenceType[Dict[str, Any]],
            SequenceData,
        ],
        column_external_ids: Optional[SequenceType[str]],
        id: int = None,
        external_id: str = None,
    ) -> None:
        """`Insert rows into a sequence <https://docs.cognite.com/api/v1/#operation/postSequenceData>`_

        Args:
            column_external_ids (Optional[SequenceType[str]]): List of external id for the columns of the sequence.
            rows (Union[ Dict[int, SequenceType[Union[int, float, str]]], SequenceType[Tuple[int, SequenceType[Union[int, float, str]]]], SequenceType[Dict[str,Any]], SequenceData]):  The rows you wish to insert.
                Can either be a list of tuples, a list of {"rowNumber":... ,"values": ...} objects, a dictionary of rowNumber: data, or a SequenceData object. See examples below.
            id (int): Id of sequence to insert rows into.
            external_id (str): External id of sequence to insert rows into.

        Returns:
            None

        Examples:
            Your rows of data can be a list of tuples where the first element is the rownumber and the second element is the data to be inserted::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> seq = c.sequences.create(Sequence(columns=[{"valueType": "STRING", "externalId":"col_a"},{"valueType": "DOUBLE", "externalId":"col_b"}]))
                >>> data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
                >>> c.sequences.data.insert(column_external_ids=["col_a","col_b"], rows=data, id=1)

            They can also be provided as a list of API-style objects with a rowNumber and values field::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data = [{"rowNumber": 123, "values": ['str',3]}, {"rowNumber": 456, "values": ["bar",42]} ]
                >>> c.sequences.data.insert(data, id=1, column_external_ids=["col_a","col_b"]) # implicit columns are retrieved from metadata

            Or they can be a given as a dictionary with row number as the key, and the value is the data to be inserted at that row::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data = {123 : ['str',3], 456 : ['bar',42] }
                >>> c.sequences.data.insert(column_external_ids=['stringColumn','intColumn'], rows=data, id=1)

            Finally, they can be a SequenceData object retrieved from another request. In this case column_external_ids from this object are used as well.

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data = c.sequences.data.retrieve(id=2,start=0,end=10)
                >>> c.sequences.data.insert(rows=data, id=1,column_external_ids=None)
        """
        if isinstance(rows, SequenceData):
            column_external_ids = rows.column_external_ids
            rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]

        if isinstance(rows, dict):
            all_rows: Union[Dict, SequenceType] = [{"rowNumber": k, "values": v} for k, v in rows.items()]
        elif isinstance(rows, SequenceType) and len(rows) > 0 and isinstance(rows[0], dict):
            all_rows = rows
        elif isinstance(rows, SequenceType) and (len(rows) == 0 or isinstance(rows[0], tuple)):
            all_rows = [{"rowNumber": k, "values": v} for k, v in rows]
        else:
            raise ValueError("Invalid format for 'rows', expected a list of tuples, list of dict or dict")

        base_obj = Identifier.of_either(id, external_id).as_dict()
        base_obj.update(self._process_columns(column_external_ids))

        if len(all_rows) > 0:
            rows_per_request = min(
                self._SEQ_POST_LIMIT_ROWS, int(self._SEQ_POST_LIMIT_VALUES / len(all_rows[0]["values"]))
            )
        else:
            rows_per_request = self._SEQ_POST_LIMIT_ROWS

        row_objs = [{"rows": all_rows[i : i + rows_per_request]} for i in range(0, len(all_rows), rows_per_request)]
        tasks = [({**base_obj, **rows},) for rows in row_objs]  # type: ignore
        summary = utils._concurrency.execute_tasks_concurrently(
            self._insert_data, tasks, max_workers=self._config.max_workers
        )
        summary.raise_compound_exception_if_failed_tasks()

    def insert_dataframe(self, dataframe: "pandas.DataFrame", id: int = None, external_id: str = None) -> None:
        """`Insert a Pandas dataframe. <https://docs.cognite.com/api/v1/#operation/postSequenceData>`_

        The index of the dataframe must contain the row numbers. The names of the remaining columns specify the column external ids.
        The sequence and columns must already exist.

        Args:
            dataframe (pandas.DataFrame):  Pandas DataFrame object containing the sequence data.
            id (int): Id of sequence to insert rows into.
            external_id (str): External id of sequence to insert rows into.

        Returns:
            None

        Examples:
            Multiply data in the sequence by 2::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.sequences.data.retrieve_dataframe(id=123, start=0, end=None)
                >>> c.sequences.data.insert_dataframe(df*2, id=123)
        """
        dataframe = dataframe.replace({math.nan: None})
        data = [(v[0], list(v[1:])) for v in dataframe.itertuples()]
        column_external_ids = [str(s) for s in dataframe.columns]
        self.insert(rows=data, column_external_ids=column_external_ids, id=id, external_id=external_id)

    def _insert_data(self, task: Dict[str, Any]) -> None:
        self._post(url_path=self._DATA_PATH, json={"items": [task]})

    def delete(self, rows: SequenceType[int], id: int = None, external_id: str = None) -> None:
        """`Delete rows from a sequence <https://docs.cognite.com/api/v1/#operation/deleteSequenceData>`_

        Args:
            rows (SequenceType[int]): List of row numbers.
            id (int): Id of sequence to delete rows from.
            external_id (str): External id of sequence to delete rows from.

        Returns:
            None

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.sequences.data.delete(id=0, rows=[1,2,42])
        """
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj["rows"] = rows

        self._post(url_path=self._DATA_PATH + "/delete", json={"items": [post_obj]})

    def delete_range(self, start: int, end: Union[int, None], id: int = None, external_id: str = None) -> None:
        """`Delete a range of rows from a sequence. Note this operation is potentially slow, as retrieves each row before deleting. <https://docs.cognite.com/api/v1/#operation/deleteSequenceData>`_

        Args:
            start (int): Row number to start from (inclusive).
            end (Union[int, None]): Upper limit on the row number (exclusive).
                Set to None or -1 to delete all rows until end of sequence.
            id (int): Id of sequence to delete rows from.
            external_id (str): External id of sequence to delete rows from.

        Returns:
            None

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.sequences.data.delete_range(id=0, start=0, end=None)
        """
        sequence = self._sequences_api.retrieve(id=id, external_id=external_id)
        assert sequence is not None
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj.update(self._process_columns(column_external_ids=[sequence.column_external_ids[0]]))
        post_obj.update({"start": start, "end": end})
        for data, _ in self._fetch_data(post_obj):
            if data:
                self.delete(rows=[r["rowNumber"] for r in data], external_id=external_id, id=id)

    def retrieve(
        self,
        start: int,
        end: Union[int, None],
        column_external_ids: Optional[SequenceType[str]] = None,
        external_id: Union[str, SequenceType[str]] = None,
        id: Union[int, SequenceType[int]] = None,
        limit: int = None,
    ) -> Union[SequenceData, SequenceDataList]:
        """`Retrieve data from a sequence <https://docs.cognite.com/api/v1/#operation/getSequenceData>`_

        Args:
            start (int): Row number to start from (inclusive).
            end (Union[int, None]): Upper limit on the row number (exclusive). Set to None or -1 to get all rows
                until end of sequence.
            column_external_ids (Optional[SequenceType[str]]): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            id (int): Id of sequence.
            external_id (str): External id of sequence.
            limit (int): Maximum number of rows to return per sequence. 10000 is the maximum limit per request.


        Returns:
            List of sequence data

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.data.retrieve(id=0, start=0, end=None)
                >>> tuples = [(r,v) for r,v in res.items()] # You can use this iterator in for loops and list comprehensions,
                >>> single_value = res[23] # ... get the values at a single row number,
                >>> col = res.get_column(external_id='columnExtId') # ... get the array of values for a specific column,
                >>> df = res.to_pandas() # ... or convert the result to a dataframe
        """
        post_objs = IdentifierSequence.load(id, external_id).as_dicts()

        def _fetch_sequence(post_obj: Dict[str, Any]) -> SequenceData:
            post_obj.update(self._process_columns(column_external_ids=column_external_ids))
            post_obj.update({"start": start, "end": end, "limit": limit})
            seqdata: List = []
            columns: List = []
            for data, columns in self._fetch_data(post_obj):
                seqdata.extend(data)
            return SequenceData(
                id=post_obj.get("id"), external_id=post_obj.get("externalId"), rows=seqdata, columns=columns
            )

        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            _fetch_sequence, [(x,) for x in post_objs], max_workers=self._config.max_workers
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        results = tasks_summary.joined_results()
        if len(post_objs) == 1:
            return results[0]
        else:
            return SequenceDataList(results)

    def retrieve_latest(
        self,
        id: int = None,
        external_id: str = None,
        column_external_ids: Optional[SequenceType[str]] = None,
        before: int = None,
    ) -> SequenceData:
        """`Retrieves the last row (i.e the row with the highest row number) in a sequence. <https://docs.cognite.com/api/v1/#operation/getLatestSequenceRow>`_

        Args:
            id (optional, int): Id or list of ids.
            external_id (optional, str): External id or list of external ids.
            column_external_ids: (optional, SequenceType[str]): external ids of columns to include. Omitting wil return all columns.
            before: (optional, int): Get latest datapoint before this row number.

        Returns:
            SequenceData: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest row in a sequence before row number 1000::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.data.retrieve_latest(id=1, before=1000)
        """
        identifier = Identifier.of_either(id, external_id).as_dict()
        res = self._do_request(
            "POST", self._DATA_PATH + "/latest", json={**identifier, "before": before, "columns": column_external_ids}
        ).json()
        return SequenceData(id=res["id"], external_id=res.get("external_id"), rows=res["rows"], columns=res["columns"])

    def retrieve_dataframe(
        self,
        start: int,
        end: Union[int, None],
        column_external_ids: Optional[List[str]] = None,
        external_id: str = None,
        column_names: str = None,
        id: int = None,
        limit: int = None,
    ) -> "pandas.DataFrame":
        """`Retrieve data from a sequence as a pandas dataframe <https://docs.cognite.com/api/v1/#operation/getSequenceData>`_

        Args:
            start (int): (inclusive) row number to start from.
            end (Union[int, None]): (exclusive) upper limit on the row number. Set to None or -1 to get all rows
                until end of sequence.
            column_external_ids (Optional[SequenceType[str]]): List of external id for the columns of the sequence.  If 'None' is passed, all columns will be retrieved.
            id (int): Id of sequence
            external_id (str): External id of sequence.
            column_names (str):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId". Default is "externalId|columnExternalId" for queries on more than one sequence, and "columnExternalId" for queries on a single sequence.
            limit (int): Maximum number of rows to return per sequence.

        Returns:
             pandas.DataFrame

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.sequences.data.retrieve_dataframe(id=0, start=0, end=None)
        """
        if isinstance(external_id, List) or isinstance(id, List) or (id is not None and external_id is not None):
            column_names_default = "externalId|columnExternalId"
        else:
            column_names_default = "columnExternalId"
        return self.retrieve(start, end, column_external_ids, external_id, id, limit).to_pandas(
            column_names=column_names or column_names_default
        )

    def _fetch_data(self, task: Dict[str, Any]) -> Iterator[Tuple[List, List]]:
        remaining_limit = task.get("limit")
        columns: List[str] = []
        cursor = None
        if task["end"] == -1:
            task["end"] = None
        while True:
            task["limit"] = min(self._SEQ_RETRIEVE_LIMIT, remaining_limit or self._SEQ_RETRIEVE_LIMIT)
            task["cursor"] = cursor
            resp = self._post(url_path=self._DATA_PATH + "/list", json=task).json()
            data = resp["rows"]
            columns = columns or resp["columns"]
            yield data, columns
            cursor = resp.get("nextCursor")
            if remaining_limit:
                remaining_limit -= len(data)
            if not cursor or (remaining_limit is not None and remaining_limit <= 0):
                break

    def _process_columns(self, column_external_ids: Optional[SequenceType[str]]) -> Dict[str, SequenceType[str]]:
        if column_external_ids is None:
            return {}  # for defaults
        return {"columns": column_external_ids}
