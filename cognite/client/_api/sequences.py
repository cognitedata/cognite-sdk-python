import math
from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Sequence, SequenceFilter, SequenceList, SequenceUpdate
from cognite.client.utils._experimental_warning import experimental_api


@experimental_api(api_name="Sequences")
class SequencesAPI(APIClient):
    _RESOURCE_PATH = "/sequences"
    _LIST_CLASS = SequenceList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = SequencesDataAPI(*args, **kwargs)

    def __call__(
        self, chunk_size: int = None, limit: int = None
    ) -> Generator[Union[Sequence, SequenceList], None, None]:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int, optional): Number of sequences to return in each chunk. Defaults to yielding one event a time.
            limit (int, optional): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Yields:
            Union[Sequence, SequenceList]: yields Sequence one by one if chunk is not specified, else SequenceList objects.
        """
        return self._list_generator(method="GET", chunk_size=chunk_size, filter=None, limit=limit)

    def __iter__(self) -> Generator[Sequence, None, None]:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            Sequence: yields Sequence one by one.
        """
        return self.__call__()

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Sequence]:
        """Retrieve a single sequence by id.

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Sequence]: Requested sequences or None if it does not exist.

        Examples:

            Get sequences by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(id=1)

            Get sequences by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None
    ) -> SequenceList:
        """Retrieve multiple sequences by id.

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

        Returns:
            SequenceList: The requested sequences.

        Examples:

            Get sequences by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve_multiple(ids=[1, 2, 3])

            Get sequences by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(self, limit: int = 25) -> SequenceList:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            limit (int, optional): Max number of sequences to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            SequenceList: The requested sequences.

        Examples:

            List sequences::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.list(limit=5)

            Iterate over sequences::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for seq in c.sequences:
                ...     seq # do something with the sequences

            Iterate over chunks of sequences to reduce memory load::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for seq_list in c.sequences(chunk_size=2500):
                ...     seq_list # do something with the sequences
        """
        return self._list(method="GET", filter=None, limit=limit)

    def create(self, sequences: Union[Sequence, List[Sequence]]) -> Union[Sequence, SequenceList]:
        """Create one or more sequences.

        Args:
            sequences (Union[Sequence, List[Sequence]]): Sequence or list of Sequence to create.

        Returns:
            Union[Sequence, SequenceList]: The created sequences.

        Examples:

            Create a new sequences::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import Sequence
                >>> c = CogniteClient()
                >>> ts = c.sequences.create(Sequence(name="my ts"))
        """
        return self._create_multiple(items=sequences)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more sequences.

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete sequences by id or external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[Sequence, SequenceUpdate, List[Union[Sequence, SequenceUpdate]]]
    ) -> Union[Sequence, SequenceList]:
        """Update one or more sequences.

        Args:
            item (Union[Sequence, SequenceUpdate, List[Union[Sequence, SequenceUpdate]]]): Sequences to update

        Returns:
            Union[Sequence, SequenceList]: Updated sequences.

        Examples:

            Update a sequences that you have fetched. This will perform a full update of the sequences::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = c.sequences.update(res)

            Perform a partial update on a sequences, updating the description and adding a new field to metadata::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> c = CogniteClient()
                >>> my_update = SequenceUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.sequences.update(my_update)
        """
        return self._update_multiple(items=item)

    def search(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        filter: Union[SequenceFilter, Dict] = None,
        limit: int = None,
    ) -> SequenceList:
        """Search for sequences.

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

            Search for a sequences::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.sequences.search(name="some name")
        """
        return self._search(
            search={"name": name, "description": description, "query": query}, filter=filter, limit=limit
        )


class SequencesDataAPI(APIClient):
    _DATA_PATH = "/sequences/data"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._SEQ_POST_LIMIT = 10000

    def insert(
        self,
        columns: List[Union[int, str]],
        rows: Dict[int, List[Union[int, float, str]]],
        id: int = None,
        external_id: str = None,
    ) -> None:
        """Insert rows into a sequence

        Args:
            columns (List[Union[int, str]]): List of id or external id for the columns of the sequence
            rows (Dict[int, List[Union[int, float, str]]]): Dictionary of row number => data, where data is a list corresponding to the given columns
            id (int): Id of sequence to insert rows into.
            external_id (str): External id of sequence to insert rows into.

        Returns:
            None

        Examples:

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> # with datetime objects
                >>> data = {123 : ['str',3], 456 : ['bar',42] }
                >>> res1 = c.sequences.data.insert(columns=['stringColumn','intColumn'], rows=data, id=1)
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        base_obj = self._process_ids(id, external_id, wrap_ids=True)[0]
        base_obj.update(self._process_columns(columns))

        all_rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]
        row_objs = [
            {"rows": all_rows[i : i + self._SEQ_POST_LIMIT]} for i in range(0, len(all_rows), self._SEQ_POST_LIMIT)
        ]
        tasks = [({**base_obj, **rows},) for rows in row_objs]
        summary = utils._concurrency.execute_tasks_concurrently(
            self._insert_data, tasks, max_workers=self._config.max_workers
        )
        summary.raise_compound_exception_if_failed_tasks()

    def _insert_data(self, task):
        self._post(url_path=self._DATA_PATH, json={"items": [task]})

    def delete(self, rows: List[int], id: int = None, external_id: str = None) -> None:
        """Delete rows from a sequence

        Args:
            rows (List[int]): List of row numbers
            id (int): Id of sequence to delete rows from
            external_id (str): External id of sequence to delete rows from

        Returns:
            None

        Examples:

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res1 = c.sequences.data.delete(id=0, rows=[1,2,42])
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        post_obj = self._process_ids(id, external_id, wrap_ids=True)[0]
        post_obj["rows"] = rows

        self._post(url_path=self._DATA_PATH + "/delete", json={"items": [post_obj]})

    def retrieve(
        self, start: int, end: int, columns: List[Union[int, str]] = None, external_id: str = None, id: int = None
    ) -> List[dict]:
        """Retrieve data from a sequence

        Args:
            start (int): Row number to start from (inclusive)
            end (int): Upper limit on the row number (exclusive)
            columns (List[Union[int, str]]): List of id or external id for the columns of the sequence. Defaults to all if None is passed.
            id (int): Id of sequence
            external_id (str): External id of sequence

        Returns:
            List of sequence data
        """
        col, data = self._retrieve_with_columns(start, end, columns, external_id, id)
        return data

    def retrieve_dataframe(
        self, start: int, end: int, columns: List[Union[int, str]] = None, external_id: str = None, id: int = None
    ):
        """Retrieve data from a sequence as a pandas dataframe

        Args:
            start: (inclusive) row number to start from
            end: (exclusive) upper limit on the row number
            columns: list of id or external id for the columns of the sequence. Defaults to all if None is passed.
            id (int): Id of sequence
            external_id (str): External id of sequence

        Returns:
             pandas.DataFrame
        """
        pd = utils._auxiliary.local_import("pandas")
        col, data = self._retrieve_with_columns(start, end, columns, external_id, id)

        transposed_data = zip(
            *[[d or math.nan for d in row["values"]] for row in data]
        )  # make sure string columns don't have 'None'
        data_dict = {k: list(v) for k, v in zip(col, transposed_data)}
        return pd.DataFrame(index=[d["rowNumber"] for d in data], data=data_dict)

    def _retrieve_with_columns(
        self, start: int, end: int, columns: List[Union[int, str]] = None, external_id: str = None, id: int = None
    ) -> Tuple[List, List[dict]]:
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        post_obj = self._process_ids(id, external_id, wrap_ids=True)[0]
        post_obj.update(self._process_columns(columns))
        post_obj.update({"inclusiveFrom": start, "exclusiveTo": end})
        return self._fetch_data(post_obj)

    def _fetch_data(self, task):
        seqdata = []
        while True:
            items = self._post(url_path=self._DATA_PATH + "/list", json={"items": [task]}).json()["items"]
            data = items[0]["rows"]
            columns = [c["externalId"] or c["id"] for c in items[0]["columns"]]
            seqdata.extend(data)
            if len(data) < self._SEQ_POST_LIMIT:
                break
            task["inclusiveFrom"] = data[-1]["rowNumber"] + 1
        return columns, seqdata

    def _process_columns(self, columns):
        if columns is None:
            return {}
        return {"columns": [{"externalId": col} if isinstance(col, str) else {"id": col} for col in columns]}
