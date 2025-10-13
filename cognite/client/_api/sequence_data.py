from __future__ import annotations

import math
import typing
import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    SequenceRows,
    SequenceRowsList,
)
from cognite.client.utils._auxiliary import handle_renamed_argument
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SequencesDataAPI(APIClient):
    _DATA_PATH = "/sequences/data"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._SEQ_POST_LIMIT_ROWS = 10_000
        self._SEQ_POST_LIMIT_VALUES = 100_000
        self._SEQ_RETRIEVE_LIMIT = 10_000

    def insert(
        self,
        rows: SequenceRows
        | dict[int, typing.Sequence[int | float | str]]
        | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]]
        | typing.Sequence[dict[str, Any]],
        columns: SequenceNotStr[str] | None = None,
        id: int | None = None,
        external_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        """`Insert rows into a sequence <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        Args:
            rows (SequenceRows | dict[int, typing.Sequence[int | float | str]] | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]] | typing.Sequence[dict[str, Any]]):  The rows you wish to insert. Can either be a list of tuples, a list of {"rowNumber":... ,"values": ...} objects, a dictionary of rowNumber: data, or a SequenceData object. See examples below.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Examples:
            Your rows of data can be a list of tuples where the first element is the rownumber and the second element is the data to be inserted:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Sequence, SequenceColumn
                >>> client = CogniteClient()
                >>> seq = client.sequences.create(Sequence(columns=[SequenceColumn(value_type="String", external_id="col_a"),
                ...     SequenceColumn(value_type="Double", external_id ="col_b")]))
                >>> data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
                >>> client.sequences.data.insert(columns=["col_a","col_b"], rows=data, id=1)

            They can also be provided as a list of API-style objects with a rowNumber and values field:

                >>> data = [{"rowNumber": 123, "values": ['str',3]}, {"rowNumber": 456, "values": ["bar",42]} ]
                >>> client.sequences.data.insert(data, id=1, columns=["col_a","col_b"]) # implicit columns are retrieved from metadata

            Or they can be a given as a dictionary with row number as the key, and the value is the data to be inserted at that row:

                >>> data = {123 : ['str',3], 456 : ['bar',42] }
                >>> client.sequences.data.insert(columns=['stringColumn','intColumn'], rows=data, id=1)

            Finally, they can be a SequenceData object retrieved from another request. In this case columns from this object are used as well.

                >>> data = client.sequences.data.retrieve(id=2,start=0,end=10)
                >>> client.sequences.data.insert(rows=data, id=1,columns=None)
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)
        if isinstance(rows, SequenceRows):
            columns = rows.column_external_ids
            rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]

        if isinstance(rows, dict):
            all_rows: dict | typing.Sequence = [{"rowNumber": k, "values": v} for k, v in rows.items()]
        elif isinstance(rows, typing.Sequence) and len(rows) > 0 and isinstance(rows[0], dict):
            all_rows = rows
        elif isinstance(rows, typing.Sequence) and (len(rows) == 0 or isinstance(rows[0], tuple)):
            all_rows = [{"rowNumber": k, "values": v} for k, v in rows]
        else:
            raise TypeError("Invalid format for 'rows', expected a list of tuples, list of dict or dict")

        base_obj = Identifier.of_either(id, external_id).as_dict()
        base_obj.update(self._wrap_columns(columns))

        if len(all_rows) > 0:
            rows_per_request = min(
                self._SEQ_POST_LIMIT_ROWS, int(self._SEQ_POST_LIMIT_VALUES / len(all_rows[0]["values"]))
            )
        else:
            rows_per_request = self._SEQ_POST_LIMIT_ROWS

        row_objs = [{"rows": all_rows[i : i + rows_per_request]} for i in range(0, len(all_rows), rows_per_request)]
        tasks = [({**base_obj, **rows},) for rows in row_objs]
        summary = execute_tasks(self._insert_data, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks()

    def insert_dataframe(
        self, dataframe: pandas.DataFrame, id: int | None = None, external_id: str | None = None, dropna: bool = True
    ) -> None:
        """`Insert a Pandas dataframe. <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        The index of the dataframe must contain the row numbers. The names of the remaining columns specify the column external ids.
        The sequence and columns must already exist.

        Args:
            dataframe (pandas.DataFrame):  Pandas DataFrame object containing the sequence data.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.
            dropna (bool): Whether to drop rows where all values are missing. Default: True.

        Examples:
            Insert three rows into columns 'col_a' and 'col_b' of the sequence with id=123:

                >>> from cognite.client import CogniteClient
                >>> import pandas as pd
                >>> client = CogniteClient()
                >>> df = pd.DataFrame({'col_a': [1, 2, 3], 'col_b': [4, 5, 6]}, index=[1, 2, 3])
                >>> client.sequences.data.insert_dataframe(df, id=123)
        """
        if dropna:
            # These will be rejected by the API, hence we remove them by default:
            dataframe = dataframe.dropna(how="all")
        dataframe = dataframe.replace({math.nan: None})  # TODO: Optimization required (memory usage)
        data = [(row.Index, row[1:]) for row in dataframe.itertuples()]
        columns = list(dataframe.columns.astype(str))
        self.insert(rows=data, columns=columns, id=id, external_id=external_id)

    def _insert_data(self, task: dict[str, Any]) -> None:
        self._post(url_path=self._DATA_PATH, json={"items": [task]})

    def delete(self, rows: typing.Sequence[int], id: int | None = None, external_id: str | None = None) -> None:
        """`Delete rows from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            rows (typing.Sequence[int]): List of row numbers.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.sequences.data.delete(id=1, rows=[1,2,42])
        """
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj["rows"] = rows

        self._post(url_path=self._DATA_PATH + "/delete", json={"items": [post_obj]})

    def delete_range(self, start: int, end: int | None, id: int | None = None, external_id: str | None = None) -> None:
        """`Delete a range of rows from a sequence. Note this operation is potentially slow, as retrieves each row before deleting. <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to delete all rows until end of sequence.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.sequences.data.delete_range(id=1, start=0, end=None)
        """
        sequence = self._cognite_client.sequences.retrieve(external_id=external_id, id=id)
        assert sequence is not None
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj.update(self._wrap_columns(column_external_ids=sequence.column_external_ids))
        post_obj.update({"start": start, "end": end})
        for resp in self._fetch_data(post_obj):
            if rows := resp["rows"]:
                self.delete(rows=[r["rowNumber"] for r in rows], external_id=external_id, id=id)

    @overload
    def retrieve(
        self,
        *,
        external_id: str,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: SequenceNotStr[str],
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    @overload
    def retrieve(
        self,
        *,
        id: int,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    def retrieve(
        self,
        *,
        id: typing.Sequence[int],
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    def retrieve(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        id: int | typing.Sequence[int] | None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> SequenceRows | SequenceRowsList:
        """`Retrieve data from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            external_id (str | SequenceNotStr[str] | None): The external id of the sequence to retrieve from.
            id (int | typing.Sequence[int] | None): The internal if the sequence to retrieve from.
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to get all rows until end of sequence.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            limit (int | None): Maximum number of rows to return per sequence. Pass None to fetch all (possibly limited by 'end').
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Returns:
            SequenceRows | SequenceRowsList: SequenceRows if a single identifier was given, else SequenceDataList

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.data.retrieve(id=1)
                >>> tuples = [(r,v) for r,v in res.items()] # You can use this iterator in for loops and list comprehensions,
                >>> single_value = res[23] # ... get the values at a single row number,
                >>> col = res.get_column(external_id='columnExtId') # ... get the array of values for a specific column,
                >>> df = res.to_pandas() # ... or convert the result to a dataframe
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)

        ident_sequence = IdentifierSequence.load(id, external_id)
        identifiers = ident_sequence.as_dicts()

        def _fetch_sequence(post_obj: dict[str, Any]) -> SequenceRows:
            post_obj.update(self._wrap_columns(column_external_ids=columns))
            post_obj.update({"start": start, "end": end, "limit": limit})

            row_response_iterator = self._fetch_data(post_obj)
            # Get the External ID and ID from the first response
            sequence_rows = next(row_response_iterator)
            for row_response in row_response_iterator:
                sequence_rows["rows"].extend(row_response["rows"])

            return SequenceRows._load(sequence_rows)

        tasks_summary = execute_tasks(_fetch_sequence, list(zip(identifiers)), max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_list_element_unwrap_fn=ident_sequence.extract_identifiers
        )
        results = tasks_summary.joined_results()
        if ident_sequence.is_singleton():
            return results[0]
        else:
            return SequenceRowsList(results)

    def retrieve_last_row(
        self,
        id: int | None = None,
        external_id: str | None = None,
        columns: SequenceNotStr[str] | None = None,
        before: int | None = None,
        **kwargs: Any,
    ) -> SequenceRows:
        """`Retrieves the last row (i.e the row with the highest row number) in a sequence. <https://developer.cognite.com/api#tag/Sequences/operation/getLatestSequenceRow>`_

        Args:
            id (int | None): Id or list of ids.
            external_id (str | None): External id or list of external ids.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            before (int | None): (optional, int): Get latest datapoint before this row number.
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Returns:
            SequenceRows: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest row in a sequence before row number 1000:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.data.retrieve_last_row(id=1, before=1000)
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)
        identifier = Identifier.of_either(id, external_id).as_dict()
        res = self._do_request(
            "POST", self._DATA_PATH + "/latest", json={**identifier, "before": before, "columns": columns}
        ).json()
        return SequenceRows._load(res)

    def retrieve_dataframe(
        self,
        start: int,
        end: int | None,
        column_external_ids: list[str] | None = None,
        external_id: str | None = None,
        column_names: str | None = None,
        id: int | None = None,
        limit: int | None = None,
    ) -> pandas.DataFrame:
        """`Retrieve data from a sequence as a pandas dataframe <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            start (int): (inclusive) row number to start from.
            end (int | None): (exclusive) upper limit on the row number. Set to None or -1 to get all rows until end of sequence.
            column_external_ids (list[str] | None): List of external id for the columns of the sequence.  If 'None' is passed, all columns will be retrieved.
            external_id (str | None): External id of sequence.
            column_names (str | None):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId". Default is "externalId|columnExternalId" for queries on more than one sequence, and "columnExternalId" for queries on a single sequence.
            id (int | None): Id of sequence
            limit (int | None): Maximum number of rows to return per sequence.

        Returns:
            pandas.DataFrame: The requested sequence data in a pandas DataFrame

        Examples:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = client.sequences.data.retrieve_dataframe(id=1, start=0, end=None)
        """
        warnings.warn("This method is deprecated. Use retrieve(...).to_pandas(..) instead.", DeprecationWarning)
        if isinstance(external_id, list) or isinstance(id, list) or (id is not None and external_id is not None):
            column_names_default = "externalId|columnExternalId"
        else:
            column_names_default = "columnExternalId"

        if external_id is not None and id is None:
            return self.retrieve(
                external_id=external_id, start=start, end=end, limit=limit, columns=column_external_ids
            ).to_pandas(
                column_names=column_names or column_names_default,  # type: ignore  [arg-type]
            )
        elif id is not None and external_id is None:
            return self.retrieve(id=id, start=start, end=end, limit=limit, columns=column_external_ids).to_pandas(
                column_names=column_names or column_names_default,  # type: ignore  [arg-type]
            )
        else:
            raise ValueError("Either external_id or id must be specified")

    def _fetch_data(self, task: dict[str, Any]) -> Iterator[dict[str, Any]]:
        remaining_limit = task.get("limit")
        cursor = None
        if task["end"] == -1:
            task["end"] = None
        while True:
            task["limit"] = min(self._SEQ_RETRIEVE_LIMIT, remaining_limit or self._SEQ_RETRIEVE_LIMIT)
            task["cursor"] = cursor
            resp = self._post(url_path=self._DATA_PATH + "/list", json=task).json()
            yield resp
            cursor = resp.get("nextCursor")
            if remaining_limit:
                remaining_limit -= len(resp["rows"])
            if not cursor or (remaining_limit is not None and remaining_limit <= 0):
                break

    def _wrap_columns(self, column_external_ids: SequenceNotStr[str] | None) -> dict[str, SequenceNotStr[str]]:
        if column_external_ids is None:
            return {}  # for defaults
        return {"columns": column_external_ids}
