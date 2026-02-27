from __future__ import annotations

import math
import typing
import warnings
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    SequenceRows,
    SequenceRowsList,
)
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class SequencesDataAPI(APIClient):
    _RESOURCE_PATH = "/sequences/data"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._SEQ_POST_LIMIT_ROWS = 10_000
        self._SEQ_POST_LIMIT_VALUES = 100_000
        self._SEQ_RETRIEVE_LIMIT = 10_000

    async def insert(
        self,
        rows: SequenceRows
        | dict[int, typing.Sequence[int | float | str]]
        | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]]
        | typing.Sequence[dict[str, Any]],
        columns: SequenceNotStr[str] | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        """`Insert rows into a sequence <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        Args:
            rows (SequenceRows | dict[int, typing.Sequence[int | float | str]] | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]] | typing.Sequence[dict[str, Any]]):  The rows you wish to insert. Can either be a list of tuples, a list of {"rowNumber":... ,"values": ...} objects, a dictionary of rowNumber: data, or a SequenceData object. See examples below.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.

        Examples:
            Your rows of data can be a list of tuples where the first element is the rownumber and the second element is the data to be inserted:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceWrite, SequenceColumnWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> seq = client.sequences.create(
                ...     SequenceWrite(
                ...         columns=[
                ...             SequenceColumnWrite(value_type="STRING", external_id="col_a"),
                ...             SequenceColumnWrite(value_type="DOUBLE", external_id ="col_b")
                ...         ],
                ...     )
                ... )
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
        match rows:
            case SequenceRows():
                columns = rows.column_external_ids
                all_rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]
            case []:
                all_rows = []
            case dict():
                all_rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]
            case [dict(), *_]:  # Assume homogeneous
                all_rows = list(rows)  # type: ignore [arg-type]
            case [(_, _), *_]:  # more assume homogeneous
                all_rows = [{"rowNumber": k, "values": v} for k, v in rows]
            case _:
                raise TypeError("Invalid format for 'rows', expected a list of tuples, list of dict or dict")

        base_obj = Identifier.of_either(id, external_id).as_dict()
        base_obj.update(self._wrap_columns(columns))

        if len(all_rows) == 0:
            rows_per_request = self._SEQ_POST_LIMIT_ROWS
        else:
            try:
                length = len(all_rows[0].get("values"))  # type: ignore [arg-type]
            except (KeyError, TypeError):
                raise ValueError("Could not determine number of columns from first row, is the format correct?")

            rows_per_request = min(self._SEQ_POST_LIMIT_ROWS, int(self._SEQ_POST_LIMIT_VALUES / length))

        tasks = [
            AsyncSDKTask(self._insert_data, task=base_obj | {"rows": row_chunk})
            for row_chunk in split_into_chunks(all_rows, rows_per_request)
        ]
        summary = await execute_async_tasks(tasks)
        summary.raise_compound_exception_if_failed_tasks()

    async def insert_dataframe(
        self, dataframe: pd.DataFrame, id: int | None = None, external_id: str | None = None, dropna: bool = True
    ) -> None:
        """`Insert a Pandas dataframe. <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        The index of the dataframe must contain the row numbers. The names of the remaining columns specify the column external ids.
        The sequence and columns must already exist.

        Args:
            dataframe (pd.DataFrame):  Pandas DataFrame object containing the sequence data.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.
            dropna (bool): Whether to drop rows where all values are missing. Default: True.

        Examples:
            Insert three rows into columns 'col_a' and 'col_b' of the sequence with id=123:

                >>> from cognite.client import CogniteClient
                >>> import pandas as pd
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = pd.DataFrame({'col_a': [1, 2, 3], 'col_b': [4, 5, 6]}, index=[1, 2, 3])
                >>> client.sequences.data.insert_dataframe(df, id=123)
        """
        if dropna:
            # These will be rejected by the API, hence we remove them by default:
            dataframe = dataframe.dropna(how="all")
        dataframe = dataframe.replace({math.nan: None})  # TODO: Optimization required (memory usage)
        data = [(row.Index, row[1:]) for row in dataframe.itertuples()]
        columns = list(dataframe.columns.astype(str))
        await self.insert(rows=data, columns=columns, id=id, external_id=external_id)

    async def _insert_data(self, task: dict[str, Any]) -> None:
        await self._post(url_path=self._RESOURCE_PATH, json={"items": [task]}, semaphore=self._get_semaphore("write"))

    async def delete(self, rows: typing.Sequence[int], id: int | None = None, external_id: str | None = None) -> None:
        """`Delete rows from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            rows (typing.Sequence[int]): List of row numbers.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

            Delete rows from a sequence:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.sequences.data.delete(id=1, rows=[1,2,42])
        """
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj["rows"] = rows

        await self._post(
            url_path=self._RESOURCE_PATH + "/delete",
            json={"items": [post_obj]},
            semaphore=self._get_semaphore("delete"),
        )

    async def delete_range(
        self, start: int, end: int | None, id: int | None = None, external_id: str | None = None
    ) -> None:
        """`Delete a range of rows from a sequence. Note this operation is potentially slow, as retrieves each row before deleting. <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to delete all rows until end of sequence.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.sequences.data.delete_range(id=1, start=0, end=None)
        """
        sequence = await self._cognite_client.sequences.retrieve(external_id=external_id, id=id)
        assert sequence is not None
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj.update(self._wrap_columns(column_external_ids=sequence.column_external_ids))
        post_obj.update({"start": start, "end": end})
        async for resp in self._fetch_data(post_obj):
            if rows := resp["rows"]:
                await self.delete(rows=[r["rowNumber"] for r in rows], external_id=external_id, id=id)

    @overload
    async def retrieve(
        self,
        external_id: None = None,
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> NoReturn: ...

    @overload
    async def retrieve(
        self,
        external_id: str,
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    async def retrieve(
        self,
        external_id: SequenceNotStr[str],
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    @overload
    async def retrieve(
        self,
        external_id: None = None,
        *,
        id: int,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    async def retrieve(
        self,
        external_id: None = None,
        *,
        id: typing.Sequence[int],
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    @overload
    async def retrieve(
        self,
        external_id: SequenceNotStr[str] | str,
        id: typing.Sequence[int] | int,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    async def retrieve(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        id: int | typing.Sequence[int] | None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows | SequenceRowsList:
        """`Retrieve data from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            external_id (str | SequenceNotStr[str] | None): The external id of the sequence to retrieve from.
            id (int | typing.Sequence[int] | None): The internal if the sequence to retrieve from.
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to get all rows until end of sequence.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            limit (int | None): Maximum number of rows to return per sequence. Pass None to fetch all (possibly limited by 'end').

        Returns:
            SequenceRows | SequenceRowsList: SequenceRows if a single identifier was given, else SequenceRowsList

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.data.retrieve(id=1)
                >>> tuples = [(r,v) for r,v in res.items()] # You can use this iterator in for loops and list comprehensions,
                >>> single_value = res[23] # ... get the values at a single row number,
                >>> col = res.get_column(external_id='columnExtId') # ... get the array of values for a specific column,
                >>> df = res.to_pandas() # ... or convert the result to a dataframe
        """
        ident_sequence = IdentifierSequence.load(id, external_id)

        async def _fetch_sequence(post_obj: dict[str, Any]) -> SequenceRows:
            post_obj.update(self._wrap_columns(column_external_ids=columns))
            post_obj.update({"start": start, "end": end, "limit": limit})

            row_response_iterator = self._fetch_data(post_obj)
            # Get the External ID and ID from the first response
            sequence_rows = await anext(row_response_iterator)
            async for row_response in row_response_iterator:
                sequence_rows["rows"].extend(row_response["rows"])

            return SequenceRows._load(sequence_rows)

        tasks = [AsyncSDKTask(_fetch_sequence, id_) for id_ in ident_sequence.as_dicts()]
        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_list_element_unwrap_fn=ident_sequence.extract_identifiers
        )
        results = tasks_summary.joined_results()
        if ident_sequence.is_singleton():
            return results[0]
        else:
            return SequenceRowsList(results)

    async def retrieve_last_row(
        self,
        id: int | None = None,
        external_id: str | None = None,
        columns: SequenceNotStr[str] | None = None,
        before: int | None = None,
    ) -> SequenceRows:
        """`Retrieves the last row (i.e the row with the highest row number) in a sequence. <https://developer.cognite.com/api#tag/Sequences/operation/getLatestSequenceRow>`_

        Args:
            id (int | None): Id or list of ids.
            external_id (str | None): External id or list of external ids.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            before (int | None): (optional, int): Get latest datapoint before this row number.

        Returns:
            SequenceRows: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest row in a sequence before row number 1000:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.data.retrieve_last_row(id=1, before=1000)
        """
        identifier = Identifier.of_either(id, external_id).as_dict()
        res = await self._post(
            self._RESOURCE_PATH + "/latest",
            json={**identifier, "before": before, "columns": columns},
            semaphore=self._get_semaphore("read"),
        )
        return SequenceRows._load(res.json())

    async def retrieve_dataframe(
        self,
        start: int,
        end: int | None,
        columns: list[str] | None = None,
        external_id: str | None = None,
        column_names: str | None = None,
        id: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """`Retrieve data from a sequence as a pandas dataframe <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            start (int): (inclusive) row number to start from.
            end (int | None): (exclusive) upper limit on the row number. Set to None or -1 to get all rows until end of sequence.
            columns (list[str] | None): List of external id for the columns of the sequence.  If 'None' is passed, all columns will be retrieved.
            external_id (str | None): External id of sequence.
            column_names (str | None):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId". Default is "externalId|columnExternalId" for queries on more than one sequence, and "columnExternalId" for queries on a single sequence.
            id (int | None): Id of sequence
            limit (int | None): Maximum number of rows to return per sequence.

        Returns:
            pd.DataFrame: The requested sequence data in a pandas DataFrame

        Examples:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = client.sequences.data.retrieve_dataframe(id=1, start=0, end=None)
        """
        warnings.warn("This method is deprecated. Use retrieve(...).to_pandas(..) instead.", DeprecationWarning)
        # TODO(doctrino): type annotation does not mention list?
        if isinstance(external_id, list) or isinstance(id, list) or (id is not None and external_id is not None):
            column_names_default = "externalId|columnExternalId"
        else:
            column_names_default = "columnExternalId"

        if external_id is not None and id is None:
            response = await self.retrieve(external_id=external_id, start=start, end=end, limit=limit, columns=columns)
            return response.to_pandas(column_names=column_names or column_names_default)  # type: ignore  [arg-type]

        elif id is not None and external_id is None:
            response = await self.retrieve(id=id, start=start, end=end, limit=limit, columns=columns)
            return response.to_pandas(column_names=column_names or column_names_default)  # type: ignore  [arg-type]
        else:
            raise ValueError("Either external_id or id must be specified")

    async def _fetch_data(self, task: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        remaining_limit = task.get("limit")
        cursor = None
        if task["end"] == -1:
            task["end"] = None
        semaphore = self._get_semaphore("read")
        while True:
            task["limit"] = min(self._SEQ_RETRIEVE_LIMIT, remaining_limit or self._SEQ_RETRIEVE_LIMIT)
            task["cursor"] = cursor
            resp = (await self._post(url_path=self._RESOURCE_PATH + "/list", json=task, semaphore=semaphore)).json()
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
