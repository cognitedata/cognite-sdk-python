"""
===============================================================================
05468959ae43bae014e192eed2dff0fc
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import typing
from typing import TYPE_CHECKING, Any, NoReturn, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import SequenceRows, SequenceRowsList
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas as pd


class SyncSequencesDataAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def insert(
        self,
        rows: SequenceRows
        | dict[int, typing.Sequence[int | float | str]]
        | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]]
        | typing.Sequence[dict[str, Any]],
        columns: SequenceNotStr[str] | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        """
        `Insert rows into a sequence <https://api-docs.cognite.com/20230101/tag/Sequences/operation/postSequenceData>`_

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
        return run_sync(
            self.__async_client.sequences.data.insert(rows=rows, columns=columns, id=id, external_id=external_id)
        )

    def insert_dataframe(
        self, dataframe: pd.DataFrame, id: int | None = None, external_id: str | None = None, dropna: bool = True
    ) -> None:
        """
        `Insert a Pandas dataframe. <https://api-docs.cognite.com/20230101/tag/Sequences/operation/postSequenceData>`_

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
        return run_sync(
            self.__async_client.sequences.data.insert_dataframe(
                dataframe=dataframe, id=id, external_id=external_id, dropna=dropna
            )
        )

    def delete(self, rows: typing.Sequence[int], id: int | None = None, external_id: str | None = None) -> None:
        """
        `Delete rows from a sequence <https://api-docs.cognite.com/20230101/tag/Sequences/operation/deleteSequenceData>`_

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
        return run_sync(self.__async_client.sequences.data.delete(rows=rows, id=id, external_id=external_id))

    def delete_range(self, start: int, end: int | None, id: int | None = None, external_id: str | None = None) -> None:
        """
        `Delete a range of rows from a sequence. Note this operation is potentially slow, as retrieves each row before deleting. <https://api-docs.cognite.com/20230101/tag/Sequences/operation/deleteSequenceData>`_

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
        return run_sync(
            self.__async_client.sequences.data.delete_range(start=start, end=end, id=id, external_id=external_id)
        )

    @overload
    def retrieve(
        self,
        external_id: None = None,
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> NoReturn: ...

    @overload
    def retrieve(
        self,
        external_id: str,
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str],
        id: None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    @overload
    def retrieve(
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
    def retrieve(
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
    def retrieve(
        self,
        external_id: SequenceNotStr[str] | str,
        id: typing.Sequence[int] | int,
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
    ) -> SequenceRows | SequenceRowsList:
        """
        `Retrieve data from a sequence <https://api-docs.cognite.com/20230101/tag/Sequences/operation/getSequenceData>`_

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
        return run_sync(
            self.__async_client.sequences.data.retrieve(
                external_id=external_id, id=id, start=start, end=end, columns=columns, limit=limit
            )
        )

    def retrieve_last_row(
        self,
        id: int | None = None,
        external_id: str | None = None,
        columns: SequenceNotStr[str] | None = None,
        before: int | None = None,
    ) -> SequenceRows:
        """
        `Retrieves the last row (i.e the row with the highest row number) in a sequence. <https://api-docs.cognite.com/20230101/tag/Sequences/operation/getLatestSequenceRow>`_

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
        return run_sync(
            self.__async_client.sequences.data.retrieve_last_row(
                id=id, external_id=external_id, columns=columns, before=before
            )
        )

    def retrieve_dataframe(
        self,
        start: int,
        end: int | None,
        columns: list[str] | None = None,
        external_id: str | None = None,
        column_names: str | None = None,
        id: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """
        `Retrieve data from a sequence as a pandas dataframe <https://api-docs.cognite.com/20230101/tag/Sequences/operation/getSequenceData>`_

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
        return run_sync(
            self.__async_client.sequences.data.retrieve_dataframe(
                start=start,
                end=end,
                columns=columns,
                external_id=external_id,
                column_names=column_names,
                id=id,
                limit=limit,
            )
        )
