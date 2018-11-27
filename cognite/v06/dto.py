# -*- coding: utf-8 -*-
"""Data Objects

This module contains data objects used to represent the data returned from the API.
"""
import pandas as pd
from typing import List


class Column:
    """Data transfer object for a column.

    Args:
        id (int):           ID of the column.
        name (str):         Name of the column.
        external_id (str):  External ID of the column.
        value_type (str):   Data type of the column.
        metadata (dict):    Custom, application specific metadata. String key -> String Value.
    """

    id: int
    name: str
    externalId: str
    valueType: str
    metadata: dict

    def __init__(
            self,
            id: int,
            name: str,
            external_id: str,
            value_type: str,
            metadata: dict
    ):
        self.id = id
        self.name = name
        self.externalId = external_id
        self.valueType = value_type
        self.metadata = metadata

    @staticmethod
    def from_JSON(the_column: dict):
        return Column(
            id=the_column['id'],
            name=the_column['name'],
            external_id=the_column.get('externalId', None),
            value_type=the_column['valueType'],
            metadata=the_column['metadata']
        )


class Sequence:
    """Data transfer object for a sequence.

    Args:
        id (int):           ID of the sequence.
        name (str):         Name of the sequence.
        external_id (str):  External ID of the sequence.
        asset_id (int):     ID of the asset the sequence is connected to, if any.
        columns (list):     List of columns in the sequence.
        description (str):  Description of the sequence.
        metadata (dict):    Custom, application specific metadata. String key -> String Value.
    """

    id: int
    name: str
    externalId: str
    assetId: int
    columns: List[Column]
    description: str
    metadata: dict

    def __init__(
            self,
            id: int,
            name: str,
            external_id: str,
            asset_id: int,
            columns: List[Column],
            description: str,
            metadata: dict
    ):
        self.id = id
        self.name = name
        self.externalId = external_id
        self.assetId = asset_id
        self.columns = columns
        self.description = description
        self.metadata = metadata

    @staticmethod
    def from_JSON(the_sequence: dict):
        return Sequence(
            id=the_sequence['id'],
            name=the_sequence['name'],
            external_id=the_sequence.get('externalId', None),
            asset_id=the_sequence.get('assetId', None),
            columns=[
                Column.from_JSON(the_column)
                for the_column in the_sequence['columns']
            ],
            description=the_sequence['description'],
            metadata=the_sequence['metadata']
        )


class RowValue:
    """Data transfer object for the value in a row in a sequence.

    Args:
        column_id (int):  The ID of the column that this value is for.
        value (str):      The actual value.
    """

    columnId: int
    value: str # Can be either string, float, or boolean

    def __init__(
            self,
            column_id: int,
            value: str
    ):
        self.columnId = column_id
        self.value = value

    @staticmethod
    def from_JSON(the_row_value: dict):
        return RowValue(
            column_id=the_row_value['columnId'],
            value=the_row_value['value']
        )


class Row:
    """Data transfer object for a row of data in a sequence.

    Args:
        row_number (int):  The row number for this row.
        values (list):     The values in this row.
    """
    rowNumber: int
    values: List[RowValue]

    def __init__(
            self,
            row_number: int,
            values: List[RowValue]
    ):
        self.rowNumber = row_number
        self.values = values

    @staticmethod
    def from_JSON(the_row: dict):
        return Row(
            row_number=the_row['rowNumber'],
            values=[
                RowValue.from_JSON(the_row_value)
                for the_row_value in the_row['values']
            ]
        )

    def get_row_as_csv(self):
        return ','.join([str(x.value) for x in self.values])


class SequenceDataResponse:
    """Data transfer object for the data in a sequence, used when receiving data.

    Args:
        rows (list):  List of rows with the data.
    """

    rows: List[Row]

    def __init__(
            self,
            rows: List[Row]
    ):
        self.rows = rows

    @staticmethod
    def from_JSON(the_data: dict):
        return SequenceDataResponse(
            rows=[
                Row.from_JSON(the_row)
                for the_row in the_data['rows']
            ]
        )

    @staticmethod
    def _row_has_value_for_column(row: Row, column_id: int):
        return column_id in [value.columnId for value in row.values]

    @staticmethod
    def _get_value_for_column(row: Row, column_id: int):
        return next(value.value for value in row.values if value.columnId == column_id)

    def to_pandas(self):
        """Returns data as a pandas dataframe"""

        # Create the empty dataframe
        column_ids = [value.columnId for value in self.rows[0].values]
        my_df = pd.DataFrame(
            columns=column_ids
        )
        # Fill the dataframe with values. We might not have data for every column, so we need to be careful
        for row in self.rows:
            data_this_row: List[float] = []
            for column_id in column_ids:
                # Do we have a value for this column?
                if self._row_has_value_for_column(row, column_id):
                    data_this_row.append(
                        self._get_value_for_column(row, column_id)
                    )
                else:
                    data_this_row.append(
                        'null'
                    )
            my_df.loc[len(my_df)] = data_this_row
        return my_df

    def to_json(self):
        """Returns data as a json object"""
        pass


class SequenceDataRequest:
    """Data transfer object for requesting sequence data.

    Args:
        inclusive_from (int):    Row number to get from (inclusive).
        inclusive_to (int):      Row number to get to (inclusive).
        limit (int):             How many rows to return.
        column_ids (List[int]):  ids of the columns to get data for.
    """

    inclusiveFrom: int
    inclusiveTo: int
    limit: int = 100
    columnIds: List[int] = []

    def __init__(
            self,
            inclusive_from: int,
            inclusive_to: int,
            limit: int = 100,
            column_ids: List[int] = None
    ):
        self.inclusiveFrom = inclusive_from
        self.inclusiveTo = inclusive_to
        self.limit = limit
        self.columnIds = column_ids or []
