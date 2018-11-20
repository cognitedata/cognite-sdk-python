# -*- coding: utf-8 -*-
"""Data Objects

This module contains data objects used to represent the data returned from the API.
"""

from typing import List, Union


class Column:
    """Data transfer object for a column.

    Args:
        id (int):           ID of the column.
        name (str):         Name of the column.
        externalId (str):   External ID of the column.
        dataType (str):     Data type of the column.
        metadata (dict):    Custom, application specific metadata. String key -> String Value.
    """

    id: Union[int, None]
    name: str
    externalId: str
    dataType: str
    metadata: dict

    def __init__(
            self,
            id: Union[int, None],
            name: str,
            externalId: str,
            dataType: str,
            metadata: dict
    ):
        self.id = id
        self.name = name
        self.externalId = externalId
        self.dataType = dataType
        self.metadata = metadata

    @staticmethod
    def from_JSON(the_column):
        return Column(
            id=the_column['id'],
            name=the_column['name'],
            externalId=the_column.get('externalId', None),
            dataType=the_column['dataType'],
            metadata=the_column['metadata']
        )


class Sequence:
    """Data transfer object for a sequence.

    Args:
        id (int):           ID of the sequence.
        name (str):         Name of the sequence.
        externalId (str):   External ID of the sequence.
        assetId (int):      ID of the asset the sequence is connected to, if any.
        columns (list):     List of columns in the sequence.
        description (str):  Description of the sequence.
        metadata (dict):    Custom, application specific metadata. String key -> String Value.
    """

    id: Union[int, None]
    name: str
    externalId: str
    assetId: int
    columns: List[Column]
    description: str
    metadata: dict

    def __init__(
            self,
            id: Union[int, None],
            name: str,
            externalId: str,
            assetId: Union[int, None],
            columns: List[Column],
            description: str,
            metadata: dict
    ):
        self.id = id
        self.name = name
        self.externalId = externalId
        self.assetId = assetId
        self.columns = columns
        self.description = description
        self.metadata = metadata

    @staticmethod
    def from_JSON(the_sequence: dict):
        return Sequence(
            id=the_sequence['id'],
            name=the_sequence['name'],
            externalId=the_sequence.get('externalId', None),
            assetId=the_sequence.get('assetId', None),
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

    column_id: int
    value: str # Can be either string, float, or boolean

    def __init__(
            self,
            column_id: int,
            value: str
    ):
        self.column_id = column_id
        self.value = value


class Row:
    """Data transfer object for a row of data in a sequence.

    Args:
        row_number (int):  The row number for this row.
        values (list):     The values in this row.
    """
    row_number: int
    values: List[RowValue]

    def __init__(
            self,
            row_number: int,
            values: List[RowValue]
    ):
        self.row_number = row_number
        self.values = values

    def get_row_as_csv(self):
        return ','.join([str(x.value) for x in self.values])


class SequenceData:
    """Data transfer object for the data in a sequence.

    Args:
        id (int):     ID of the sequence.
        rows (list):  List of rows with the data.
    """

    id: int
    rows: List[Row]

    def __init(
            self,
            id: int,
            rows: List[Row]
    ):
        self.id = id
        self.rows = rows
