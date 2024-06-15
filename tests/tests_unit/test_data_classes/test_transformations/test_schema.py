import pytest

from cognite.client.data_classes import TransformationSchemaColumn
from cognite.client.data_classes.transformations.schema import (
    TransformationSchemaStructType,
    TransformationSchemaUnknownType,
)


@pytest.fixture
def struct_schema():
    return {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "string", "nullable": True, "metadata": {}},
            {"name": "type", "type": "string", "nullable": True, "metadata": {}},
            {"name": "uri", "type": "string", "nullable": True, "metadata": {}},
        ],
    }


@pytest.fixture
def unknown_schema(struct_schema):
    return {
        "type": "something-unknown",
        "fooBar": True,
        "barList": [{"1": 2}],
    }


def test_transformation_schema_column__struct(struct_schema):
    # TODO: TransformationSchemaArrayType & TransformationSchemaMapType
    api_response = {"name": "foo", "sql_type": "bar", "type": struct_schema, "nullable": False}

    col = TransformationSchemaColumn.load(api_response)
    assert isinstance(col, TransformationSchemaColumn)
    assert isinstance(col.type, TransformationSchemaStructType)
    assert col.type.type == "struct"


def test_transformation_schema_column__unknown_schema(unknown_schema):
    api_response = {"name": "foo", "sql_type": "bar", "type": unknown_schema, "nullable": False}

    col = TransformationSchemaColumn.load(api_response)
    assert isinstance(col, TransformationSchemaColumn)
    assert isinstance(col.type, TransformationSchemaUnknownType)
    assert col.type.type == "something-unknown"
