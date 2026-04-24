from __future__ import annotations

from unittest.mock import patch

import pytest
from jsonschema.exceptions import SchemaError

from cognite.client._api.ai.python_schema import ErrorResult, SchemaResult, extract_function_schema

# ---------------------------------------------------------------------------
# US1 — Core Schema Extraction + Primitive Types
# ---------------------------------------------------------------------------


@pytest.mark.us1
def test_no_handle_function():
    result = extract_function_schema("def process(name: str): pass")
    assert isinstance(result, ErrorResult)
    assert result.errors[0] == "Function 'handle' not found."


@pytest.mark.us1
def test_missing_type_annotation():
    result = extract_function_schema("def handle(name: str, age): pass")
    assert isinstance(result, ErrorResult)
    assert any("Missing type annotation for parameter(s): age" in e for e in result.errors)


@pytest.mark.us1
def test_duplicate_argument_names():
    result = extract_function_schema("def handle(name: str, name: int): pass")
    assert isinstance(result, ErrorResult)
    assert any("Duplicate argument name(s) found: name" in e for e in result.errors)


@pytest.mark.us1
def test_invalid_python_syntax():
    result = extract_function_schema("def handle(name: str\n pass")
    assert isinstance(result, ErrorResult)
    assert len(result.errors) > 0


@pytest.mark.us1
def test_primitive_types():
    code = "def handle(name: str, age: int, height: float, is_active: bool) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["name"] == {"type": "string"}
    assert result.properties["age"] == {"type": "integer"}
    assert result.properties["height"] == {"type": "number"}
    assert result.properties["is_active"] == {"type": "boolean"}
    assert result.required == ["age", "height", "is_active", "name"]


@pytest.mark.us1
def test_primitive_list_types():
    code = "def handle(names: list[str], ages: list[int], heights: list[float], flags: list[bool]) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["names"] == {"type": "array", "items": {"type": "string"}}
    assert result.properties["ages"] == {"type": "array", "items": {"type": "integer"}}
    assert result.properties["heights"] == {"type": "array", "items": {"type": "number"}}
    assert result.properties["flags"] == {"type": "array", "items": {"type": "boolean"}}


@pytest.mark.us1
def test_nested_list_unsupported():
    result = extract_function_schema("def handle(nested: list[list[str]]) -> None: pass")
    assert isinstance(result, ErrorResult)
    assert any("nested list" in e for e in result.errors)


@pytest.mark.us1
def test_async_handle():
    code = "async def handle(name: str) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["name"] == {"type": "string"}
    assert result.required == ["name"]


@pytest.mark.us1
def test_empty_handle():
    result = extract_function_schema("def handle() -> None: pass")
    assert isinstance(result, SchemaResult)
    assert result.properties == {}
    assert result.required == []


@pytest.mark.us1
def test_no_docstring():
    code = "def handle(name: str, age: int) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert "description" not in result.properties["name"]
    assert "description" not in result.properties["age"]


@pytest.mark.us1
def test_multiple_functions_only_handle_extracted():
    code = """
def helper(x: int):
    pass

def handle(name: str):
    pass
"""
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert list(result.properties.keys()) == ["name"]


@pytest.mark.us1
def test_draft7_validation_failure():
    with patch(
        "cognite.client._api.ai.python_schema.Draft7Validator.check_schema",
        side_effect=SchemaError("bad schema"),
    ):
        result = extract_function_schema("def handle(name: str): pass")
    assert isinstance(result, ErrorResult)
    assert len(result.errors) > 0


# ---------------------------------------------------------------------------
# US2 — Extended Type Support
# ---------------------------------------------------------------------------


@pytest.mark.us2
def test_nodeid_scalar_and_list():
    code = "def handle(node: NodeId, node_list: list[NodeId]) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)

    expected_scalar = {
        "type": "object",
        "properties": {"space": {"type": "string"}, "externalId": {"type": "string"}},
        "required": ["externalId", "space"],
    }
    assert result.properties["node"] == expected_scalar

    expected_list = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {"space": {"type": "string"}, "externalId": {"type": "string"}},
            "required": ["externalId", "space"],
        },
    }
    assert result.properties["node_list"] == expected_list
    assert "properties" not in result.properties["node_list"]
    assert "required" not in result.properties["node_list"]


@pytest.mark.us2
def test_datetime_scalar_and_list():
    code = "def handle(date: datetime, date_list: list[datetime]) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["date"] == {"type": "string", "format": "date-time"}
    assert result.properties["date_list"] == {"type": "array", "items": {"type": "string", "format": "date-time"}}


@pytest.mark.us2
def test_datetime_fully_qualified():
    code = "def handle(date: datetime.datetime) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["date"] == {"type": "string", "format": "date-time"}


@pytest.mark.us2
def test_pd_dataframe():
    code = "def handle(x: int, assets: pd.DataFrame) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["assets"]["type"] == "string"
    assert result.properties["assets"]["format"] == "cognite-query-id"


@pytest.mark.us2
def test_dataframe_bare():
    code = "def handle(assets: DataFrame) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, SchemaResult)
    assert result.properties["assets"]["type"] == "string"
    assert result.properties["assets"]["format"] == "cognite-query-id"


@pytest.mark.us2
def test_list_dataframe_unsupported():
    result = extract_function_schema("def handle(dfs: list[pd.DataFrame]) -> None: pass")
    assert isinstance(result, ErrorResult)
    assert any("list[pd.DataFrame]" in e for e in result.errors)


@pytest.mark.us2
@pytest.mark.parametrize(
    "annotation",
    ["dict", "any", "MyCustomNodeId"],
)
def test_unsupported_scalar_type(annotation: str):
    code = f"def handle(param: {annotation}) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, ErrorResult)
    assert any(annotation.lower() in e.lower() for e in result.errors)


@pytest.mark.us2
@pytest.mark.parametrize(
    "annotation",
    ["list[list[str]]", "list[set]", "list[tuple]", "list[complex]"],
)
def test_unsupported_list_type(annotation: str):
    code = f"def handle(param: {annotation}) -> None: pass"
    result = extract_function_schema(code)
    assert isinstance(result, ErrorResult)
    assert len(result.errors) > 0


@pytest.mark.us2
@pytest.mark.parametrize(
    "annotation",
    ["dict", "any", "MyCustomNodeId", "list[list[str]]", "list[set]", "list[tuple]", "list[complex]"],
)
def test_unsupported_type_with_docstring(annotation: str):
    code = f'''
def handle(param: {annotation}) -> None:
    """Summary.

    Args:
        param: some description
    """
    pass
'''
    result = extract_function_schema(code)
    assert isinstance(result, ErrorResult)
