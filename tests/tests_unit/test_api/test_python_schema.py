from __future__ import annotations

from unittest.mock import patch

import pytest

from cognite.client._api.ai.python_schema import ErrorResult, SchemaResult, extract_function_schema
from jsonschema.exceptions import SchemaError


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
