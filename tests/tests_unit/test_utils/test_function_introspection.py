"""Unit tests for function introspection utilities."""

from __future__ import annotations

import pytest

from cognite.client.utils._function_introspection import (
    function_to_json_schema,
    parse_google_docstring,
    type_to_json_schema,
)


class TestParseGoogleDocstring:
    """Tests for parse_google_docstring function."""

    def test_basic_docstring(self) -> None:
        """Test parsing a basic docstring."""
        docstring = """Short description.

        Args:
            param1: First parameter.
            param2: Second parameter.

        Returns:
            Some value.
        """
        desc, params = parse_google_docstring(docstring)

        assert desc == "Short description."
        assert params["param1"] == "First parameter."
        assert params["param2"] == "Second parameter."

    def test_multiline_description(self) -> None:
        """Test multiline function description."""
        docstring = """This is a long description
        that spans multiple lines
        before the Args section.

        Args:
            param: A parameter.
        """
        desc, params = parse_google_docstring(docstring)

        assert desc == "This is a long description that spans multiple lines before the Args section."
        assert params["param"] == "A parameter."

    def test_multiline_param_description(self) -> None:
        """Test multiline parameter descriptions."""
        docstring = """Function description.

        Args:
            param: This is a long description
                that spans multiple lines
                in the docstring.
        """
        desc, params = parse_google_docstring(docstring)

        assert desc == "Function description."
        assert params["param"] == "This is a long description that spans multiple lines in the docstring."

    def test_param_with_type_annotation(self) -> None:
        """Test parameter with type annotation in docstring."""
        docstring = """Function description.

        Args:
            param (str): String parameter.
            count (int): Integer parameter.
        """
        desc, params = parse_google_docstring(docstring)

        assert params["param"] == "String parameter."
        assert params["count"] == "Integer parameter."

    def test_no_docstring(self) -> None:
        """Test with None docstring."""
        desc, params = parse_google_docstring(None)

        assert desc == ""
        assert params == {}

    def test_no_args_section(self) -> None:
        """Test docstring without Args section."""
        docstring = """Just a description.

        Returns:
            Some value.
        """
        desc, params = parse_google_docstring(docstring)

        assert desc == "Just a description."
        assert params == {}


class TestTypeToJsonSchema:
    """Tests for type_to_json_schema function."""

    def test_primitive_int(self) -> None:
        """Test int type."""
        schema = type_to_json_schema(int, "param")
        assert schema == {"type": "integer"}

    def test_primitive_float(self) -> None:
        """Test float type."""
        schema = type_to_json_schema(float, "param")
        assert schema == {"type": "number"}

    def test_primitive_str(self) -> None:
        """Test str type."""
        schema = type_to_json_schema(str, "param")
        assert schema == {"type": "string"}

    def test_primitive_bool(self) -> None:
        """Test bool type."""
        schema = type_to_json_schema(bool, "param")
        assert schema == {"type": "boolean"}

    def test_list_int(self) -> None:
        """Test list[int] type."""
        schema = type_to_json_schema(list[int], "param")
        assert schema == {"type": "array", "items": {"type": "integer"}}

    def test_list_float(self) -> None:
        """Test list[float] type."""
        schema = type_to_json_schema(list[float], "param")
        assert schema == {"type": "array", "items": {"type": "number"}}

    def test_list_str(self) -> None:
        """Test list[str] type."""
        schema = type_to_json_schema(list[str], "param")
        assert schema == {"type": "array", "items": {"type": "string"}}

    def test_list_bool(self) -> None:
        """Test list[bool] type."""
        schema = type_to_json_schema(list[bool], "param")
        assert schema == {"type": "array", "items": {"type": "boolean"}}

    def test_bare_list_raises_error(self) -> None:
        """Test that bare list type raises TypeError."""
        with pytest.raises(TypeError, match="without item type"):
            type_to_json_schema(list, "param")

    def test_unsupported_type_raises_error(self) -> None:
        """Test that unsupported types raise TypeError."""
        with pytest.raises(TypeError, match="unsupported type"):
            type_to_json_schema(dict, "param")

    def test_unsupported_list_item_type_raises_error(self) -> None:
        """Test that unsupported list item types raise TypeError."""
        with pytest.raises(TypeError, match="unsupported list item type"):
            type_to_json_schema(list[dict], "param")


class TestFunctionToJsonSchema:
    """Tests for function_to_json_schema function."""

    def test_basic_function(self) -> None:
        """Test basic function with primitives."""

        def add(a: float, b: float) -> float:
            """Add two numbers.

            Args:
                a: First number.
                b: Second number.
            """
            return a + b

        desc, schema = function_to_json_schema(add)

        assert desc == "Add two numbers."
        assert schema == {
            "type": "object",
            "properties": {
                "a": {"type": "number", "description": "First number."},
                "b": {"type": "number", "description": "Second number."},
            },
            "required": ["a", "b"],
        }

    def test_all_primitive_types(self) -> None:
        """Test all supported primitive types."""

        def example(name: str, count: int, value: float, enabled: bool) -> None:
            """Example function.

            Args:
                name: A string.
                count: An integer.
                value: A float.
                enabled: A boolean.
            """
            pass

        desc, schema = function_to_json_schema(example)

        assert schema["properties"]["name"]["type"] == "string"
        assert schema["properties"]["count"]["type"] == "integer"
        assert schema["properties"]["value"]["type"] == "number"
        assert schema["properties"]["enabled"]["type"] == "boolean"
        assert schema["required"] == ["name", "count", "value", "enabled"]

    def test_optional_parameters(self) -> None:
        """Test optional parameters (default=None)."""

        def greet(name: str, title: str | None = None) -> str:
            """Greet a person.

            Args:
                name: The person's name.
                title: Optional title.
            """
            return f"Hello, {title} {name}!" if title else f"Hello, {name}!"

        desc, schema = function_to_json_schema(greet)

        assert "name" in schema["required"]
        assert "title" not in schema["required"]
        assert "title" in schema["properties"]

    def test_function_with_no_parameters(self) -> None:
        """Test function with no parameters."""

        def get_status() -> str:
            """Get current status.

            Returns:
                The status.
            """
            return "OK"

        desc, schema = function_to_json_schema(get_status)

        assert desc == "Get current status."
        assert schema == {"type": "object", "properties": {}}
        assert "required" not in schema

    def test_function_with_no_docstring(self) -> None:
        """Test function with no docstring."""

        def add(a: float, b: float) -> float:
            return a + b

        desc, schema = function_to_json_schema(add)

        assert desc == "add"
        assert "a" in schema["properties"]
        assert "b" in schema["properties"]

    def test_custom_description_override(self) -> None:
        """Test custom description override."""

        def add(a: float, b: float) -> float:
            """Add two numbers."""
            return a + b

        desc, schema = function_to_json_schema(add, description="Custom description")

        assert desc == "Custom description"

    def test_missing_type_annotation_raises_error(self) -> None:
        """Test that missing type annotation raises TypeError."""

        def bad(param) -> None:
            """Bad function."""
            pass

        with pytest.raises(TypeError, match="missing type annotation"):
            function_to_json_schema(bad)

    def test_list_types(self) -> None:
        """Test list parameter types."""

        def process_lists(names: list[str], scores: list[float]) -> None:
            """Process lists.

            Args:
                names: List of names.
                scores: List of scores.
            """
            pass

        desc, schema = function_to_json_schema(process_lists)

        assert schema["properties"]["names"]["type"] == "array"
        assert schema["properties"]["names"]["items"]["type"] == "string"
        assert schema["properties"]["scores"]["type"] == "array"
        assert schema["properties"]["scores"]["items"]["type"] == "number"
