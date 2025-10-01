"""Utilities for introspecting Python functions to generate JSON Schema.

This module provides utilities for converting Python function signatures and docstrings
into JSON Schema definitions, primarily for use with agent function calling features.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from typing import Any, get_args, get_origin, get_type_hints


def parse_google_docstring(docstring: str | None) -> tuple[str, dict[str, str]]:
    """Parse Google-style docstring to extract description and parameter descriptions.

    Args:
        docstring (str | None): The function's docstring.

    Returns:
        tuple[str, dict[str, str]]: Tuple of (function_description, param_descriptions_dict).

    Examples:
        >>> def example(param1: str, param2: int) -> None:
        ...     '''Example function.
        ...
        ...     Args:
        ...         param1: First parameter.
        ...         param2: Second parameter.
        ...     '''
        ...     pass
        >>> desc, params = parse_google_docstring(example.__doc__)
        >>> desc
        'Example function.'
        >>> params['param1']
        'First parameter.'
    """
    if not docstring:
        return "", {}

    # Split into lines
    lines = docstring.strip().split("\n")

    # Extract function description (everything before Args section)
    description_lines = []
    param_descriptions = {}
    in_args_section = False
    current_param = None
    current_param_desc_lines = []

    for line in lines:
        stripped = line.strip()

        # Check if we're entering Args section
        if stripped in ("Args:", "Arguments:", "Parameters:"):
            in_args_section = True
            continue

        # Check if we're leaving Args section (Returns, Raises, etc.)
        if in_args_section and stripped and stripped.endswith(":"):
            # Save any pending parameter description
            if current_param and current_param_desc_lines:
                param_descriptions[current_param] = " ".join(current_param_desc_lines).strip()
            break

        # If we're not in args section yet, check if we hit Returns/Raises (end of description)
        if (
            not in_args_section
            and stripped
            and stripped.endswith(":")
            and stripped
            in (
                "Returns:",
                "Return:",
                "Raises:",
                "Raises:",
                "Yields:",
                "Yield:",
                "Note:",
                "Notes:",
                "Example:",
                "Examples:",
            )
        ):
            break

        if in_args_section:
            # Parse parameter line: "param_name: description" or "param_name (type): description"
            param_match = re.match(r"^\s*(\w+)(?:\s*\([^)]+\))?\s*:\s*(.*)$", line)
            if param_match:
                # Save previous parameter if any
                if current_param and current_param_desc_lines:
                    param_descriptions[current_param] = " ".join(current_param_desc_lines).strip()

                # Start new parameter
                current_param = param_match.group(1)
                current_param_desc_lines = [param_match.group(2)] if param_match.group(2) else []
            elif current_param and stripped:
                # Continuation of current parameter description
                current_param_desc_lines.append(stripped)
        else:
            # Part of function description
            if stripped:
                description_lines.append(stripped)

    # Save last parameter
    if current_param and current_param_desc_lines:
        param_descriptions[current_param] = " ".join(current_param_desc_lines).strip()

    # Join description lines
    description = " ".join(description_lines).strip()

    return description, param_descriptions


def type_to_json_schema(param_type: type, param_name: str) -> dict[str, Any]:
    """Convert Python type hint to JSON Schema type.

    Supports primitive types (int, float, str, bool) and lists of primitives.

    Args:
        param_type (type): The type annotation.
        param_name (str): The parameter name (for error messages).

    Returns:
        dict[str, Any]: JSON Schema type object.

    Raises:
        TypeError: If the type is not supported.

    Examples:
        >>> type_to_json_schema(int, "count")
        {'type': 'integer'}
        >>> type_to_json_schema(list[str], "names")
        {'type': 'array', 'items': {'type': 'string'}}
    """
    # Handle primitives
    if param_type is int:
        return {"type": "integer"}
    elif param_type is float:
        return {"type": "number"}
    elif param_type is str:
        return {"type": "string"}
    elif param_type is bool:
        return {"type": "boolean"}

    # Check for bare list type (without item type annotation)
    if param_type is list:
        raise TypeError(f"Parameter '{param_name}' has type 'list' without item type. Use list[int], list[str], etc.")

    # Handle list types with item type annotation
    origin = get_origin(param_type)
    if origin is list:
        args = get_args(param_type)
        if not args:
            raise TypeError(
                f"Parameter '{param_name}' has type 'list' without item type. Use list[int], list[str], etc."
            )

        item_type = args[0]
        # Only support primitives in lists
        if item_type is int:
            return {"type": "array", "items": {"type": "integer"}}
        elif item_type is float:
            return {"type": "array", "items": {"type": "number"}}
        elif item_type is str:
            return {"type": "array", "items": {"type": "string"}}
        elif item_type is bool:
            return {"type": "array", "items": {"type": "boolean"}}
        else:
            raise TypeError(
                f"Parameter '{param_name}' has unsupported list item type '{item_type.__name__}'. "
                f"Supported list types: list[int], list[float], list[str], list[bool]"
            )

    # Unsupported type
    type_name = getattr(param_type, "__name__", str(param_type))
    raise TypeError(
        f"Parameter '{param_name}' has unsupported type '{type_name}'. "
        f"Supported types: int, float, str, bool, list[int], list[float], list[str], list[bool]"
    )


def function_to_json_schema(func: Callable[..., Any], description: str | None = None) -> tuple[str, dict[str, Any]]:
    """Generate JSON Schema from a Python function's signature and docstring.

    This function introspects a Python function to extract its signature, type hints,
    and docstring, then generates a JSON Schema that describes the function's parameters.

    Args:
        func (Callable[..., Any]): The Python function to introspect.
        description (str | None): Optional description override. If not provided, extracted from docstring.

    Returns:
        tuple[str, dict[str, Any]]: Tuple of (function_description, parameters_schema).

    Raises:
        TypeError: If a parameter is missing a type annotation or has an unsupported type.

    Examples:
        >>> def add(a: float, b: float) -> float:
        ...     '''Add two numbers.
        ...
        ...     Args:
        ...         a: First number.
        ...         b: Second number.
        ...     '''
        ...     return a + b
        >>> desc, schema = function_to_json_schema(add)
        >>> desc
        'Add two numbers.'
        >>> schema['properties']['a']
        {'type': 'number', 'description': 'First number.'}
        >>> schema['required']
        ['a', 'b']
    """
    # Get function signature
    sig = inspect.signature(func)

    # Get type hints
    try:
        type_hints = get_type_hints(func)
    except Exception as e:
        raise TypeError(f"Failed to get type hints for function '{func.__name__}': {e}") from e

    # Parse docstring if description not provided
    if description is None:
        description, param_descriptions = parse_google_docstring(func.__doc__)
        # Use function name as description if no docstring
        if not description:
            description = func.__name__
    else:
        _, param_descriptions = parse_google_docstring(func.__doc__)

    # Build JSON Schema
    properties = {}
    required = []

    for param_name, param in sig.parameters.items():
        # Skip *args, **kwargs, self, cls
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        # Check if parameter has type hint
        if param_name not in type_hints:
            raise TypeError(f"Parameter '{param_name}' is missing type annotation")

        param_type = type_hints[param_name]

        # Convert type to JSON Schema
        schema_type = type_to_json_schema(param_type, param_name)

        # Add description if available
        if param_name in param_descriptions:
            schema_type["description"] = param_descriptions[param_name]

        properties[param_name] = schema_type

        # Check if required (no default or default is not None for Optional types)
        if param.default is inspect.Parameter.empty:
            required.append(param_name)
        elif param.default is not None:
            # Has a non-None default, so it's required but with a default
            required.append(param_name)
        # If default is None, it's optional (not in required list)

    # Build final parameters schema
    parameters_schema = {
        "type": "object",
        "properties": properties,
    }
    if required:
        parameters_schema["required"] = required

    return description, parameters_schema
