from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import TypeAlias

from jsonschema.exceptions import SchemaError
from jsonschema.validators import Draft7Validator

DefaultValue: TypeAlias = str | int | float | bool
FunctionDef: TypeAlias = ast.FunctionDef | ast.AsyncFunctionDef

COGNITE_QUERY_ID_FORMAT = "cognite-query-id"
DATAFRAME_PARAMETER_TYPES = {"pd.DataFrame", "DataFrame"}
QUERY_ID_DESCRIPTION_SUFFIX = "**THIS FIELD IS POPULATED BY PROVIDING A query_id FROM THE MEMORY TABLE**"


@dataclass
class SchemaResult:
    type: str = "object"
    properties: dict[str, dict] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)


@dataclass
class ErrorResult:
    errors: list[str]


def _extract_argument_details(func_def: FunctionDef) -> tuple[list[str], dict[str, str | None]]:
    argument_names = [arg.arg for arg in func_def.args.args]
    argument_annotations: dict[str, str | None] = {
        arg.arg: (ast.unparse(arg.annotation) if arg.annotation is not None else None) for arg in func_def.args.args
    }
    return argument_names, argument_annotations


def _find_duplicate_argument_names(argument_names: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in argument_names:
        if name in seen and name not in duplicates:
            duplicates.append(name)
        seen.add(name)
    return duplicates


def _find_missing_type_annotations(argument_annotations: dict[str, str | None]) -> list[str]:
    return [name for name, annotation in argument_annotations.items() if annotation is None]


def _validate_function_arguments(func_def: FunctionDef) -> list[str]:
    argument_names, argument_annotations = _extract_argument_details(func_def)
    errors: list[str] = []
    duplicates = _find_duplicate_argument_names(argument_names)
    if duplicates:
        errors.append(f"Duplicate argument name(s) found: {', '.join(duplicates)}")
    missing = _find_missing_type_annotations(argument_annotations)
    if missing:
        errors.append(f"Missing type annotation for parameter(s): {', '.join(missing)}")
    return errors


def _extract_argument_defaults(func_def: FunctionDef, argument_names: list[str]) -> dict[str, DefaultValue]:
    defaults = func_def.args.defaults
    if not defaults:
        return {}
    offset = len(argument_names) - len(defaults)
    return {argument_names[offset + i]: ast.unparse(default) for i, default in enumerate(defaults)}


def _parse_list_type(annotation: str) -> tuple[bool, str | None]:
    if annotation.startswith("list["):
        return True, annotation[5:-1]
    return False, None


def _scalar_to_schema(name: str, base_type: str, in_list: bool) -> tuple[dict | None, str | None]:
    primitive_map: dict[str, dict] = {
        "str": {"type": "string"},
        "int": {"type": "integer"},
        "float": {"type": "number"},
        "bool": {"type": "boolean"},
    }
    if base_type in primitive_map:
        return dict(primitive_map[base_type]), None
    if base_type == "NodeId":
        return {
            "type": "object",
            "properties": {"space": {"type": "string"}, "externalId": {"type": "string"}},
            "required": ["externalId", "space"],
        }, None
    if base_type in ("datetime", "datetime.datetime"):
        return {"type": "string", "format": "date-time"}, None
    if base_type in DATAFRAME_PARAMETER_TYPES:
        if in_list:
            return None, f"Unsupported type for parameter '{name}': list[{base_type}]"
        return {"type": "string", "format": COGNITE_QUERY_ID_FORMAT}, None
    if in_list:
        return None, f"Unsupported type for parameter '{name}': list[{base_type}]"
    return None, f"Unsupported type for parameter '{name}': {base_type}"


def _parse_single_annotation(
    name: str, annotation: str, default_value: DefaultValue | None
) -> tuple[dict | None, str | None]:
    is_list, inner_type = _parse_list_type(annotation)

    if is_list:
        inner_is_list, _ = _parse_list_type(inner_type or "")
        if inner_is_list:
            return None, f"Unsupported type for parameter '{name}': nested list types are not supported"
        items_schema, err = _scalar_to_schema(name, inner_type or "", in_list=True)
        if err:
            return None, err
        return {"type": "array", "items": items_schema}, None

    scalar_prop, err = _scalar_to_schema(name, annotation, in_list=False)
    if err or scalar_prop is None:
        return None, err

    return scalar_prop, None


def _parse_type_annotations(func_def: FunctionDef) -> SchemaResult | ErrorResult:
    argument_names, argument_annotations = _extract_argument_details(func_def)
    argument_defaults = _extract_argument_defaults(func_def, argument_names)

    errors: list[str] = []
    properties: dict[str, dict] = {}
    required_params: list[str] = []

    for name in argument_names:
        annotation = argument_annotations.get(name)
        if annotation is None:
            errors.append(f"Missing type annotation for parameter '{name}'")
            continue
        default_value = argument_defaults.get(name)
        prop, err = _parse_single_annotation(name, annotation, default_value)
        if err:
            errors.append(err)
        else:
            properties[name] = prop  # type: ignore[assignment]
            if default_value is None:
                required_params.append(name)

    if errors:
        return ErrorResult(errors)
    return SchemaResult(type="object", properties=properties, required=sorted(required_params))


def _parse_docstring(func_def: FunctionDef, schema: SchemaResult) -> SchemaResult:
    return schema


def extract_function_schema(code: str) -> SchemaResult | ErrorResult:
    try:
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return ErrorResult(["Failed to parse Python code. Check that the code is valid Python code."])

        handle_func: FunctionDef | None = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "handle":
                handle_func = node
                break

        if handle_func is None:
            return ErrorResult(["Function 'handle' not found."])

        errors = _validate_function_arguments(handle_func)
        if errors:
            return ErrorResult(errors)

        result = _parse_type_annotations(handle_func)
        if isinstance(result, ErrorResult):
            return result

        try:
            result = _parse_docstring(handle_func, result)
        except ValueError as e:
            return ErrorResult([str(e)])

        schema_dict = {"type": result.type, "properties": result.properties, "required": result.required}
        try:
            Draft7Validator.check_schema(schema_dict)
        except SchemaError as e:
            return ErrorResult([str(e)])

        return result

    except Exception as e:
        return ErrorResult([f"Unexpected error during schema extraction: {e}"])
