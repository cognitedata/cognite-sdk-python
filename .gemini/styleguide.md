# Cognite Python Style Guide

## Key Principles

- **Strong Typing**: Use type hints extensively with MyPy. Avoid `Any` when possible
- **Type Safety**: Use dataclasses models for complex data
  structures instead of untyped dictionaries
- **CogniteResource**: Use `CogniteResource` for DTO (Data Transfer Object) that are request or response
  objects to/from the Cognite API
- **CogniteObject**: Use `CogniteObject` for objects that are within `CogniteResource` objects.
- **IO Safety**: Always use typed data structures for file operations and data parsing
- **Readability**: Code should be immediately understandable
- **Maintainability**: Write code that is easy to modify and extend
- **Consistency**: Follow established patterns across the codebase

## Principles on doing pull request reviews

- **Main point first.** Start with the key feedback or required action.
- **Be concise.** Use short, direct comments. Avoid unnecessary explanations.
- **Actionable suggestions.** If something needs fixing, state exactly what and how.
- **One issue per comment.** Separate unrelated feedback for clarity.
- **Code, not prose.** Prefer code snippets or examples over long text.
- **Background only if needed.** Add context only if the main point isn't obvious.

## How to do pull request summaries

- **Short recap.** Summarize the main point of the PR in one or two sentences.
- **Don't repeat the PR description.** Only add new or clarifying information.
- **Be brief unless needed.** Only write a longer summary if the PR description
  is missing crucial details.
- **Extend, don't duplicate.** If more detail is needed, clearly state what is
  missing from the PR description and add only the necessary context.

## Line Length and Formatting

- **Maximum line length**: 120 characters (configured in ruff)
- **Target Python version**: 3.10+
- **Indentation**: 4 spaces per level

## Type Hints

- **Required**: All functions, methods, and class attributes must have type hints
- **Avoid `Any`**: Use specific types whenever possible
- **File operations**: Always parse file content into typed structures

```python
# Good - typed data structure
@dataclass
class Config:
    model_name: str
    temperature: float
    max_tokens: int

def load_config(path: Path) -> Config:
    data = json.loads(path.read_text())
    return Config(**data)

# Bad - untyped dictionary
def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())
```

## Imports

- **Group imports**: Standard library, third-party, local application
- **Absolute imports**: Always use absolute imports for clarity
- **Sort alphabetically** within groups
- **Type checking imports**: Use `TYPE_CHECKING` for type-only imports

```python
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from requests import Response

from cognite.client.data_classes import Asset

if TYPE_CHECKING:
    from cognite.client import CogniteClient
```

## Naming Conventions

- **Variables/functions**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Classes**: `PascalCase`
- **Modules**: `snake_case`
- **Private members**: Single leading underscore `_private`

## Docstrings

Use concise docstrings with Args/Returns in google-style format. Based on repository patterns:

```python
def render_header(header: str) -> str:
    """
    Renders a (markdown) heading.

    Args:
        header (str): header

    Returns:
        str: The rendered header
    """
    return f"{header}\n{'=' * len(header)}\n"

def walk_sdk_documentation(content: Tag, parser: Parser[T]) -> Iterable[T]:
    """Parse the content of a file and yields documents. The parser controls how
    the sections are transformed into documents."""
    # Implementation here
```

**Docstring patterns**:

- Start with a concise description
- Use `Args:` and `Returns:` for complex functions
- Omit obvious parameter descriptions
- Keep descriptions brief and factual
- Ok for `__init__` methods to omit docstring

## Error Handling

- **Specific exceptions**: Avoid broad `Exception` catches
- **Graceful handling**: Provide meaningful error messages
- **Type safety**: Return `None` or use Union types for fallible operations

```python
def validate_data_modeling_identifier(space: str | None, external_id: str | None = None) -> None:
    if space and space in {"space", "cdf", "dms", "pg3", "shared", "system", "node", "edge"}:
        raise ValueError(f"The space ID: {space!r} is reserved. Please use another ID.")
    if external_id and external_id in { "space", "externalId", "createdTime", "lastUpdatedTime", "deletedTime"}:
        raise ValueError(f"The external ID: {external_id!r} is reserved. Please use another ID.")
```

## Tooling

- **Pre-commit**: `pre-commit run --all-files` for comprehensive checks


## Data Structures

**Prefer typed structures**:

```python
# Good
@dataclass
class FunctionError:
    function_name: str
    message: str

# Good
class QueryCompletion(BaseModel):
    query: str
    variables: dict[str, Any]

# Avoid
error_data = {"function_name": "foo", "message": "bar"}
```

## Logging

- Use the `logging` module with appropriate levels
- Include contextual information for debugging
  