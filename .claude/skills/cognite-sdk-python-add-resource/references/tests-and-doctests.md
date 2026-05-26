# Tests and doctests for a new sub-API

Reference: `tests/tests_unit/test_api/test_data_modeling/test_streams.py` (~149 lines).

This file focuses on **what to write for a new sub-API**.

## Layered fixture pattern

Round-2 review requested splitting mock responses into reusable fixtures rather than inline dicts. Streams's pattern — copy this shape:

```python
import re
from collections.abc import Callable
import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.streams import (
    Stream, StreamList, StreamWrite, ...
)
from tests.utils import jsgz_load


@pytest.fixture
def make_stream_response() -> Callable[..., dict]:
    def _make(external_id: str = "st1", created_time: int = 10) -> dict:
        return {
            "externalId": external_id,
            "createdTime": created_time,
            "createdFromTemplate": "ImmutableTestStream",
            "type": "Immutable",
            "settings": {...},
        }
    return _make


@pytest.fixture
def stream_response(make_stream_response: Callable[..., dict]) -> dict:
    return make_stream_response()


@pytest.fixture
def stream_list_response(stream_response: dict) -> dict:
    return {"items": [stream_response]}


@pytest.fixture
def streams_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.data_modeling.streams._base_url_with_base_path + "/streams"
```

Three layers:
1. **`make_<x>_response`** — factory; takes overrides, returns a fresh dict.
2. **`<x>_response`** — single-instance default, calls the factory.
3. **`<x>_list_response`** — wraps the single in `{"items": [...]}`.

Plus a **`<feature>_base_url`** fixture derived from the live async client. This keeps test URLs in sync with `_RESOURCE_PATH` — if you later change the path, tests don't need URL updates.

## Top-level fixtures (already provided)

`tests/tests_unit/conftest.py:35,53` defines:
- `cognite_client` (sync `CogniteClient`)
- `async_client` (`AsyncCogniteClient`)

Both are class-scoped. `httpx_mock: HTTPXMock` comes from `pytest_httpx` (a dev dep already in `pyproject.toml`).

The `tests/conftest.py` autouse `require_semaphore_on_every_request` fixture forces every internal HTTP call to use a semaphore — the helpers handle this; if you hand-rolled HTTP it'll fail loudly. To opt out: `@pytest.mark.allow_no_semaphore("<reason>")`.

## What to test

Streams's tests cover:

1. **List parses items.** `httpx_mock.add_response(method="GET", url=..., json=stream_list_response)`, call `await async_client.data_modeling.streams.list()`, assert type is `StreamList` and at least one field on the item.
2. **Retrieve passes query params.** Assert `requests[0].url.params["includeStatistics"].lower() == "true"`.
3. **Create posts a single item.** Assert the wire payload via `jsgz_load(requests[0].content)` (helper at `tests/utils.py`).
4. **Create chunks ≥2 items.** Register two responses, assert `len(requests) == 2`, assert each request body matches one of the two inputs. Proves `_CREATE_LIMIT=1`.
5. **Delete chunks ≥2 items.** Same shape, hits `/streams/delete`. Proves `_DELETE_LIMIT=1`.

URL matching tip — the `_list` helper injects `?limit=...` even when you don't expose it. Match flexibly:

```python
import re
url_pattern = re.compile(re.escape(streams_base_url) + r"(?:\?.+)?$")
httpx_mock.add_response(method="GET", url=url_pattern, json=stream_list_response)
```

## What NOT to test

**Don't** write load/dump round-trip tests for your DTOs. Round-2 maintainer review:

> "I think most of these tests are already covered by our automatic testing, at least load/dump roundtrips of varying sorts, meaning most if not all of these tests can be removed."

PR #2534 had a full `tests/.../test_data_models/test_streams.py` round-trip suite that got **deleted entirely**. There's an existing parametric harness keyed off `_RESOURCE` registries that exercises every `CogniteResource._load(...).dump() == original` round-trip. Look for it before duplicating.

## Async vs sync — pick your fixture

The streams tests use **async** (`async_client`) because async is the source of truth and easier to assert against `httpx_mock`. `pytest.ini` has `anyio_mode = auto` so `async def test_…` works without explicit decorators.

If your scenario only makes sense via the sync path (e.g. testing the sync wrapper itself), use `cognite_client`. But if you can write the test once against async, do — sync is auto-generated and equivalent.

## Retry-classification test entry

Append your endpoint(s) to the parametric list in `tests/tests_unit/test_api_client.py` (around lines 1841-1843):

```python
("POST", "https://api.cognitedata.com/api/v1/projects/bla/streams", False),
("POST", "https://api.cognitedata.com/api/v1/projects/bla/streams/delete", False),
```

`False` if the endpoint is in `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` (non-idempotent). `True` for retry-safe.

## Doctest registration

The trap caught by PR #2534 commit `987b7d1` ("ensure we run tests on docstrings"). File: `tests/tests_unit/test_docstring_examples.py`.

### Nested API

Extend the import block (lines 32-41) and the per-group test method:

```python
# Top of file:
from cognite.client._api.data_modeling import (
    containers,
    data_models,
    graphql,
    instances,
    spaces,
    statistics,
    streams,            # <-- add (alphabetical)
    views,
)
...

# Inside class TestDocstringExamples:
def test_data_modeling(self) -> None:
    run_docstring_tests(containers)
    ...
    run_docstring_tests(streams)   # <-- add
```

### Flat API

Add an import to the top-of-file `from cognite.client._api import (...)` block (lines 12-31) and a new method on the class:

```python
def test_<feature>(self) -> None:
    run_docstring_tests(<feature>)
```

The class is decorated with `@patch("cognite.client.CogniteClient", CogniteClientMock)`, so `>>>` examples that build a real `CogniteClient()` work via the mock.

### Doctest content (in the API class docstrings)

Every public method needs an `Examples:` section with at least one `>>>` block. Conventions (also enforced by `scripts/custom_checks/docstr_examples.py`):

- One `from cognite.client import CogniteClient` per method's `Examples:` block.
- One `client = CogniteClient()` per method's `Examples:` block.
- `>>>` for executable lines, `...` for continuations.
- For async-only methods, hint `>>> # async_client = AsyncCogniteClient()` as a comment but doctest still drives the sync mock.

See `cognite/client/_api/data_modeling/streams.py:47-67` for the canonical method-level `Examples:` block.

If a docstring example needs a fixture (env var, credential, special mock shape), patch it in `test_docstring_examples.py` — see `test_credential_providers` for the decorator pattern.

## Pre-flight commands for tests/doctests

```bash
# Run only your new tests:
poetry run pytest tests/tests_unit/test_api/test_data_modeling/test_streams.py -v

# Run only your doctests (filter by group):
poetry run pytest tests/tests_unit/test_docstring_examples.py -k data_modeling -v

# Full unit suite (parallel):
poetry run pytest tests/tests_unit -n4 --dist loadscope

# RST doctests (e.g. quickstart):
poetry run pytest docs/source/quickstart.rst
```
