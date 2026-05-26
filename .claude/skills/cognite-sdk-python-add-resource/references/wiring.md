# Wiring a new sub-API — the five locations

Adding a new sub-API needs edits in five places. Skipping any one of them produces a different failure mode (some loud, some silent). Streams (PR #2534) hit all of them — concrete line references are inline below.

## 1. Parent group's `__init__.py` (instantiation)

### Nested API

Edit `cognite/client/_api/<group>/__init__.py`. Streams (`cognite/client/_api/data_modeling/__init__.py`):

```python
from cognite.client._api.data_modeling.streams import StreamsAPI    # line 12
...
class DataModelingAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        ...
        self.streams = StreamsAPI(config, api_version, cognite_client)   # line 31
```

Imports go alphabetically; the assignment list inside `__init__` follows the same order.

### Flat API

Edit `cognite/client/_cognite_client.py` — the `__init__` of `AsyncCogniteClient` (~lines 126-164):

```python
from cognite.client._api.<feature> import <Feature>API
...
class AsyncCogniteClient:
    def __init__(self, ...) -> None:
        ...
        self.<feature> = <Feature>API(self._config, self._API_VERSION, self)
```

The sync sibling (`_sync_cognite_client.py`) is auto-generated — do not edit.

## 2. Docs-time import block in `_cognite_client.py`

Even for **nested** APIs, you also add the import inside the `BUILD_COGNITE_SDK_DOCS` block in `cognite/client/_cognite_client.py` (~lines 45-104):

```python
if _should_build_docs := os.getenv("BUILD_COGNITE_SDK_DOCS") == "true":
    ...
    from cognite.client._api.data_modeling.streams import StreamsAPI    # streams sits at line 55
    ...
```

Why: Sphinx (which sets `BUILD_COGNITE_SDK_DOCS=true`) needs the class symbol importable at the top level so `autosummary :: AsyncCogniteClient.data_modeling.streams` resolves. **Skipping this fails `make html SPHINXOPTS="-W --keep-going"`.** PR #2534 burned commit `1e6937c` ("fix sphinx doc generation") on this miss.

For flat APIs, the import in step 1 is at module scope and Sphinx finds it; you usually don't need a second block entry — but copy the pattern of nearby flat imports if in doubt.

## 3. Mocks in `cognite/client/testing.py`

This file holds **two mirrored mock classes**: `AsyncCogniteClientMock` (~line 200) and `CogniteClientMock` (~line 410). Both need the new API for **nested** APIs.

### Async mock (lines 23, 229, 240 for streams)

```python
from cognite.client._api.data_modeling.streams import StreamsAPI       # line 23
...
class AsyncCogniteClientMock(MagicMock):
    def __init__(self, *args, **kwargs):
        ...
        dm_streams = create_autospec(StreamsAPI, instance=True, spec_set=True)   # line 229
        ...
        self.data_modeling = create_autospec(
            DataModelingAPI,
            instance=True,
            containers=dm_containers,
            ...
            streams=dm_streams,                                                  # line 240
        )
        flip_spec_set_on(self.data_modeling, dm_statistics)
```

### Sync mock (lines 107, 430, 441)

```python
from cognite.client._sync_api.data_modeling.streams import SyncStreamsAPI   # line 107
...
class CogniteClientMock(MagicMock):
    def __init__(self, *args, **kwargs):
        ...
        sync_dm_streams = create_autospec(SyncStreamsAPI, instance=True, spec_set=True)   # line 430
        ...
        self.data_modeling = create_autospec(
            SyncDataModelingAPI,
            ...
            streams=sync_dm_streams,                                                       # line 441
        )
```

### Failure mode

**Skipping this step does not fail any obvious lint.** It just means consumer code that uses `CogniteClientMock` (e.g. `from cognite.client.testing import monkeypatch_cognite_client`) will hit `AttributeError` on `mock.data_modeling.streams`. Wire it early so your unit tests work end-to-end.

For flat APIs, the registration is on the top-level `*Mock` class (not a child group). Look at how `AssetsAPI` is registered if in doubt.

## 4. Re-export DTOs

Nested: edit `cognite/client/data_classes/<group>/__init__.py`. Streams (`cognite/client/data_classes/data_modeling/__init__.py:122-132,257-265`):

```python
# Imports — alphabetical, both across blocks and within the block.
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamLifecycleSettings,
    StreamLimit,
    StreamLimitSettings,
    StreamList,
    StreamSettings,
    StreamTemplate,
    StreamTemplateWriteSettings,
    StreamWrite,
)

# __all__ — alphabetical
__all__ = [
    ...
    "Stream",
    "StreamLifecycleSettings",
    "StreamLimit",
    "StreamLimitSettings",
    "StreamList",
    "StreamSettings",
    "StreamTemplate",
    "StreamTemplateWriteSettings",
    "StreamWrite",
    ...
]
```

Flat: same pattern in `cognite/client/data_classes/__init__.py`.

`mypy.ini` sets `no_implicit_reexport = true`, so importing into the namespace alone is **not** sufficient — you must also append to `__all__`. Failure mode: downstream consumers writing `from cognite.client.data_classes.data_modeling import StreamWrite` get a mypy error.

## 5. Sync codegen

Async is the source of truth. Sync mirror is generated by `scripts/sync_client_codegen/main.py`.

```bash
# Regenerate just your file:
python scripts/sync_client_codegen/main.py run --files cognite/client/_api/data_modeling/streams.py

# Wholesale:
python scripts/sync_client_codegen/main.py run --all-files

# CI verification mode (fails if any sync file is stale):
python scripts/sync_client_codegen/main.py verify
```

Generated outputs:
- `cognite/client/_sync_api/<group>/<feature>.py` — every method calls `run_sync(self.__async_client.<group>.<feature>.<method>(...))`.
- `cognite/client/_sync_api/<group>/__init__.py` — auto-regenerated to include the new `Sync<Feature>API`.

The first lines of each generated file include a hash of the async source (e.g. `cognite/client/_sync_api/data_modeling/streams.py:1-6`). If async drifts, the hash mismatches and `verify` fails.

**Never hand-edit anything under `_sync_api/`.** If a sync file gets corrupt or stale, delete it (or scramble the hash) and rerun `run` — it'll regenerate clean.

PR #2534 burned three commits on the regen loop because codegen was run too early:
- `5a52db6` — set chunking limits on async
- `1e39c1b` — regenerate sync after the limits change
- `b25a668` — clean up F401 unused-imports the regen left behind

**Lesson: regenerate sync at the very end**, after the async signature has stabilised. If you're iterating during review, regen each round; the diff stays small.

## Tables: file-level summary

| Edit | Path | Why |
| --- | --- | --- |
| Instantiate | `_api/<group>/__init__.py` (nested) or `_cognite_client.py` (flat) | Wire the API onto the client at runtime. |
| Docs-time import | `_cognite_client.py` `BUILD_COGNITE_SDK_DOCS` block | Sphinx autosummary resolution. |
| Async mock | `testing.py` (`AsyncCogniteClientMock`) | Consumer test mocks. |
| Sync mock | `testing.py` (`CogniteClientMock`) | Consumer test mocks (sync flavor). |
| DTO re-exports | `data_classes/<group>/__init__.py` + `__all__` | Public namespace; `no_implicit_reexport=true`. |
| Sync codegen | (run script) → `_sync_api/<group>/<feature>.py` + `_sync_api/<group>/__init__.py` | Sync mirror; CI `verify` enforces. |
