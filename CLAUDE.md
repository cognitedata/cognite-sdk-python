# CLAUDE.md — `cognite-sdk-python`

Repo-wide conventions, traps, and idioms. This file is auto-loaded into every session in this directory.

For dev setup, the CI gauntlet, the release flow, and PR-title rules, read the canonical files directly: `CONTRIBUTING.md`, `.pre-commit-config.yaml`, `.github/workflows/build.yml`, `.github/workflows/conventional-pr-title.yml`, `release-please-config.json`, `.github/pull_request_template.md`. This file deliberately does not paraphrase them.

## Mental model

Async is the source of truth. Every real implementation lives under `cognite/client/_api/<group>/<feature>.py`; the primary client is `AsyncCogniteClient` (`cognite/client/_cognite_client.py`). The sync flavor under `cognite/client/_sync_api/...` (and the public `CogniteClient` in `cognite/client/_sync_cognite_client.py`) is **auto-generated** from the async sources by `scripts/sync_client_codegen/main.py`. Each generated file carries an md5-style hash banner — never hand-edit. Most users still import `from cognite.client import CogniteClient` and get the sync flavor, so both surfaces must read like a polished public API.

## Hard rules

1. **Sync mirror is generated.** After editing any `cognite/client/_api/...` file, run `python scripts/sync_client_codegen/main.py run --files <path>` (pre-commit also does it). Stage the regenerated `cognite/client/_sync_api/...` file in the same commit. CI runs `... verify` and fails on hash drift. If a sync file gets corrupt, scramble its hash banner or delete it and let `run` regenerate.

2. **Every public method needs an `Examples:` doctest, AND the module needs to be registered.** Doctests for `cognite/client/...` modules don't auto-collect — they run from `tests/tests_unit/test_docstring_examples.py::TestDocstringExamples::test_<group>` via `run_docstring_tests(<module>)`. New module → add the import at the top **and** the call in the matching `test_<group>` method. Forgetting this is silent: PR #2534 commit `987b7d1` exists for exactly this trap.

3. **No `dict[str, Any]` in public signatures.** Build typed DTOs. Use `Literal[...]` for closed enums (`type: Literal["Immutable", "Mutable"]`), `Sequence[X]` (not `MutableSequence`) in public params, and `cognite.client.utils.useful_types.SequenceNotStr` when you need "list of strings but reject a bare `str`". The canonical anti-example is `StreamWrite.settings: dict | StreamTemplateWriteSettings` — killed in PR #2534 commit `934d0e1`.

4. **`<X>` / `<X>Write` / `<X>List` naming and read/write split.** Read DTO subclasses `WriteableCogniteResource["<X>Write"]` and implements `as_write() -> <X>Write`; write DTO does too, returning `self`. List class is `<X>List(CogniteResourceList[<X>])` (mix in `ExternalIDTransformerMixin` if items have `external_id`). API class is `<Feature>API`. Older code may use `<X>Apply` or `<X>Upsert` — for new code use `Write`. The pair is what lets `_create_multiple` accept either flavor.

5. **Use the `APIClient` helpers; don't roll your own POST.** `_create_multiple`, `_retrieve`, `_retrieve_multiple`, `_list` / `_list_generator`, `_delete_multiple` handle chunking, semaphores, identifier shaping, single-vs-list overloads, and `404 → None` for retrieve. For one-item-per-request endpoints set `_CREATE_LIMIT = 1` (and/or `_DELETE_LIMIT = 1`) in `__init__` and let the helper chunk a `Sequence[X]` into N calls. Don't validate `len(items) == 1` by hand.

6. **`@overload` the singular/plural pair.** Pass one, return one; pass a list, return a list:
   ```python
   @overload
   async def create(self, items: StreamWrite) -> Stream: ...
   @overload
   async def create(self, items: Sequence[StreamWrite]) -> StreamList: ...
   async def create(self, items: StreamWrite | Sequence[StreamWrite]) -> Stream | StreamList: ...
   ```

7. **`from __future__ import annotations` at the top of every module.** Universal in this repo and required for the `X | Y` syntax on Python 3.10 (the canonical dev version).

8. **No internal codenames in the public surface.** Class names, docstrings, parameter names, error messages — all customer-facing. PR #2534 had to scrub internal product codenames everywhere before merge.

9. **`FeaturePreviewWarning` for alpha/beta features.** Construct in `__init__` as `self._warning = FeaturePreviewWarning(api_maturity=..., sdk_maturity=..., feature_name=...)`. Call `self._warning.warn()` as the **first line** of every public method body. It's a `FutureWarning` subclass at `cognite/client/utils/_experimental.py`, picklable, and `pytest.ini` already filters it to `ignore`. Pick `sdk_maturity="alpha"` whenever you want freedom to redesign the Python interface, even if the backend API is GA.

10. **Don't edit `CHANGELOG.md` or `cognite/client/_version.py` by hand.** release-please owns both; they're regenerated from Conventional Commit titles on every push to `master`.

## Anti-patterns (recurring review pushback from PR #2534)

1. **BAD:** `dict[str, Any]` in a public signature.
   **GOOD:** Build a typed DTO (subclass `CogniteResource`).
2. **BAD:** Internal codenames in class names or docstrings.
   **GOOD:** Scrub to public-facing terminology before merge.
3. **BAD:** New module with `Examples:` blocks but no entry in `test_docstring_examples.py`.
   **GOOD:** Register it (silently never runs otherwise).
4. **BAD:** Hand-rolled `if len(items) > 1: raise ValueError(...)` on a single-item-per-request endpoint.
   **GOOD:** `self._CREATE_LIMIT = 1`; the helper chunks.
5. **BAD:** Ad-hoc utility helpers stuffed inside an API module.
   **GOOD:** Put them in `cognite/client/utils/...` (or, more often, prove you don't need them).
6. **BAD:** Class docstring that just restates the class name in a sentence.
   **GOOD:** Write something informational, or let `Args:` / `Returns:` / `Examples:` carry the load.
7. **BAD:** `MutableSequence[X]` in public surface, or inconsistent `Sequence` / `MutableSequence` between `create()` and `delete()`.
   **GOOD:** `Sequence[X]` everywhere.
8. **BAD:** Overriding `dump(self, camel_case=True)` "just in case".
   **GOOD:** Only override when nesting needs manual handling — inherited `convert_all_keys_to_camel_case` covers the common case (PR #2534 deleted three of four such overrides).
9. **BAD:** A bespoke `<X>DeleteItem` class for an endpoint that takes only external IDs.
   **GOOD:** `delete(external_id: str | SequenceNotStr[str])`.
10. **BAD:** PR description left stale after a redesign.
    **GOOD:** Update it before merge.

## Adding a new sub-API

If the task is "add SDK support for a new resource group" (a new sub-API like streams), use the **`cognite-sdk-python-add-resource` skill** at `.claude/skills/cognite-sdk-python-add-resource/`. It has the 16-step boilerplate checklist, decision tree (flat vs nested, alpha vs GA, chunking), iteration order, and per-file references. The rules above still apply, but the skill walks the wiring (`testing.py` mocks, docs-time imports, doctest registration, sync codegen) that's easy to miss otherwise.

## Examples to copy

- **Alpha sub-API with read/write split + chunking:** `cognite/client/_api/data_modeling/streams.py` (~160 lines), DTOs at `cognite/client/data_classes/data_modeling/streams.py`, tests at `tests/tests_unit/test_api/test_data_modeling/test_streams.py`.
- **Established flat resource:** `cognite/client/_api/assets.py`, `events.py`, `time_series.py`.
- **Beta API + alpha SDK with `_api_subversion`:** `cognite/client/_api/agents/agents.py`.
- **DTO base classes:** `cognite/client/data_classes/_base.py` (`CogniteResource`, `WriteableCogniteResource`, `CogniteResourceList`, `WriteableCogniteResourceList`, `ExternalIDTransformerMixin`).
- **APIClient helpers:** `cognite/client/_api_client.py`.
- **URL retry classification:** `cognite/client/utils/_url.py` (`NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS`).
- **Sync codegen:** `scripts/sync_client_codegen/main.py` (CLI: `run --files`, `run --all-files`, `verify`).
