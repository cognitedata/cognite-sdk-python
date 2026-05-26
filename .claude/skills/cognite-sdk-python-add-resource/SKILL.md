---
name: cognite-sdk-python-add-resource
description: Use when adding a new sub-API / resource group / endpoint coverage to cognite-sdk-python — e.g. "wrap the X API", "add support for the Y endpoints", "how do I add Z to this SDK", "what files do I need to touch for a new API". Triggers on any work that creates new files under cognite/client/_api/ or cognite/client/data_classes/, or that mentions patterns specific to new APIs (`_create_multiple`, `_RESOURCE_PATH`, `WriteableCogniteResource`, `FeaturePreviewWarning`, sync codegen, doctest registration). Does NOT trigger on routine bug fixes / refactors that don't introduce a new resource. Even when the user only says "I want to add streams-style endpoints", this skill should fire.
---

# Adding a new sub-API to cognite-sdk-python

This skill walks you through introducing a new resource group (sub-API) to the SDK. The streams PR (#2534) is the worked example throughout — when in doubt, open `cognite/client/_api/data_modeling/streams.py` and copy the shape.

## When this skill applies

Use this skill if you're:
- Creating a new file under `cognite/client/_api/` or `cognite/client/_api/<group>/` for a CDF resource the SDK doesn't yet wrap
- Adding the parallel DTOs under `cognite/client/data_classes/[<group>/]<feature>.py`
- Wiring a new API onto `AsyncCogniteClient` (or onto a parent group like `DataModelingAPI`)

If you're only modifying an existing API, skip this skill.

## Decision-tree opener — answer these THREE before writing any code

### 1. Flat or nested?

- **Flat** (`cognite/client/_api/<feature>.py`) — feature stands alone at `client.<feature>`. Examples: `assets.py`, `events.py`, `documents.py`. Wired in `cognite/client/_cognite_client.py` `__init__`.
- **Nested** (`cognite/client/_api/<group>/<feature>.py`) — feature is a sub-resource at `client.<group>.<feature>`. Examples: everything under `data_modeling/`, `agents/`, `ai/`, `simulators/`. Wired in the group's own `__init__.py`.

Pick **nested** if a parent group already exists and the feature belongs to it. Streams was originally placed at `cognite/client/_api/streams/__init__.py` and got relocated to `cognite/client/_api/data_modeling/streams.py` during review — costing a relocation churn. Decide before authoring.

The flat/nested choice mirrors across `cognite/client/data_classes/`, `cognite/client/_sync_api/`, `tests/tests_unit/test_api/`, and `docs/source/`.

### 2. Alpha/beta or GA?

Drives whether you instantiate `FeaturePreviewWarning`:

- **GA API + GA SDK** → no preview warning (don't spam users on stable features).
- **Anything else** → instantiate `FeaturePreviewWarning(api_maturity=..., sdk_maturity=..., feature_name="...")` in `__init__` and call `self._warning.warn()` as the **first line of every public method**. `api_maturity` and `sdk_maturity` are independent dials.
  - Streams: `api_maturity="General Availability", sdk_maturity="alpha"` (API is GA, SDK shape may evolve).
  - Agents: `api_maturity="beta", sdk_maturity="alpha"` plus `self._api_subversion = "beta"` for beta-route routing.

See `references/alpha-and-warnings.md` for details.

### 3. Does the create/delete endpoint chunk?

If the server accepts only one item per request (or some other small N):

- **GOOD:** set `self._CREATE_LIMIT = N` and/or `self._DELETE_LIMIT = N` in `__init__`.
- **BAD:** hand-rolling `if len(items) > 1: raise ValueError(...)` — `_create_multiple` / `_delete_multiple` chunk for you.
- **BAD:** dropping the `Sequence[X]` overload — pass-one/get-one and pass-list/get-list ergonomics stay.
- Streams sets both to `1`. Public signature is unchanged from the multi-item case.

If the create/delete are **not idempotent at the server**, also register the path in `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` (`cognite/client/utils/_url.py`).

---

## The 16-step boilerplate checklist

Tags: `[U]` universal, `[C]` conditional, `[O]` optional. Each step names the file(s) to edit and points at the streams reference.

1. **[U] Decide layout.** Flat vs nested (see above). Streams chose nested under `data_modeling/`.

2. **[U] Author the DTOs first** at `cognite/client/data_classes/[<group>/]<feature>.py`. Subclass `CogniteResource` / `WriteableCogniteResource[<Write>]` / `CogniteResourceList[<Read>]` from `cognite.client.data_classes._base`. Implement `_load`; override `dump` only when nesting needs recursion. Add `as_write()` on the read class. Reference: `cognite/client/data_classes/data_modeling/streams.py`. Deep dive: `references/dto-design.md`.

3. **[U] Re-export DTOs** in the group-level `__init__.py` (alphabetical in both the import block and `__all__`). For nested: `cognite/client/data_classes/<group>/__init__.py`. For flat: `cognite/client/data_classes/__init__.py`. Reference: `cognite/client/data_classes/data_modeling/__init__.py:122-132,257-265`. Mypy's `no_implicit_reexport=true` means namespace import alone is not enough.

4. **[U] Implement the async API class** at `cognite/client/_api/[<group>/]<feature>.py`. Subclass `APIClient`. Set `_RESOURCE_PATH`. Use `_create_multiple` / `_list` / `_retrieve` / `_delete_multiple` — **do not hand-roll HTTP**. Reference: `cognite/client/_api/data_modeling/streams.py`. The helper signatures are in `cognite/client/_api_client.py`.

5. **[C] Set `_CREATE_LIMIT` / `_DELETE_LIMIT` / `_LIST_LIMIT`** in `__init__` *only when* the endpoint chunks. Streams sets both `_CREATE_LIMIT = 1` and `_DELETE_LIMIT = 1`.
   - **BAD:** writing `if len(items) > 1: raise ValueError(...)` — `_create_multiple` chunks for you.
   - **BAD:** dropping the `Sequence[X]` overload — keep the singular/plural pair (step 4 still applies).
   - **GOOD:** just set the limit. Public signature, overloads, and body stay identical to the multi-item case.

6. **[C] Add `FeaturePreviewWarning`** if API or SDK is not GA-stable. Instantiate in `__init__` (`self._warning = FeaturePreviewWarning(...)`); call `self._warning.warn()` as the first line of every public method body. See `references/alpha-and-warnings.md`.

7. **[U] Wire the new API class into the parent group's `__init__.py`** (nested) — e.g. add `self.streams = StreamsAPI(config, api_version, cognite_client)` to `DataModelingAPI.__init__`. For flat: register on `AsyncCogniteClient.__init__` in `cognite/client/_cognite_client.py`. Reference: `cognite/client/_api/data_modeling/__init__.py:31`. Walkthrough: `references/wiring.md`.

8. **[U] Add the docs-time import** in `cognite/client/_cognite_client.py`. There's a `BUILD_COGNITE_SDK_DOCS` block (~lines 45-104) that imports nested-API classes so Sphinx can resolve `AsyncCogniteClient.<group>.<feature>`. Add `from cognite.client._api.<group>.<feature> import <Feature>API` (alphabetical neighbours). Without this, `make html` under `-W --keep-going` fails. Reference line: 55. See `references/wiring.md`.

9. **[U] Register mocks in `cognite/client/testing.py`** for both the async (`<Feature>API`) and the sync (`Sync<Feature>API`) class. Add `<feature> = create_autospec(<Feature>API, instance=True, spec_set=True)` and pass it as a kwarg into the parent group's `create_autospec(...)` so consumer unit tests pick up the new attribute. Streams pattern at lines 23, 107, 229, 240, 430, 441. **Skipping this step does not fail any lint** — but downstream `CogniteClientMock` users will hit `AttributeError`. See `references/wiring.md`.

10. **[C] Add the resource path to `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS`** in `cognite/client/utils/_url.py` *only when* the create/delete endpoints are not idempotent server-side. Streams adds `"streams"` (line 37). Also append a parametric entry in `tests/tests_unit/test_api_client.py` (the retry-classification list around lines 1841-1843).

11. **[U] Write unit tests** at `tests/tests_unit/test_api/[<group>/]test_<feature>.py` using `pytest_httpx`. Use the `cognite_client` (sync) and `async_client` fixtures from `tests/tests_unit/conftest.py`. Layered fixture pattern: `make_<x>_response` (factory) → `<x>_response` (single) → `<x>_list_response` (wrapper) → `<feature>_base_url` (derived from live client). Reference: `tests/tests_unit/test_api/test_data_modeling/test_streams.py`. Cover create/list/retrieve/delete + chunking proofs (when `_CREATE_LIMIT=1`/`_DELETE_LIMIT=1`). **Don't write load/dump round-trip tests** — there's an existing parametric harness keyed off `_RESOURCE`. See `references/tests-and-doctests.md`.

12. **[U] Register doctests** in `tests/tests_unit/test_docstring_examples.py` — extend the import block AND append `run_docstring_tests(<feature>)` to the appropriate `TestDocstringExamples.test_<group>` method. **Forgetting this was an explicit fix-up commit in PR #2534** (`987b7d1`). Without it your `>>>` examples never run. See `references/tests-and-doctests.md`.

13. **[U] Add docs** in `docs/source/<group>.rst` (nested) or `docs/source/cognite.rst` / a new top-level rst (flat). Add a `.. autosummary::` block for the API methods + an `.. automodule::` for the data classes. Match neighbour heading levels (`-` top, `^` nested). Reference: `docs/source/data_modeling.rst:289-302`.

14. **[U] Run sync codegen** from the repo root:
    ```bash
    python scripts/sync_client_codegen/main.py run --files cognite/client/_api/[<group>/]<feature>.py
    ```
    Commit the generated `cognite/client/_sync_api/[<group>/]<feature>.py` (and the regenerated `_sync_api/<group>/__init__.py`). Pre-commit will run this hook. **Never hand-edit anything under `_sync_api/`.** Run codegen *last*, after the async signature is stable, to avoid repeated regen+lint commits — PR #2534 ate three commits on this loop.

15. **[U] Pre-flight before opening PR** — see Pre-flight section below.

16. **[U] Open PR with a Conventional Commit title** (`feat(<feature>): …`).

---

## Iteration order — what to do first vs last

PR #2534 went through three review rounds; most of the churn was avoidable. Distilled lessons:

1. **Author the DTOs first**, with the maximally-typed shape. No `dict[str, Any]`, no `Any`, use `Literal[...]` for closed enums. This avoids the round-1 typing rewrite.
2. **Implement the API class using helpers from the start.** Set `_CREATE_LIMIT`/`_DELETE_LIMIT` if applicable. Don't write any HTTP yourself.
3. **Write the docstrings (with `>>>` Examples blocks) before the tests.** Forces every `Args:`/`Returns:`/`Examples:` block to exist. No restating the class header; one-line class docstring.
4. **Wire docs (rst) + the docs-time import in `_cognite_client.py` + the doctest registration together.** Three small, easily-missed steps. PR #2534 forgot two of them and burned fix-up commits `1e6937c` ("fix sphinx doc generation") and `987b7d1` ("ensure we run tests on docstrings") on merge day.
5. **Wire mocks early in `testing.py`** (both async and sync) so unit tests that mock the SDK work from day one.
6. **Write the unit tests** — chunking proofs, doctest-friendly fixture names, plus the `test_api_client.py` retry entry.
7. **Run sync codegen LAST.** Once the async signature is stable. PR #2534 burned `5a52db6` → `1e39c1b` → `b25a668` (limit change → regen → fix unused imports) because codegen ran mid-iteration.
8. **Add `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` if applicable** + the matching `test_api_client.py` entry.
9. **Run pre-flight locally.** Open PR with Conventional Commit title.

The maintainer's pushback concentrates on (a) helper usage, (b) typing strictness, (c) docstring discipline + working `>>>` examples. Address these before requesting review.

---

## Pre-flight checklist

Run all of these locally before opening the PR:

```bash
# What CI's lint job runs:
pre-commit run --all-files

# Type-check explicitly (also runs in pre-commit):
poetry run dmypy run -- cognite tests

# Fast unit tests:
poetry run pytest tests/tests_unit -n4 --dist loadscope

# Doctests in your new module specifically:
poetry run pytest tests/tests_unit/test_docstring_examples.py -k <group> -v

# Sphinx warnings-as-errors build:
cd docs && make html SPHINXOPTS="-W --keep-going"

# Sync codegen explicit verify:
python scripts/sync_client_codegen/main.py verify
```

Item-by-item smoke:
- [ ] `_RESOURCE_PATH` set on the API class.
- [ ] `_CREATE_LIMIT` / `_DELETE_LIMIT` set if endpoint chunks.
- [ ] `FeaturePreviewWarning` instantiated **and** `.warn()` called as the first line of every public method (if alpha/beta).
- [ ] Every public method has Args/Returns/Examples docstring with at least one working `>>>` block.
- [ ] No `dict[str, Any]` in public signatures or DTO `__init__` args.
- [ ] No `Any` returns; `Literal[...]` for closed enums.
- [ ] No internal codenames (e.g. project codenames) anywhere user-facing.
- [ ] `_load` implemented on every DTO; `dump` overridden only when nested CogniteResources need recursion.
- [ ] Read/write split: `<X>` inherits `WriteableCogniteResource[<X>Write]`; both implement `as_write()`. Pure read-only API → inherit `CogniteResource` directly and skip the write half.
- [ ] Re-exports in the data-classes `__init__.py` and `__all__` (alphabetical).
- [ ] Parent group `__init__.py` (nested) or `_cognite_client.py` (flat) instantiates the new API.
- [ ] Docs-time import added in the `BUILD_COGNITE_SDK_DOCS` block of `_cognite_client.py`.
- [ ] Mocks registered in `testing.py` for async **and** sync (twice — once per *Mock class).
- [ ] Sync mirror regenerated and committed (no manual edits).
- [ ] Doctest registered in `test_docstring_examples.py`.
- [ ] Unit tests covering create/list/retrieve/delete + chunking proofs (if applicable).
- [ ] `test_api_client.py` parametric retry list updated.
- [ ] `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` updated if applicable.
- [ ] `docs/source/<group>.rst` adds API + data-classes section.
- [ ] PR title is Conventional Commit (`feat(<feature>): …`).
- [ ] **Don't** edit `CHANGELOG.md`.
- [ ] PR description matches the merged code (update as design evolves).


---

## Canonical reference: streams (PR #2534)

When you want to see a real, recently-merged example end-to-end:

- API class: `cognite/client/_api/data_modeling/streams.py` (~160 lines)
- DTOs: `cognite/client/data_classes/data_modeling/streams.py` (~217 lines)
- Sync mirror (auto-generated, do not edit): `cognite/client/_sync_api/data_modeling/streams.py`
- Tests: `tests/tests_unit/test_api/test_data_modeling/test_streams.py` (~149 lines)
- Wiring touchpoints: `cognite/client/_api/data_modeling/__init__.py:12,31` ; `cognite/client/_cognite_client.py:55` ; `cognite/client/testing.py:23,107,229,240,430,441` ; `cognite/client/data_classes/data_modeling/__init__.py:122-132,257-265` ; `cognite/client/utils/_url.py:37` ; `tests/tests_unit/test_docstring_examples.py:39,132` ; `docs/source/data_modeling.rst:289-302` ; `tests/tests_unit/test_api_client.py:1841-1843`

For a guided tour of how all those pieces fit together, see `references/streams-walkthrough.md`.

---

## References (load on demand)

- `references/dto-design.md` — DTO authoring deep dive: `<X>`/`<X>Write`/`<X>List` triple, `_load`/`dump` rules, `as_write()`, settings-as-typed-class pattern, when to nest.
- `references/wiring.md` — exhaustive walkthrough of the five wiring locations.
- `references/tests-and-doctests.md` — fixture pattern, `pytest_httpx`, doctest registration.
- `references/alpha-and-warnings.md` — `FeaturePreviewWarning`, beta routing, `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS`.
- `references/streams-walkthrough.md` — guided tour of the streams PR files.

