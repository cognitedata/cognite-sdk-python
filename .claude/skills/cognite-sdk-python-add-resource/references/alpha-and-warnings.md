# Alpha/beta gating, FeaturePreviewWarning, and retry classification

## `FeaturePreviewWarning`

Source: `cognite/client/utils/_experimental.py:7-43`. Subclass of `FutureWarning`, picklable (overrides `__reduce__` because `APIClient` instances get pickled across xdist workers).

```python
class FeaturePreviewWarning(FutureWarning):
    def __init__(
        self,
        api_maturity: Literal["alpha", "beta", "General Availability"],
        sdk_maturity: Literal["alpha", "beta"],
        feature_name: str,
        pluralize: bool = False,
    ): ...
    def warn(self) -> None: ...    # emits via warnings.warn(self, stacklevel=2)
```

`pytest.ini` filters this warning class to `ignore`, so raising one is free in tests. The warning is real at runtime and shows up to end users.

## Two independent dials: `api_maturity` and `sdk_maturity`

They are deliberately decoupled.

| `api_maturity` | Meaning |
| --- | --- |
| `"alpha"` | Backend itself unstable; expect endpoint-shape changes. |
| `"beta"` | Backend stable-ish, will get a `DeprecationWarning` before breakages. |
| `"General Availability"` | Backend is GA. |

| `sdk_maturity` | Meaning |
| --- | --- |
| `"alpha"` | Python interface itself may change — we want freedom to redesign DTOs/method signatures. |
| `"beta"` | Python interface mostly stable. |

### Streams: GA API + alpha SDK

```python
self._warning = FeaturePreviewWarning(
    api_maturity="General Availability",
    sdk_maturity="alpha",
    feature_name="Streams",
)
```

Suggested in round-2 review (`reviews_and_comments.md` L92): "lets add a 'SDK in alpha' warning to the StreamsAPI itself to give ourselves some freedom in changing/modifying the SDK API design for some time." Adopted verbatim.

### Agents: beta API + alpha SDK + beta routing

`cognite/client/_api/agents/agents.py:28-29`:

```python
class AgentsAPI(APIClient):
    def __init__(self, ...) -> None:
        super().__init__(...)
        self._api_subversion = "beta"   # routes requests to /api/beta/...
        self._warnings = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Agent",
        )
```

Note: `_api_subversion = "beta"` is what makes the SDK hit the beta route prefix. Set it when the endpoint lives at `/api/beta/...`.

## Where to instantiate

In the API class `__init__`, after `super().__init__(...)`, alongside any `_CREATE_LIMIT`/`_DELETE_LIMIT`:

```python
def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
    super().__init__(config, api_version, cognite_client)
    self._CREATE_LIMIT = 1
    self._DELETE_LIMIT = 1
    self._warning = FeaturePreviewWarning(
        api_maturity="General Availability", sdk_maturity="alpha", feature_name="Streams"
    )
```

Attribute name: streams uses `_warning`, agents uses `_warnings` (plural). **Pick one and stay consistent within your file.** The name is local; nothing depends on the spelling.

## Where to call `.warn()`

**First line of every public method body.** Not in `__init__`. Not in helpers.

```python
async def list(self) -> StreamList:
    """..."""
    self._warning.warn()
    return await self._list(method="GET", list_cls=StreamList, resource_cls=Stream)
```

Every public-facing entry point — `create`, `list`, `retrieve`, `delete`, `update`, etc. — should warn once per call. Internal helpers (anything with a leading underscore) don't.

## When NOT to use FeaturePreviewWarning

GA SDK + GA API features: don't add it. Mature features (`assets.py`, `events.py`, `time_series.py`) have no preview warning. Adding one to a stable feature spams users for no reason.

## `feature_name`

The public-facing display name: `"Streams"`, not `"streams"`. `"Agent"` (singular), not `"AgentsAPI"`. Set `pluralize=True` only if the warning text reads better with the plural form ("Streams are in alpha" vs "Stream is in alpha").

---

## Retry classification — `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS`

File: `cognite/client/utils/_url.py`.

Add the resource path (the `_RESOURCE_PATH` minus the leading slash) to `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` only when **create/delete on the server are not idempotent** — i.e. retrying the same POST may produce duplicates or fail. Streams adds `"streams"` (line 37):

```python
NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS: tuple[str, ...] = (
    "annotations",
    "assets",
    ...
    "streams",
    ...
)
```

The matching regex (`NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN`, lines 51-64) uses `^/<path>(/delete)?(\?.*)?$`. The `(\?.*)?$` was added in commit `11ce01c` so that `_list`'s injected `?limit=...` query string still classifies correctly.

### When in doubt — leave it off

Default is "retryable". The only time you absolutely need to register here is when **the server cannot tolerate duplicate POSTs**. The author's review comment claimed streams was added "since we now use `_list`", but the actual reason is server-side non-idempotency on POST.

### Test entry

If you add a path here, also add a parametric entry in `tests/tests_unit/test_api_client.py` (around lines 1841-1843):

```python
("POST", "https://api.cognitedata.com/api/v1/projects/bla/<feature>", False),
("POST", "https://api.cognitedata.com/api/v1/projects/bla/<feature>/delete", False),
```

`False` = not retryable. The matching test exercises the regex against the actual URL shape `_list` produces (with the optional `?limit=` suffix).

## Beta API routing

If your endpoint lives at `/api/beta/...` rather than `/api/v1/...`:

```python
class <Feature>API(APIClient):
    _RESOURCE_PATH = "/<feature>"

    def __init__(self, ...) -> None:
        super().__init__(...)
        self._api_subversion = "beta"
        ...
```

`_api_subversion = "beta"` switches the URL prefix from `/api/v1/projects/<project>/<feature>` to `/api/beta/projects/<project>/<feature>`. See `AgentsAPI` for the live pattern.
