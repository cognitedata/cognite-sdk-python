# Records API — Implementation Plan

## Confirmed decisions

| Decision | Choice |
|---|---|
| Client path | `client.data_modeling.records` |
| Preview warning | `api_maturity="General Availability", sdk_maturity="alpha"` |
| Aggregate DTOs | Full typed tree (recursive) |
| `sync()` shape | `RecordSyncIterator` — iterable, exposes `.cursor` after drain |
| Filter DSL | Reuse `Filter` from `data_classes/filters.py` |
| List method name | `list()` |
| Delete params | `Record | RecordWrite | Sequence[Record | RecordWrite]` |

---

## Files to create (2)

| File | Contents |
|---|---|
| `cognite/client/data_classes/data_modeling/records.py` | All DTOs |
| `cognite/client/_api/data_modeling/records.py` | `RecordsAPI` |

## Files to modify (~14)

| File | Change |
|---|---|
| `cognite/client/data_classes/data_modeling/__init__.py` | Re-export all public DTOs |
| `cognite/client/_api/data_modeling/__init__.py` | Instantiate `RecordsAPI`, add to `DataModelingAPI` |
| `cognite/client/_cognite_client.py` | Docs-time import in `BUILD_COGNITE_SDK_DOCS` block |
| `cognite/client/testing.py` | Mock async + sync `RecordsAPI` (×2) |
| `cognite/client/utils/_url.py` | Add ingest + upsert paths to `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` |
| `tests/tests_unit/test_api/test_data_modeling/test_records.py` | **New** — unit tests |
| `tests/tests_unit/test_docstring_examples.py` | Register doctest module |
| `tests/tests_unit/test_api_client.py` | Add retry classification entries |
| `docs/source/data_modeling.rst` | `autosummary` + `automodule` sections |
| `cognite/client/_sync_api/data_modeling/records.py` | **Generated** by sync codegen (do not hand-edit) |

---

## DTO design (`data_classes/data_modeling/records.py`)

### Write-side

```python
class RecordSourceReference(CogniteObject):      # {type, space, externalId}
class RecordSource(CogniteObject):                # {source: RecordSourceReference, properties: dict}
class RecordWrite(CogniteResource):               # {space, externalId, sources: list[RecordSource]}
class RecordWriteList(CogniteResourceList[RecordWrite])
```

### Read-side

```python
class Record(WriteableCogniteResource[RecordWrite]):
    # {space, externalId, createdTime, lastUpdatedTime, properties: {space: {container: {prop: val}}}}
    # as_write() reconstructs RecordWrite from nested properties
class RecordList(WriteableCogniteResourceList[RecordWrite, Record])
```

### Sync-side

```python
class SyncRecord(Record):                         # adds status: Literal["created","updated","deleted"]
class SyncRecordList(CogniteResourceList[SyncRecord])
class RecordSyncIterator:                         # wraps generator, exposes .cursor after drain
```

### Shared request helpers

```python
class TimeRange(CogniteObject):                   # gte/gt/lte/lt (str|int)
class SourceSelector(CogniteObject):              # {source: RecordSourceReference, properties: list[str]}
class SortSpec(CogniteObject):                    # {property: list[str], direction}
class TargetUnits(CogniteObject):                 # unit conversion (unitSystemName or properties list)
```

### Aggregate DTO tree (full typed, ~150 lines)

```python
# Metric aggregates (scalar result):
class AvgAggregate, CountAggregate, MinAggregate, MaxAggregate, SumAggregate

# Bucket aggregates (recursive sub-aggregates):
class UniqueValuesAggregate                       # property, aggregates?
class TimeHistogramAggregate                      # property, calendarInterval|fixedInterval, hardBounds?, aggregates?
class NumberHistogramAggregate                    # property, interval, hardBounds?, aggregates?
class FiltersAggregate                            # filters: list[Filter], aggregates?
class NumberHistogramHardBounds                   # min?, max?

# TypeAlias — avoids ABC overhead:
AggregateSpec: TypeAlias = AvgAggregate | CountAggregate | ... | FiltersAggregate
AggregateResult: TypeAlias = dict[str, Any]       # parsed response bucket/scalar
```

---

## API class — method signatures (`_api/data_modeling/records.py`)

```python
class RecordsAPI(APIClient):
    # No class-level _RESOURCE_PATH — parameterized per call: f"/streams/{stream_id}/records"
    # FeaturePreviewWarning(api_maturity="General Availability", sdk_maturity="alpha", feature_name="Records")

    async def ingest(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        # POST /streams/{id}/records — 202 empty body
        # Chunks in 1000 using split_into_chunks + self._post (can't use _create_multiple — no response body)

    async def upsert(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        # POST /streams/{id}/records/upsert — 202 empty body, mutable only
        # Same chunking approach

    async def delete(self, stream_id: str, items: Record | RecordWrite | Sequence[Record | RecordWrite]) -> None:
        # POST /streams/{id}/records/delete — 200 empty body, mutable only
        # Extract space+externalId from items; chunk in 1000

    async def list(
        self,
        stream_id: str,
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        sources: Sequence[SourceSelector] | None = None,
        sort: Sequence[SortSpec] | None = None,
        limit: int | None = 25,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> RecordList:
        # POST /streams/{id}/records/filter — single page, no cursor

    def sync(
        self,
        stream_id: str,
        *,
        cursor: str | None = None,
        initialize_cursor: str | None = None,
        filter: Filter | None = None,
        sources: Sequence[SourceSelector] | None = None,
        limit: int | None = None,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> RecordSyncIterator:
        # Returns RecordSyncIterator (iterable over SyncRecord, has .cursor attribute)
        # Internally loops while hasNext=True, then exhausts

    async def aggregate(
        self,
        stream_id: str,
        aggregates: dict[str, AggregateSpec],
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> dict[str, AggregateResult]:
        # POST /streams/{id}/records/aggregate
```

---

## Tricky implementation notes

1. **`_RESOURCE_PATH` is not a class constant** — every method builds `f"/streams/{stream_id}/records"` inline. This means we don't get `_create_multiple`'s auto-chunking. Use `split_into_chunks` from `cognite/client/utils/_auxiliary.py` + `self._post`.

2. **Ingest/upsert/delete all return empty bodies** — return `None`. The SDK's normal helpers (`_create_multiple`, `_delete_multiple`) expect response bodies. Use `self._post` directly.

3. **Delete is POST, not HTTP DELETE** — `_delete_multiple` sends HTTP `DELETE`. For records, it's `POST .../delete`. Use `self._post` directly.

4. **`NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS`** — needs entries for paths matching `.*/streams/.*/records$` and `.*/streams/.*/records/upsert$`. Check the existing entry format (streams uses a prefix string; may need regex or a wildcard).

5. **`RecordSyncIterator.cursor`** — the sync loop consumes pages until `hasNext=False`, then sets `self.cursor = last_cursor`. Caller saves it for the next call.

6. **`as_write()` on `Record`** — reconstructs `RecordWrite` from `properties: {space: {container: {prop: val}}}` by iterating the nested structure and building `sources: [{source: {type, space, externalId}, properties: {prop: val}}]`.

7. **`SyncRecord.properties` can be `None`** for `status="deleted"` tombstones — override `_load` to handle missing key.

8. **Aggregate response** is `{aggregates: {id: bucket_or_metric_result}}` — the result dict value type varies by aggregate type. `AggregateResult: TypeAlias = dict[str, Any]` is the pragmatic call since each bucket type has a completely different shape.

---

## Iteration order (lessons from streams PR #2534)

1. DTOs first — get types right before API methods
2. API class using `self._post` from the start — no helpers to retrofit later
3. Docstrings + `>>>` examples before unit tests
4. Wiring (DataModelingAPI, `_cognite_client.py`, `testing.py`, `data_modeling.rst`, doctest registration) all together
5. Unit tests
6. Sync codegen **last** — only after all async signatures are stable
7. `NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS` + `test_api_client.py` entry

---

## API reference (cleaned from OpenAPI spec)

### Shared concepts

- **Path prefix:** `/api/20230101/projects/{project}/streams/{streamId}/records/...`
- **ACLs:** Write endpoints need `StreamRecordsAcl:WRITE`; read endpoints need `StreamRecordsAcl:READ` + `DataModelsAcl:READ`
- **Record identity:** `space` + `externalId` pair
- **Immutable vs mutable streams:**
  - Immutable: `lastUpdatedTime` range **required** on list/aggregate; upsert and delete are not allowed (422)
  - Mutable: duplicate `space+externalId` within a batch is rejected (422)
- **Property path format:** 3-element array `[space, containerExternalId, propertyId]`; top-level fields use 1-element arrays
- **`sources` selector:** `[{source: {type: "container", space, externalId}, properties: ["*" | propId, ...]}]` (1–10 items)
- **Partial success:** Ingest/upsert/delete are non-transactional; `error.partial` lists per-record outcomes on 500/503
- **Response size limit:** 20 MB max for list responses

### 1. Ingest (`POST /streams/{id}/records`)

- **Request:** `{items: array[1..1000]}` where each item has `space`, `externalId`, `sources`
- **Response:** 202 empty body
- **409:** Some mutable records already exist — `error.partial` lists per-record outcomes
- **422:** Duplicate `space+externalId` pairs in request (mutable), or request > 10 MB
- **Immutable deduplication:** Records where all fields are identical are deduplicated (eventually consistent)

### 2. Upsert (`POST /streams/{id}/records/upsert`)

- **Request:** Identical schema to ingest
- **Response:** 202 empty body
- **Mutable only:** 422 if attempted on immutable stream

### 3. Delete (`POST /streams/{id}/records/delete`)

- **Request:** `{items: array[1..1000]}` where each item has `space`, `externalId`
- **Response:** 200 empty body
- **Mutable only:** 422 if attempted on immutable stream
- **`ignoreUnknownIds=true` implicit:** Unknown IDs do not cause failure
- **Tombstones on sync:** Deleted records appear in sync with `status: "deleted"` for at least 3 days

### 4. List/filter (`POST /streams/{id}/records/filter`)

- **Request:**

  | Field | Type | Required | Notes |
  |---|---|---|---|
  | `lastUpdatedTime` | TimeRange | conditional | Required for immutable streams; needs at least one lower bound |
  | `filter` | Filter DSL | no | max 100 nodes, depth 10 |
  | `sources` | array[1..10] | no | Which container properties to return |
  | `sort` | array[1..5] | no | `{property, direction?}` |
  | `limit` | int | no | 1–1000, default 10 |
  | `targetUnits` | object | no | Unit conversion |
  | `includeTyping` | bool | no | |

- **Response:** `{items: array[Record], typing?: {...}}`
- **No cursor** — pagination requires chunking by `lastUpdatedTime` range manually
- **Filter DSL operators:** `and`, `or`, `not`, `matchAll`, `equals`, `range`, `in`, `prefix`, `exists`, `hasData`, `containsAll`, `containsAny`

### 5. Sync (`POST /streams/{id}/records/sync`)

- **Request:**

  | Field | Type | Required | Notes |
  |---|---|---|---|
  | `cursor` | string | conditional | Resume from previous cursor |
  | `initializeCursor` | string | conditional | Starting point if no cursor (e.g. `"7d-ago"`, `"400h-ago"`) |
  | `filter` | Filter DSL | no | |
  | `sources` | array[1..10] | no | |
  | `limit` | int | no | 1–1000, default 10 |
  | `targetUnits` | object | no | |
  | `includeTyping` | bool | no | |

- **Response:** `{items: array[SyncRecord], nextCursor: string, hasNext: bool, typing?: {...}}`
- **`hasNext=true`:** More records available — keep polling with `nextCursor`
- **SyncRecord** has additional `status: "created" | "updated" | "deleted"`; `properties` is absent for deleted records

### 6. Aggregate (`POST /streams/{id}/records/aggregate`)

- **Request:**

  | Field | Type | Required | Notes |
  |---|---|---|---|
  | `aggregates` | dict[id, AggregateSpec] | yes | 1–5 top-level entries |
  | `lastUpdatedTime` | TimeRange | conditional | Required for immutable streams |
  | `filter` | Filter DSL | no | |
  | `targetUnits` | object | no | |
  | `includeTyping` | bool | no | |

- **Metric aggregates:** `avg`, `count`, `min`, `max`, `sum` — each takes `{property: [space, container, propId]}`
- **Bucket aggregates:** `uniqueValues`, `timeHistogram`, `numberHistogram`, `filters` — each supports nested `aggregates` (recursive)
- **`timeHistogram`:** requires `calendarInterval` (`"1s"`, `"1m"`, `"1h"`, `"1d"`, `"1w"`, `"1M"`, `"1q"`, `"1y"`) OR `fixedInterval` (e.g. `"42m"`)
- **`numberHistogram`:** requires `interval: float`
- **`filters`:** `{filters: array[1..10 of Filter], aggregates?}` — max 30 filter buckets total
- **Aggregate ID constraints:** no `.`, not `_count` or `_bucket_count`, pattern `^[^\[\]>.]{1,255}$`
- **Response:** `{aggregates: {id: result}}` where result is a scalar or bucket array depending on type
