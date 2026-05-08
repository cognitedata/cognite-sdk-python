# DTO design deep dive

Authoritative example: `cognite/client/data_classes/data_modeling/streams.py`.
Base classes live in `cognite/client/data_classes/_base.py`.

This file focuses on **DTO authoring patterns specific to a new sub-API**.

## The `<X>` / `<X>Write` / `<X>List` triple

For any resource with a write path:

```python
class Stream(WriteableCogniteResource["StreamWrite"]):
    """One line. Args/Returns/Examples carry the load."""
    def __init__(
        self,
        external_id: str,
        created_time: int,
        created_from_template: str,
        type: Literal["Immutable", "Mutable"],
        settings: StreamSettings,
    ) -> None:
        self.external_id = external_id
        ...

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            created_from_template=resource["createdFromTemplate"],
            type=resource["type"],
            settings=StreamSettings._load(resource["settings"]),
        )

    def as_write(self) -> StreamWrite:
        return StreamWrite(
            external_id=self.external_id,
            settings=StreamTemplateWriteSettings(
                template=StreamTemplate(name=self.created_from_template),
            ),
        )


class StreamWrite(WriteableCogniteResource["StreamWrite"]):
    """One line."""
    def __init__(self, external_id: str, settings: StreamTemplateWriteSettings) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self: ...

    def as_write(self) -> StreamWrite:
        return self


class StreamList(CogniteResourceList[Stream], ExternalIDTransformerMixin):
    _RESOURCE = Stream
```

Key points:

- The **read class** parameterises `WriteableCogniteResource["<Write>"]` and its `as_write()` builds a fresh `<Write>` from its own fields.
- The **write class** also parameterises `WriteableCogniteResource["<Self>"]` and its `as_write()` returns `self`. This is the standard pattern.
- The **list class** subclasses `CogniteResourceList[<Read>]` and sets `_RESOURCE = <Read>`. Mix in `ExternalIDTransformerMixin` (`_base.py:841`) when items have `external_id` so users can call `list.as_external_ids()`.
- Use `WriteableCogniteResourceList` (`_base.py:455`) only when the list itself participates in a read/write pair (rare; streams does NOT).
- **Pure read-only API**: skip the write half entirely; inherit `CogniteResource` directly and don't add `as_write()`.

## Naming

`<X>` (read), `<X>Write` (write payload), `<X>List` (list container). Older areas use `<X>Apply` (`SpaceApply`) or `<X>Upsert` (`AgentUpsert`); for new code prefer `Write`.

No internal codenames. PR #2534 had to scrub internal product codenames from public docstrings before merge. **Never expose internal product names anywhere user-facing.**

## `_load` pattern

Every DTO implements `_load`. Required fields: `resource["…"]`. Optional fields: `resource.get("…")`. Nested CogniteResources: `<Nested>._load(resource["…"])`. `Optional[Nested]`: `<Nested>._load_if(resource.get("…"))` (provided by `_base.py:134-137`).

```python
@classmethod
def _load(cls, resource: dict[str, Any]) -> Self:
    return cls(
        external_id=resource["externalId"],
        consumed=resource.get("consumed"),
        settings=StreamSettings._load(resource["settings"]),
        meta=Meta._load_if(resource.get("meta")),
    )
```

## When to override `dump`

Default `dump()` (`_base.py:88-97`) calls `basic_instance_dump`, which handles primitives via `convert_all_keys_to_camel_case`. **Override only when the resource holds nested `CogniteResource` children** — the inherited dump won't recursively call `.dump()` on them.

Streams overrides `dump` on `StreamLimitSettings`, `StreamSettings`, `StreamTemplateWriteSettings`, `Stream`, `StreamWrite` (each has nested `CogniteResource` children). It does **not** override on `StreamLimit`, `StreamLifecycleSettings`, `StreamTemplate` (primitives only).

Round-1 review: "dump with primitive types is redundant" — three of four overrides got deleted. **Don't add `dump` overrides you don't need.**

Pattern when overriding:

```python
def dump(self, camel_case: bool = True) -> dict[str, Any]:
    out: dict[str, Any] = {
        "external_id": self.external_id,
        "settings": self.settings.dump(camel_case=camel_case),
    }
    return convert_all_keys_to_camel_case(out) if camel_case else out
```

## Settings/template pattern (the streams headline lesson)

PR #2534 originally shipped `StreamWrite.settings: dict[str, Any]`. Review caught this and commit `934d0e1` removed the dict overload. Final shape uses **typed classes all the way down**:

```python
class StreamWrite(WriteableCogniteResource["StreamWrite"]):
    def __init__(self, external_id: str, settings: StreamTemplateWriteSettings) -> None: ...

class StreamTemplateWriteSettings(CogniteResource):
    def __init__(self, template: StreamTemplate) -> None: ...

class StreamTemplate(CogniteResource):
    def __init__(self, name: str) -> None: ...
```

Each layer is its own `CogniteResource`. **Mandatory under `.gemini/styleguide.md` — no `dict[str, Any]` in public signatures.** When tempted to "just take a dict for now", don't.

## When to nest DTOs vs. flatten

If the API spec describes a sub-object with two-or-more fields, model it as a nested `CogniteResource` (named `<X><Suffix>`, e.g. `StreamLimitSettings`). If it's a single-field wrapper that's truly opaque (rare, and watch for spec drift), still model it — the round-trip cost is tiny and your users get autocomplete.

Review caught spec drift on `StreamTemplate.version` not being in the spec → field removed (commits `2c50de3`, `8cee723`). **Verify your fields match the spec; don't invent.**

## `Literal[...]` for closed enums

`Stream.type: Literal["Immutable", "Mutable"]` (added in round 2 review). Plain `str` was rejected. If the API has a closed set, type it as `Literal[...]`.

## `__eq__` / `__str__` / `_repr_html_`

`CogniteResource` already provides these (`_base.py:76-190`). Don't override unless you have a domain reason.

## Common pitfalls (what review will catch)

- `dict[str, Any]` in any public signature → **rewrite as a typed class**.
- `Any` return type → **rewrite**.
- `MutableSequence` where `Sequence` would do → **use `Sequence`**.
- `str` where `Literal[...]` would do → **use `Literal[...]`**.
- A `<X>DeleteItem` class for an endpoint that just takes external IDs → **drop it; `delete(external_id: str | SequenceNotStr[str])` is enough**.
- Bespoke utility functions in the API module → **move to `cognite/client/utils/...`** if truly needed; usually they're not.
- Class-level docstring that just restates the class name → **one informational line; load goes in `Args:`/`Returns:`/`Examples:`**.
