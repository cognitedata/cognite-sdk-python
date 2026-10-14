from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client.utils._text import to_camel_case

if TYPE_CHECKING:
    import httpx2


_T = TypeVar("_T")

REDACTED = "***"
SENSITIVE_HEADER_NAMES = frozenset({"authorization", "proxy-authorization"})

# We also need to do a "best effort" attempt at redacting payloads that were not produced by one of our data classes,
# since a user of course can pass a payload/dict straight to e.g. client.post(). What we look for here are words
# that should never turn up in a "harmless" CDF field name.
# Note: "token" is deliberately left out: it would also match e.g. 'tokenUrl' and 'tokenExchange' (neither are secrets).
ALWAYS_REDACT_REGEX = re.compile(r"secret|password|passphrase|nonce", re.IGNORECASE)


@dataclass(frozen=True)
class SensitiveFields:
    anywhere: frozenset[str]  # Field names that always mean 'credential'
    by_discriminator: Mapping[str, frozenset[str]]  # Field names we only redact inside a certain kind of object

    def is_sensitive(self, key: str, discriminator: Any = None) -> bool:
        """Answers the following: Should we hide the value stored under this field name?"""
        if key in self.anywhere or ALWAYS_REDACT_REGEX.search(key):
            return True
        if isinstance(discriminator, str):
            return key in self.by_discriminator.get(discriminator, frozenset())
        return False


@cache
def sensitive_fields() -> SensitiveFields:
    """Collect and cache the sensitive field declarations from every data class"""
    import cognite.client.data_classes  # noqa: F401
    from cognite.client.data_classes._base import CogniteResource
    from cognite.client.utils._auxiliary import all_subclasses

    anywhere: set[str] = set()
    by_discriminator: dict[str, set[str]] = {}

    for cls in all_subclasses(CogniteResource):  # type: ignore[type-abstract]
        if not cls._SENSITIVE_FIELDS:
            continue
        # We redact both camelCase- and the snake_case version for good measure:
        spellings = {spelling for field in cls._SENSITIVE_FIELDS for spelling in (field, to_camel_case(field))}
        if cls._SENSITIVE_TYPES:
            for type_value in cls._SENSITIVE_TYPES:
                by_discriminator.setdefault(type_value, set()).update(spellings)
        else:
            anywhere.update(spellings)

    return SensitiveFields(
        anywhere=frozenset(anywhere),
        by_discriminator={key: frozenset(value) for key, value in by_discriminator.items()},
    )


def redact_headers(headers: httpx2.Headers | Mapping[str, str]) -> dict[str, str]:
    redacted = dict(headers)
    for name in redacted:
        if name.lower() in SENSITIVE_HEADER_NAMES:
            redacted[name] = REDACTED
    return redacted


def redact(obj: _T) -> _T:
    """Copy obj and return with all known sensitive fields redacted."""
    return _redact(obj, sensitive_fields())


def _redact(value: Any, fields: SensitiveFields) -> Any:
    match value:
        case {}:  # note: this matches any mapping
            # An object's "type" is what tells us whether a generic name like 'value' is a credential
            discriminator = value.get("type")
            return {
                key: _redact_credential(item) if fields.is_sensitive(str(key), discriminator) else _redact(item, fields)
                for key, item in value.items()
            }
        case [*items]:
            return [_redact(item, fields) for item in items]
        case _:
            return value


def _redact_credential(value: Any) -> dict[str, str] | str:
    if isinstance(value, dict):
        return dict.fromkeys(value, REDACTED)
    return REDACTED


def redact_response_body(text: str, max_chars: int | None = None) -> str:
    """Redact a response body so that it is safe to log. Pass `max_chars` to also truncate it"""
    from cognite.client.utils import _json_extended as _json

    try:
        parsed = _json.loads(text)
    except Exception:  # ValueError should be enough, but we must ensure this never raises
        # We don't guess or attempt to redact non-JSON content. All CDF responses that contain sensitive
        # content would be in JSON format.
        return _shorten(text, max_chars)

    return _shorten(_json.dumps(redact(parsed)), max_chars)


def _shorten(text: str, max_chars: int | None) -> str:
    from cognite.client.utils._text import shorten

    if max_chars is None:
        return text
    return shorten(text, max_chars)
