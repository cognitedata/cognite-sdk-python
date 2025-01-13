from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urljoin

if TYPE_CHECKING:
    from cognite.client.config import ClientConfig


RETRYABLE_POST_ENDPOINT_REGEX_PATTERN: re.Pattern[str] = re.compile(
    "|".join(
        rf"^/{path}(\?.*)?$"
        for path in (
            "(assets|events|files|timeseries|sequences|datasets|relationships|labels)/(list|byids|search|aggregate)",
            "files/downloadlink",
            "timeseries/(data(/(list|latest|delete))?|synthetic/query)",
            "sequences/data(/(list|delete))?",
            "raw/dbs/[^/]+/tables/[^/]+/rows(/delete)?",
            "context/entitymatching/(byids|list|jobs)",
            "sessions/revoke",
            "models/.*",
            ".*/graphql",
            "units/.*",
            "annotations/(list|byids|reverselookup)",
            r"functions/(list|byids|status|schedules/(list|byids)|\d+/calls/(list|byids))",
            r"3d/models/\d+/revisions/\d+/(mappings/list|nodes/(list|byids))",
            "documents/(aggregate|list|search)",
            "profiles/(byids|search)",
            "geospatial/(compute|crs/byids|featuretypes/(byids|list))",
            "geospatial/featuretypes/[A-Za-z][A-Za-z0-9_]{0,31}/features/(aggregate|list|byids|search|search-streaming|[A-Za-z][A-Za-z0-9_]{0,255}/rasters/[A-Za-z][A-Za-z0-9_]{0,31})",
            "transformations/(filter|byids|jobs/byids|schedules/byids|query/run)",
            "simulators/list",
            "extpipes/(list|byids|runs/list)",
            "workflows/.*",
            "hostedextractors/.*",
            "postgresgateway/.*",
            "context/diagram/.*",
            "ai/tools/documents/(summarize|ask)",
        )
    )
)
VALID_URL_PATTERN = re.compile(  # TODO: Remove playground?
    r"^https?://[a-z\d.:\-]+(?:/api/(?:v1|playground)/projects/[^/]+)?((/[^\?]+)?(\?.+)?)"
)
VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"}


def resolve_url(method: str, url_path: str, api_version: str | None, config: ClientConfig) -> tuple[bool, str]:
    if not url_path.startswith("/"):
        raise ValueError("URL path must start with '/'")
    elif method not in VALID_METHODS:
        raise ValueError(f"Method {method} is not valid. Must be one of {VALID_METHODS}")

    full_url = get_base_url_with_base_path(api_version, config) + url_path
    if valid_url := VALID_URL_PATTERN.match(full_url):
        is_retryable = can_be_retried(method, valid_url.group(1))
        return is_retryable, full_url

    raise ValueError(f"URL {full_url} is not valid. Cannot resolve whether or not it is retryable")


def get_base_url_with_base_path(api_version: str | None, config: ClientConfig) -> str:
    if api_version:
        return urljoin(config.base_url, f"/api/{api_version}/projects/{config.project}")
    return config.base_url


def can_be_retried(method: str, path: str) -> bool:
    match method:
        case "GET" | "PUT" | "PATCH":
            return True
        case "POST" if RETRYABLE_POST_ENDPOINT_REGEX_PATTERN.match(path):
            return True
        case _:
            return False


def interpolate_and_url_encode(path: str, *args: Any) -> str:
    return path.format(*[quote(str(arg), safe="") for arg in args])
