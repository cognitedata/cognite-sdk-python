from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urljoin

if TYPE_CHECKING:
    from cognite.client.config import ClientConfig


NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS: tuple[str, ...] = (
    "annotations",
    "assets",
    "context/entitymatching",
    "datasets",
    "documents",
    "events",
    "extpipes",
    "extpipes/config",
    "extpipes/runs",
    "files",
    "functions",
    "functions/[^/]+/call",
    "functions/schedules",
    "geospatial",
    "geospatial/crs",
    "geospatial/featuretypes",
    "geospatial/featuretypes/[^/]+/features",
    "hostedextractors",
    "labels",
    "postgresgateway",
    "profiles",
    "raw/dbs$",
    "raw/dbs/[^/]+/tables$",
    "relationships",
    "sequences",
    "simulators",
    "simulators/models",
    "simulators/models/revisions",
    "simulators/models/routines",
    "simulators/models/routines/revisions",
    "timeseries",
    "transformations",
    "transformations/schedules",
    "3d/models",
    "3d/models/[^/]+/revisions",
    "3d/models/[^/]+/revisions/[^/]+/mappings",
    "3d/models/[^/]+/revisions/[^/]+/nodes",
)
NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN: re.Pattern[str] = re.compile(
    r"|".join(
        rf"^/{path}(\?.*)?$"
        for path in (
            f"({r'|'.join(NON_RETRYABLE_CREATE_DELETE_RESOURCE_PATHS)})(/delete)?$",
            "ai/tools/documents/task",
            "annotations/suggest",
            "extpipes/config/revert",
            "transformations/cancel",
            "transformations/notifications",
            "transformations/run",
        )
    )
)
VALID_URL_PATTERN = re.compile(r"^https?://[a-z\d.:\-]+(?:/api/v1/projects/[^/]+)?((/[^\?]+)?(\?.+)?)")
VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"}


def resolve_url(method: str, url_path: str, api_version: str | None, config: ClientConfig) -> tuple[bool, str]:
    if not url_path.startswith("/"):
        raise ValueError("URL path must start with '/'")

    full_url = get_base_url_with_base_path(api_version, config) + url_path
    is_retryable = validate_url_and_return_retryability(method, full_url)
    return is_retryable, full_url


def validate_url_and_return_retryability(method: str, full_url: str) -> bool:
    if method not in VALID_METHODS:
        raise ValueError(f"Method {method} is not valid. Must be one of {VALID_METHODS}")

    if valid_url := VALID_URL_PATTERN.match(full_url):
        is_retryable = can_be_retried(method, valid_url.group(1))
        return is_retryable

    raise ValueError(f"URL {full_url} is not valid. Cannot resolve whether or not it is retryable")


def get_base_url_with_base_path(api_version: str | None, config: ClientConfig) -> str:
    if api_version:
        return urljoin(config.base_url, f"/api/{api_version}/projects/{config.project}")
    return config.base_url


def can_be_retried(method: str, path: str) -> bool:
    match method:
        case "GET" | "PUT" | "PATCH":
            return True
        case "POST" if not NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN.match(path):
            return True
        case _:
            return False


def interpolate_and_url_encode(path: str, *args: Any) -> str:
    return path.format(*(quote(str(arg), safe="") for arg in args))
