from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cognite.client._constants import _RUNNING_IN_BROWSER
from cognite.client.utils import _json_extended as _json
from cognite.client.utils._time import timed_cache
from cognite.client.utils._url import resolve_url

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.data_classes import AssetHierarchy


class CogniteException(Exception):
    pass


class CogniteOrganizationError(CogniteException):
    """Raised when we cannot determine the organization of the current project."""

    def __str__(self) -> str:
        return "Could not look-up organization"


class CogniteProjectAccessError(CogniteException):
    """Raised when we get a 401 response from the API which means we don't have project access at all."""

    def __init__(
        self, project: str, x_request_id: str | None, maybe_projects: list[str] | None, cluster: str | None = None
    ) -> None:
        self.x_request_id = x_request_id
        self.cluster = cluster
        self.project = project
        self.maybe_projects = maybe_projects

    @staticmethod
    @timed_cache(ttl=5)  # Don't spam requests when using concurrency
    async def _attempt_to_get_projects(client: AsyncCogniteClient, current_project: str) -> list[str] | None:
        # To avoid an infinte loop, we can't just use client.iam.token.inspect(), but use http_client directly:
        api_client = client.iam.token
        _, full_url = resolve_url(api_client, "GET", "/api/v1/token/inspect")
        full_headers = api_client._configure_headers(additional_headers=None, api_subversion=api_client._api_version)
        try:
            token_inspect = await api_client._http_client("GET", full_url, headers=full_headers, timeout=5)
            projects = {proj["projectUrlName"] for proj in token_inspect.json()["projects"]} - {current_project}
            return sorted(projects)
        except Exception:
            return None

    def __str__(self) -> str:
        msg = (
            f"You don't have access to the requested CDF project={self.project!r}, reason: Unauthorized. "
            "The token may have expired or it was invalid."
        )
        if self.maybe_projects:
            msg += f". Did you intend to use one of: {self.maybe_projects}?"
        msg += f" | code: 401 | X-Request-ID: {self.x_request_id}"
        if self.cluster:
            msg += f" | cluster: {self.cluster}"
        return msg


@dataclass
class GraphQLErrorSpec:
    message: str
    hint: str | None
    kind: str | None
    location: dict[str, dict[str, int]] | None
    locations: list[dict[str, int]] | None  # yes, the api distinguishes on plurality....
    path: list[str] | None
    extensions: dict[str, str] | None

    def __repr__(self) -> str:
        attrs_string = f"message={self.message}"
        for attr in ("hint", "kind", "location", "locations", "path", "extensions"):
            if (value := getattr(self, attr)) is not None:
                attrs_string += f", {attr}={value}"
        return f"GraphQLErrorSpec({attrs_string})"

    @classmethod
    def load(cls, data: dict[str, Any]) -> GraphQLErrorSpec:
        return cls(
            message=data["message"],
            hint=data.get("hint"),
            kind=data.get("kind"),
            location=data.get("location"),
            locations=data.get("locations"),
            path=data.get("path"),
            extensions=data.get("extensions"),
        )


class CogniteGraphQLError(CogniteException):
    def __init__(self, errors: list[GraphQLErrorSpec]) -> None:
        self.errors = errors


class CogniteRequestError(CogniteException):
    pass


class CogniteConnectionError(CogniteRequestError):
    pass


class CogniteConnectionRefused(CogniteConnectionError):
    def __init__(self) -> None:
        super().__init__("Cognite API connection refused. Please try again later.")


class CogniteReadTimeout(CogniteRequestError):
    pass


class CogniteFileUploadError(CogniteException):
    def __init__(
        self,
        message: str,
        code: int,
    ) -> None:
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return f"{self.message} | code: {self.code}"


class CogniteMultiException(CogniteException):
    def __init__(
        self,
        successful: list | None = None,
        failed: list | None = None,
        unknown: list | None = None,
        skipped: list | None = None,
    ) -> None:
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self.skipped = skipped or []

    def _truncate_elements(self, lst: list) -> str:
        truncate_at = 10
        elements = ",".join(str(element) for element in lst[:truncate_at])
        if len(lst) > truncate_at:
            elements += ", ..."
        return f"[{elements}]"

    def _get_multi_exception_summary(self) -> str:
        if len(self.successful) == 0 and len(self.unknown) == 0 and len(self.failed) == 0 and len(self.skipped) == 0:
            return ""
        summary = [
            "",  # start string with newline
            "The API Failed to process some items:",
            f"- Successful (2xx): {len(self.successful)} item(s)",
            f"- Unknown (5xx): {len(self.unknown)} item(s)",
            f"- Failed (4xx): {len(self.failed)} item(s)",
        ]
        # Only show 'skipped' when tasks were skipped to avoid confusion:
        if skipped := self.skipped:
            summary.append(f"- Skipped: {len(skipped)} item(s)")
        return "\n".join(summary)


class CogniteAPIError(CogniteMultiException):
    """Cognite API Error

    Raised if a given request fails. If one or more of concurrent requests fails, this exception will also contain
    information about which items were successfully processed (2xx), which may have been processed (5xx), and which have
    failed to be processed (4xx).

    Args:
        message (str): The error message produced by the API.
        code (int): The error code produced by the failure.
        x_request_id (str | None): The request-id generated for the failed request.
        missing (list | None): List of missing identifiers.
        duplicated (list | None): List of duplicated identifiers.
        successful (list | None): List of items which were successfully processed.
        failed (list | None): List of items which failed.
        unknown (list | None): List of items which may or may not have been successfully processed.
        skipped (list | None): List of items that were skipped due to "fail fast" mode.
        cluster (str | None): Which Cognite cluster the user's project is on.
        project (str | None): No description.
        extra (dict | None): A dict of any additional information.
    """

    def __init__(
        self,
        message: str,
        code: int,
        x_request_id: str | None = None,
        missing: list | None = None,
        duplicated: list | None = None,
        successful: list | None = None,
        failed: list | None = None,
        unknown: list | None = None,
        skipped: list | None = None,
        cluster: str | None = None,
        project: str | None = None,
        extra: dict | None = None,
    ) -> None:
        self.message = message
        self.cluster = cluster
        self.code = code
        self.project = project
        self.x_request_id = x_request_id
        self.missing = missing
        self.duplicated = duplicated
        self.extra = extra
        super().__init__(successful, failed, unknown, skipped)

    def __str__(self) -> str:
        msg = self.message
        if self.missing:
            msg += f", missing: {self._truncate_elements(self.missing)}"
        elif self.duplicated:
            msg += f", duplicated: {self._truncate_elements(self.duplicated)}"

        msg += f" | code: {self.code} | X-Request-ID: {self.x_request_id}"
        if self.cluster:
            msg += f" | cluster: {self.cluster}"
        if self.project:
            msg += f" | project: {self.project}"

        msg += self._get_multi_exception_summary()
        if self.extra:
            pretty_extra = _json.dumps(self.extra, indent=4, sort_keys=True)
            msg += f"\nAdditional error info: {pretty_extra}"
        return msg


class CogniteNotFoundError(CogniteAPIError):
    """Cognite Not Found Error

    Raised if one or more of the referenced identifiers are not found.
    """

    def __init__(
        self,
        message: str,
        code: int,
        missing: list,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, code, **kwargs)
        self.missing: list = missing

    @property
    def not_found(self) -> list:
        return self.missing


class CogniteDuplicatedError(CogniteAPIError):
    """Cognite Duplicated Error

    Raised if one or more of the referenced identifiers have been duplicated in the request.
    """

    def __init__(
        self,
        message: str,
        code: int,
        duplicated: list,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, code, **kwargs)
        self.duplicated: list = duplicated


class CogniteImportError(CogniteException, ImportError):
    """Cognite Import Error

    Raised if the user attempts to use functionality which requires an uninstalled package.

    Args:
        module (str): Name of the module which could not be imported
        message (str | None): The error message to output.
    """

    def __init__(self, module: str, message: str | None = None) -> None:
        self.module = module
        self.message = message or f"The functionality you are trying to use requires '{self.module}' to be installed."

    def __str__(self) -> str:
        return self.message


class CogniteMissingClientError(CogniteException):
    """Cognite Missing Client Error

    Raised if the user attempts to make use of a method which requires the cognite_client being set, but it is not.

    Args:
        obj (Any): Object missing client reference.
    """

    def __init__(self, obj: Any) -> None:
        self.type = type(obj)

    def __str__(self) -> str:
        return (
            f"An AsyncCogniteClient has not been set on this object ({self.type}), did you create it yourself? "
            "Hint: You can pass an instantiated client along when you initialise the object."
        )


class CogniteAuthError(CogniteException): ...


class CogniteOAuthError(CogniteAuthError):
    def __init__(self, error: str, description: str) -> None:
        self.error = error
        self.description = description

    def __str__(self) -> str:
        return f"Error generating access token: {self.error!r}. Description: {self.description}"


class CogniteAssetHierarchyError(CogniteException):
    """Cognite Asset Hierarchy validation Error.

    Raised if the given assets form an invalid hierarchy (by CDF standards).

    Args:
        message (str): The error message to output.
        hierarchy (AssetHierarchy): An instance of AssetHierarchy that holds various groups
            of assets that failed validation.
    """

    def __init__(self, message: str, hierarchy: AssetHierarchy) -> None:
        self.message = message
        self._hierarchy = hierarchy

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self!s})"

    def __str__(self) -> str:
        msg = self.message.strip() + " Issue(s): "
        causes = ["duplicates", "invalid", "unsure_parents"]
        if not self._hierarchy._ignore_orphans:
            causes.append("orphans")

        # We count the number of issues (duplicates need to be counted separately):
        n_issues = [sum(map(len, self.duplicates.values())), *(len(getattr(self, cause)) for cause in causes[1:])]
        if sum(n_issues):
            return msg + ", ".join(f"{n} {cause}" for cause, n in zip(causes, n_issues) if n)
        return msg + f"{len(self.cycles)} cycles"

    def __getattr__(self, attr: str) -> Any:
        try:
            # We try to delegate failed attribute lookups to the underlying hierarchy:
            return getattr(self._hierarchy, attr)
        except AttributeError:
            # It is confusing if the error mentions 'AssetHierarchy':
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {attr!r}") from None


class CogniteModelFailedError(CogniteException):
    def __init__(self, typename: str, id: int, error_message: str) -> None:
        self.typename = typename
        self.id = id
        self.error_message = error_message

    def __str__(self) -> str:
        return f"{self.typename} {self.id} failed with error '{self.error_message}'"


class CogniteAuthorizationError(CogniteAPIError): ...


if _RUNNING_IN_BROWSER:
    from pyodide.ffi import JsException  # type: ignore [import-not-found]
else:

    class JsException(Exception):  # type: ignore [no-redef]
        ...


PyodideJsException = JsException
