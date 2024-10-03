from __future__ import annotations

import reprlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cognite.client._constants import _RUNNING_IN_BROWSER
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import no_op

if TYPE_CHECKING:
    from cognite.client.data_classes import AssetHierarchy


class CogniteException(Exception):
    pass


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


class CogniteConnectionError(CogniteException):
    pass


class CogniteConnectionRefused(CogniteConnectionError):
    def __str__(self) -> str:
        return "Cognite API connection refused. Please try again later."


class CogniteReadTimeout(CogniteException):
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
        unwrap_fn: Callable = no_op,
    ) -> None:
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self.skipped = skipped or []
        self._unwrap_fn = unwrap_fn

    def _unwrap_list(self, lst: list) -> list:
        return [self._unwrap_fn(elem) for elem in lst]

    def _truncate_elements(self, lst: list) -> str:
        truncate_at = 10
        elements = ",".join([str(element) for element in lst[:truncate_at]])
        if len(elements) > truncate_at:
            elements += ", ..."
        return f"[{elements}]"

    def _get_multi_exception_summary(self) -> str:
        if len(self.successful) == 0 and len(self.unknown) == 0 and len(self.failed) == 0 and len(self.skipped) == 0:
            return ""
        summary = [
            "",  # start string with newline
            "The API Failed to process some items.",
            f"Successful (2xx): {self._truncate_elements(self._unwrap_list(self.successful))}",
            f"Unknown (5xx): {self._truncate_elements(self._unwrap_list(self.unknown))}",
            f"Failed (4xx): {self._truncate_elements(self._unwrap_list(self.failed))}",
        ]
        # Only show 'skipped' when tasks were skipped to avoid confusion:
        if skipped := self._unwrap_list(self.skipped):
            summary.append(f"Skipped: {skipped}")
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
        missing (list | None): (List) List of missing identifiers.
        duplicated (list | None): (List) List of duplicated identifiers.
        successful (list | None): List of items which were successfully processed.
        failed (list | None): List of items which failed.
        unknown (list | None): List of items which may or may not have been successfully processed.
        skipped (list | None): List of items that were skipped due to "fail fast" mode.
        unwrap_fn (Callable): Function to extract identifier from the Cognite resource.
        cluster (str | None): Which Cognite cluster the user's project is on.
        extra (dict | None): A dict of any additional information.

    Examples:
        Catching an API-error and handling it based on the error code::

            from cognite.client import CogniteClient
            from cognite.client.exceptions import CogniteAPIError

            client = CogniteClient()

            try:
                client.iam.token.inspect()
            except CogniteAPIError as e:
                if e.code == 401:
                    print("You are not authorized")
                elif e.code == 400:
                    print("Something is wrong with your request")
                elif e.code == 500:
                    print(f"Something went terribly wrong. Here is the request-id: {e.x_request_id}"
                print(f"The message returned from the API: {e.message}")
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
        unwrap_fn: Callable = no_op,
        cluster: str | None = None,
        extra: dict | None = None,
    ) -> None:
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.missing = missing
        self.duplicated = duplicated
        self.cluster = cluster
        self.extra = extra
        super().__init__(successful, failed, unknown, skipped, unwrap_fn)

    def __str__(self) -> str:
        msg = f"{self.message} | code: {self.code} | X-Request-ID: {self.x_request_id}"
        if self.cluster:
            msg += f" | cluster: {self.cluster}"
        if self.missing:
            msg += f"\nMissing: {self._truncate_elements(self.missing)}"
        if self.duplicated:
            msg += f"\nDuplicated: {self._truncate_elements(self.duplicated)}"
        msg += self._get_multi_exception_summary()
        if self.extra:
            pretty_extra = _json.dumps(self.extra, indent=4, sort_keys=True)
            msg += f"\nAdditional error info: {pretty_extra}"
        return msg


class CogniteNotFoundError(CogniteMultiException):
    """Cognite Not Found Error

    Raised if one or more of the referenced ids/external ids are not found.

    Args:
        not_found (list): The ids not found.
        successful (list | None): List of items which were successfully processed.
        failed (list | None): List of items which failed.
        unknown (list | None): List of items which may or may not have been successfully processed.
        skipped (list | None): List of items that were skipped due to "fail fast" mode.
        unwrap_fn (Callable): No description.
    """

    def __init__(
        self,
        not_found: list,
        successful: list | None = None,
        failed: list | None = None,
        unknown: list | None = None,
        skipped: list | None = None,
        unwrap_fn: Callable = no_op,
    ) -> None:
        self.not_found = not_found
        super().__init__(successful, failed, unknown, skipped, unwrap_fn)

    def __str__(self) -> str:
        if len(not_found := self.not_found) > 200:
            not_found = reprlib.repr(self.not_found)  # type: ignore [assignment]
        return f"Not found: {not_found}{self._get_multi_exception_summary()}"


class CogniteDuplicatedError(CogniteMultiException):
    """Cognite Duplicated Error

    Raised if one or more of the referenced ids/external ids have been duplicated in the request.

    Args:
        duplicated (list): The duplicated ids.
        successful (list | None): List of items which were successfully processed.
        failed (list | None): List of items which failed.
        unknown (list | None): List of items which may or may not have been successfully processed.
        skipped (list | None): List of items that were skipped due to "fail fast" mode.
        unwrap_fn (Callable): Function to extract identifier from the Cognite resource.
    """

    def __init__(
        self,
        duplicated: list,
        successful: list | None = None,
        failed: list | None = None,
        unknown: list | None = None,
        skipped: list | None = None,
        unwrap_fn: Callable = no_op,
    ) -> None:
        self.duplicated = duplicated
        super().__init__(successful, failed, unknown, skipped, unwrap_fn)

    def __str__(self) -> str:
        msg = f"Duplicated: {self.duplicated}"
        msg += self._get_multi_exception_summary()
        return msg


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
            f"A CogniteClient has not been set on this object ({self.type}), did you create it yourself? "
            "Hint: You can pass an instantiated client along when you initialise the object."
        )


class CogniteAuthError(CogniteException): ...


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


class ModelFailedException(Exception):
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
