from __future__ import annotations

import json
import reprlib
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Sequence

if TYPE_CHECKING:
    from cognite.client.data_classes import AssetHierarchy


class CogniteException(Exception):
    pass


class CogniteConnectionError(CogniteException):
    pass


class CogniteConnectionRefused(CogniteConnectionError):
    def __str__(self) -> str:
        return "Cognite API connection refused. Please try again later."


class CogniteReadTimeout(CogniteException):
    pass


class CogniteMultiException(CogniteException):
    def __init__(
        self, successful: Sequence = None, failed: Sequence = None, unknown: Sequence = None, unwrap_fn: Callable = None
    ):
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self._unwrap_fn = unwrap_fn or (lambda x: x)

    def _get_multi_exception_summary(self) -> str:
        if len(self.successful) == 0 and len(self.unknown) == 0 and len(self.failed) == 0:
            return ""
        return "\n".join(
            (
                "",  # start string with newline
                "The API Failed to process some items.",
                f"Successful (2xx): {list(map(self._unwrap_fn, self.successful))}",
                f"Unknown (5xx): {list(map(self._unwrap_fn, self.unknown))}",
                f"Failed (4xx): {list(map(self._unwrap_fn, self.failed))}",
            )
        )


class CogniteAPIError(CogniteMultiException):
    """Cognite API Error

    Raised if a given request fails. If one or more of concurrent requests fails, this exception will also contain
    information about which items were successfully processed (2xx), which may have been processed (5xx), and which have
    failed to be processed (4xx).

    Args:
        message (str): The error message produced by the API.
        code (int): The error code produced by the failure.
        x_request_id (str): The request-id generated for the failed request.
        missing: (List) List of missing identifiers.
        duplicated: (List) List of duplicated identifiers.
        successful (List): List of items which were successfully processed.
        failed (List): List of items which failed.
        unknown (List): List of items which may or may not have been successfully processed.
        unwrap_fn: (Callable): Function to extract identifier from the Cognite resource.
        extra (Dict): A dict of any additional information.

    Examples:
        Catching an API-error and handling it based on the error code::

            from cognite.client import CogniteClient
            from cognite.client.exceptions import CogniteAPIError

            c = CogniteClient()

            try:
                c.iam.token.inspect()
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
        x_request_id: str = None,
        missing: Sequence = None,
        duplicated: Sequence = None,
        successful: Sequence = None,
        failed: Sequence = None,
        unknown: Sequence = None,
        unwrap_fn: Callable = None,
        extra: Dict = None,
    ) -> None:
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.missing = missing
        self.duplicated = duplicated
        self.extra = extra
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self) -> str:
        msg = f"{self.message} | code: {self.code} | X-Request-ID: {self.x_request_id}"
        if self.missing:
            msg += f"\nMissing: {self.missing}"
        if self.duplicated:
            msg += f"\nDuplicated: {self.duplicated}"
        msg += self._get_multi_exception_summary()
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            msg += f"\nAdditional error info: {pretty_extra}"
        return msg


class CogniteNotFoundError(CogniteMultiException):
    """Cognite Not Found Error

    Raised if one or more of the referenced ids/external ids are not found.

    Args:
        not_found (List): The ids not found.
        successful (List): List of items which were successfully processed.
        failed (List): List of items which failed.
        unknown (List): List of items which may or may not have been successfully processed.
    """

    def __init__(
        self,
        not_found: List,
        successful: List = None,
        failed: List = None,
        unknown: List = None,
        unwrap_fn: Callable = None,
    ):
        self.not_found = not_found
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self) -> str:
        if len(not_found := self.not_found) > 200:
            not_found = reprlib.repr(self.not_found)  # type: ignore [assignment]
        return f"Not found: {not_found}{self._get_multi_exception_summary()}"


class CogniteDuplicatedError(CogniteMultiException):
    """Cognite Duplicated Error

    Raised if one or more of the referenced ids/external ids have been duplicated in the request.

    Args:
        duplicated (List): The duplicated ids.
        successful (List): List of items which were successfully processed.
        failed (List): List of items which failed.
        unknown (List): List of items which may or may not have been successfully processed.
        unwrap_fn: (Callable): Function to extract identifier from the Cognite resource.
    """

    def __init__(
        self,
        duplicated: List,
        successful: List = None,
        failed: List = None,
        unknown: List = None,
        unwrap_fn: Callable = None,
    ):
        self.duplicated = duplicated
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self) -> str:
        msg = f"Duplicated: {self.duplicated}"
        msg += self._get_multi_exception_summary()
        return msg


class CogniteImportError(CogniteException):
    """Cognite Import Error

    Raised if the user attempts to use functionality which requires an uninstalled package.

    Args:
        module (str): Name of the module which could not be imported
        message (str): The error message to output.
    """

    def __init__(self, module: str, message: str = None):
        self.module = module
        self.message = message or f"The functionality you are trying to use requires '{self.module}' to be installed."

    def __str__(self) -> str:
        return self.message


class CogniteMissingClientError(CogniteException):
    """Cognite Missing Client Error

    Raised if the user attempts to make use of a method which requires the cognite_client being set, but it is not.
    """

    def __str__(self) -> str:
        return "A CogniteClient has not been set on this object. Pass it in the constructor to use it."


class CogniteAuthError(CogniteException):
    ...


class CogniteAssetHierarchyError(CogniteException, AssertionError):
    """Cognite Asset Hierarchy validation Error.

    Raised if the given assets form an invalid hierarchy (by CDF standards).

    Note:
        For historical reasons, we make the error catchable as an AssertionError.

    Args:
        message (str): The error message to output.
        hierarchy (AssetHierarchy): An instance of AssetHierarchy that holds various groups
            of assets that failed validation.
    """

    def __init__(self, message: str, hierarchy: AssetHierarchy) -> None:
        self.message = message
        self._hierarchy = hierarchy

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)})"

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
