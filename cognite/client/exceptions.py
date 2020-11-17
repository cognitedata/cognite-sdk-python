import json
from typing import *


class CogniteException(Exception):
    pass


class CogniteConnectionError(CogniteException):
    pass


class CogniteConnectionRefused(CogniteConnectionError):
    pass


class CogniteReadTimeout(CogniteException):
    pass


class CogniteMultiException(CogniteException):
    def __init__(self, successful: List = None, failed: List = None, unknown: List = None, unwrap_fn: Callable = None):
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self._unwrap_fn = unwrap_fn or (lambda x: x)

    def _get_multi_exception_summary(self):
        if len(self.successful) > 0 or len(self.unknown) > 0 or len(self.failed) > 0:
            return "\nThe API Failed to process some items.\nSuccessful (2xx): {}\nUnknown (5xx): {}\nFailed (4xx): {}".format(
                [self._unwrap_fn(f) for f in self.successful],
                [self._unwrap_fn(f) for f in self.unknown],
                [self._unwrap_fn(f) for f in self.failed],
            )
        return ""


class CogniteAPIError(CogniteMultiException):
    """Cognite API Error

    Raised if a given request fails. If one or more of concurrent requests fails, this exception will also contain
    information about which items were successfully processed (2xx), which may have been processed (5xx), and which have
    failed to be processed (4xx).

    Args:
        message (str): The error message produced by the API
        code (int): The error code produced by the failure
        x_request_id (str): The request-id generated for the failed request.
        extra (Dict): A dict of any additional information.
        successful (List): List of items which were successfully proccessed.
        failed (List): List of items which failed.
        unknown (List): List of items which may or may not have been successfully processed.

    Examples:
        Catching an API-error and handling it based on the error code::

            from cognite.client import CogniteClient
            from cognite.client.exceptions import CogniteAPIError

            c = CogniteClient()

            try:
                c.login.status()
            except CogniteAPIError as e:
                if e.code == 401:
                    print("You are not authorized")
                elif e.code == 400:
                    print("Something is wrong with your request")
                elif e.code == 500:
                    print("Something went terribly wrong. Here is the request-id: {}".format(e.x_request_id)
                print("The message returned from the API: {}".format(e.message))
    """

    def __init__(
        self,
        message: str,
        code: int = None,
        x_request_id: str = None,
        missing: List = None,
        duplicated: List = None,
        successful: List = None,
        failed: List = None,
        unknown: List = None,
        unwrap_fn: Callable = None,
        extra: Dict = None,
    ):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.missing = missing
        self.duplicated = duplicated
        self.extra = extra
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self):
        msg = "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)
        if self.missing:
            msg += "\nMissing: {}".format(self.missing)
        if self.duplicated:
            msg += "\nDuplicated: {}".format(self.duplicated)
        msg += self._get_multi_exception_summary()
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            msg += "\nAdditional error info: {}".format(pretty_extra)
        return msg


class CogniteNotFoundError(CogniteMultiException):
    """Cognite Not Found Error

    Raised if one or more of the referenced ids/external ids are not found.

    Args:
        not_found (List): The ids not found.
        successful (List): List of items which were successfully proccessed.
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

    def __str__(self):
        msg = "Not found: {}".format(self.not_found)
        msg += self._get_multi_exception_summary()
        return msg


class CogniteDuplicatedError(CogniteMultiException):
    """Cognite Duplicated Error

    Raised if one or more of the referenced ids/external ids have been duplicated in the request.

    Args:
        duplicated (list): The duplicated ids.
        successful (List): List of items which were successfully proccessed.
        failed (List): List of items which failed.
        unknown (List): List of items which may or may not have been successfully processed.
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

    def __str__(self):
        msg = "Duplicated: {}".format(self.duplicated)
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
        self.message = message or "The functionality your are trying to use requires '{}' to be installed.".format(
            self.module
        )

    def __str__(self):
        return self.message


class CogniteMissingClientError(CogniteException):
    """Cognite Missing Client Error

    Raised if the user attempts to make use of a method which requires the cognite_client being set, but it is not.
    """

    def __str__(self):
        return "A CogniteClient has not been set on this object. Pass it in the constructor to use it."


class CogniteAPIKeyError(CogniteException):
    """Cognite API Key Error.

    Raised if the API key is missing or invalid.
    """

    pass


class CogniteDuplicateColumnsError(CogniteException):
    """Cognite Duplicate Columns Error

    Raised if the user attempts to create a dataframe through include_aggregate_names=False which results in duplicate column names.
    """

    def __init__(self, dups):
        self.message = "Can not remove aggregate names from this dataframe as it would result in duplicate column name(s) '{}'".format(
            "', '".join(dups)
        )

    def __str__(self):
        return self.message


class ModelFailedException(Exception):
    def __init__(self, typename, id, error_message):
        self.typename = typename
        self.id = id
        self.error_message = error_message

    def __str__(self):
        return f"{self.typename} {self.id} failed with error '{self.error_message}'"
