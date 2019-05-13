import json
from typing import *


class CogniteAPIError(Exception):
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
            except APIError as e:
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
        extra: Dict = None,
        successful: List = None,
        failed: List = None,
        unknown: List = None,
        unwrap_fn: Callable = None,
    ):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.extra = extra
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self._unwrap_fn = unwrap_fn or (lambda x: x)

    def __str__(self):
        msg = "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            msg = "{} | code: {} | X-Request-ID: {}\n{}".format(
                self.message, self.code, self.x_request_id, pretty_extra
            )
        if len(self.successful) > 0 or len(self.unknown) > 0 or len(self.failed) > 0:
            msg += "\nThe API Failed to process some items.\nSuccessful (2xx): {}\nUnknown (5xx): {}\nFailed (4xx): {}".format(
                [self._unwrap_fn(f) for f in self.successful],
                [self._unwrap_fn(f) for f in self.unknown],
                [self._unwrap_fn(f) for f in self.failed],
            )
        return msg


class CogniteImportError(Exception):
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


class CogniteMissingClientError(Exception):
    """Cognite Missing Client Error

    Raised if the user attempts to make use of a method which requires the cognite_client being set, but it is not.
    """

    def __str__(self):
        return "A CogniteClient has not been set on this object. Pass it in the constructor to use it."


class CogniteAPIKeyError(Exception):
    """Cognite API Key Error.

    Raised if an invalid API key has been used to authenticate.
    """

    def __str__(self):
        return "Invalid API key."
