import json
from typing import Dict


class CogniteAPIError(Exception):
    """Cognite API Error

    Raised if a given request fails.

    Args:
        message (str):  The error message produced by the API
        code (int):     The error code produced by the failure
        x_request_id (str): The request-id generated for the failed request.
        extra (Dict):   A dict of any additional information.

    Examples:
        Catching an API-error and handling it based on the error code::

            from cognite.client import CogniteClient, APIError

            client = CogniteClient()

            try:
                client.login.status()
            except APIError as e:
                if e.code == 401:
                    print("You are not authorized")
                elif e.code == 400:
                    print("Something is wrong with your request")
                elif e.code == 500:
                    print("Something went terribly wrong. Here is the request-id: {}".format(e.x_request_id)
                print("The message returned from the API: {}".format(e.message))
    """

    def __init__(self, message: str, code: int = None, x_request_id: str = None, extra: Dict = None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.extra = extra

    def __str__(self):
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            return "{} | code: {} | X-Request-ID: {}\n{}".format(
                self.message, self.code, self.x_request_id, pretty_extra
            )
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)


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
