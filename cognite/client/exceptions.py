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


class CogniteAssetPostingError(Exception):
    """Cognite Asset Posting Error

    Raised if error occurs while posting an asset hierarchy. Some assets may have been succesfully posted, so this
    exception describes which assets we know have been posted (200), which may have been posted (5xx), and which have
    not been posted (4xx).

    Args:
          posted (AssetList): List of Assets which were posted.
          may_have_been_posted (AssetList): List of Assets which were maybe posted.
          not_posted (AssetList): List of assets which were not posted.
    """

    def __init__(self, posted, may_have_been_posted, not_posted):
        self.posted = posted
        self.may_have_been_posted = may_have_been_posted
        self.not_posted = not_posted

    def __str__(self):
        msg = "Some assets failed to post.\nSucessfully posted (2xx): {}\nMay have been posted (5xx): {}\nNot posted (4xx): {}".format(
            [a.ref_id for a in self.posted],
            [a.ref_id for a in self.may_have_been_posted],
            [a.ref_id for a in self.not_posted],
        )
        return msg


class CogniteMissingClientError(Exception):
    """Cognite Missing Client Error

    Raised if the user attempts to make use of a method which requires the cognite_client being set, but it is not.
    """

    def __str__(self):
        return "A CogniteClient has not been set on this object. Pass it in the constructor to use it."
